package athena

import (
	"database/sql/driver"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/service/athena"
)

const (
	// TimestampLayout is the Go time layout string for an Athena `timestamp`.
	TimestampLayout             = "2006-01-02 15:04:05.999"
	TimestampWithTimeZoneLayout = "2006-01-02 15:04:05.999 MST"
	DateLayout                  = "2006-01-02"
)

func addToMap(mapContainer map[string]interface{}, currentValue []byte) error {
	keyVal := strings.Split(string(currentValue), "=")
	key, val := keyVal[0], keyVal[1]
	realValue, err := convertUnknownValue(val)
	if err != nil {
		return err
	}

	mapContainer[key] = realValue
	return nil
}

func addToArray(arrayContainer []interface{}, currentValue []byte) error {
	realValue, err := convertUnknownValue(string(currentValue))
	if err != nil {
		return err
	}

	arrayContainer = append(arrayContainer, realValue)
	return nil
}

func parse(val string, isMap bool) (interface{}, error) {
	mapContainer := map[string]interface{}{}
	arrayContainer := []interface{}{}
	values := val[1 : len(val)-1]
	currentValue := []byte{}
	ignoreArray := 0
	ignoreMap := 0
	for i := 0; i < len(values); i++ {
		c := values[i]

		// Only skip arrays/maps if delimiter starts after an = or a comma and space
		if c == '[' || c == '{' {
			startsValue := i == 0
			followsEqual := i-1 >= 0 && values[i-1] == '='
			followsCommaSpace := i-2 >= 0 && values[i-2] == ',' && values[i-1] == ' '
			followsDelimiter := i-1 >= 0 && (values[i-1] == '{' || values[i-1] == '[')
			if startsValue || followsEqual || followsCommaSpace || followsDelimiter {
				currentValue = append(currentValue, c)
				if c == '[' {
					ignoreArray++
				} else {
					ignoreMap++
				}

				continue
			}
		}

		// Only exit ignore if in one
		if (ignoreArray%2 == 1 && c == ']') || (ignoreMap%1 == 0 && c == '}') {
			currentValue = append(currentValue, c)
			if c == ']' {
				ignoreArray--
			} else {
				ignoreMap--
			}

			continue
		}

		if ignoreMap > 0 || ignoreArray > 0 {
			currentValue = append(currentValue, c)
			continue
		}

		// Delimiter is comma plus a space
		if c == ',' && i < len(values)-1 && values[i+1] == ' ' {
			var err error
			if isMap {
				err = addToMap(mapContainer, currentValue)

			} else {
				err = addToArray(arrayContainer, currentValue)
			}

			if err != nil {
				return nil, err
			}

			currentValue = []byte{}
			// Skip whitespace
			i++
			continue
		}

		currentValue = append(currentValue, c)
	}

	if len(currentValue) > 0 {
		fmt.Println(string(currentValue))
		var err error
		if isMap {
			err = addToMap(mapContainer, currentValue)
		} else {
			err = addToArray(arrayContainer, currentValue)
		}

		if err != nil {
			return nil, err
		}
	}

	if isMap {
		return mapContainer, nil
	}

	return arrayContainer, nil
}

func parseMap(val string) (interface{}, error) {
	return parse(val, true)
}

func parseArray(val string) (interface{}, error) {
	return parse(val, false)
}

func convertRow(columns []*athena.ColumnInfo, in []*athena.Datum, ret []driver.Value) error {
	for i, val := range in {
		coerced, err := convertValue(*columns[i].Type, val)
		if err != nil {
			return err
		}

		ret[i] = coerced
	}

	return nil
}

func convertUnknownValue(v string) (interface{}, error) {
	if _, err := strconv.ParseInt(v, 10, 16); err == nil {
		return convertValue("smallint", v)
	}

	if _, err := strconv.ParseInt(v, 10, 32); err == nil {
		return convertValue("integer", v)
	}

	if _, err := strconv.ParseInt(v, 10, 64); err == nil {
		return convertValue("bigint", v)
	}

	if v[0] == '[' && v[len(v)-1] == ']' {
		return parseArray(v)
	}

	if v[0] == '{' && v[len(v)-1] == '}' {
		return parseMap(v)
	}

	return convertValue("string", v)
}

func convertValue(val interface{}) (interface{}, error) {
	if val == "" {
		return nil, nil
	}

	switch athenaType {
	case "smallint":
		return strconv.ParseInt(val, 10, 16)
	case "integer":
		return strconv.ParseInt(val, 10, 32)
	case "bigint":
		return strconv.ParseInt(val, 10, 64)
	case "boolean":
		switch val {
		case "true":
			return true, nil
		case "false":
			return false, nil
		}
		return nil, fmt.Errorf("cannot parse '%s' as boolean", val)
	case "float":
		return strconv.ParseFloat(val, 32)
	case "double", "decimal":
		return strconv.ParseFloat(val, 64)
	case "varchar", "string":
		return val, nil
	case "timestamp":
		return time.Parse(TimestampLayout, val)
	case "timestamp with time zone":
		return time.Parse(TimestampWithTimeZoneLayout, val)
	case "date":
		return time.Parse(DateLayout, val)
	case "struct":
		panic(fmt.Errorf("cannot parse `%s` as struct", val))
	case "map":
		return parseMap(val)
	case "array":
		return parseArray(val)
	case "row":
		return parseMap(val)
	default:
		panic(fmt.Errorf("unknown type `%s` with value %s", athenaType, val))
	}
}
