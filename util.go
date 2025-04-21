package data_mongodb

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"time"

	. "github.com/infrago/base"
	"github.com/infrago/infra"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func transform(input bson.M) (Map, error) {
	output := make(Map)
	err := infra.UnmarshalJSON([]byte(input.String()), &output)
	if err != nil {
		return nil, err
	}
	return transformMap(output), nil
}

func transformMap(input map[string]any) map[string]any {
	result := make(map[string]any)
	for key, value := range input {
		result[key] = transformValue(value)
	}
	return result
}

func transformValue(value any) any {
	switch v := value.(type) {

	case map[string]any:
		// 单键 map，判断是否是特殊 BSON 类型
		if len(v) == 1 {
			switch {
			case v["$numberInt"] != nil:
				if str, ok := v["$numberInt"].(string); ok {
					if val, err := strconv.Atoi(str); err == nil {
						return int32(val)
					}
				}
			case v["$numberLong"] != nil:
				if str, ok := v["$numberLong"].(string); ok {
					if val, err := strconv.ParseInt(str, 10, 64); err == nil {
						return val
					}
				}
			case v["$numberDouble"] != nil:
				if str, ok := v["$numberDouble"].(string); ok {
					if val, err := strconv.ParseFloat(str, 64); err == nil {
						return val
					}
				}
			case v["$numberDecimal"] != nil:
				if str, ok := v["$numberDecimal"].(string); ok {
					if val, err := strconv.ParseFloat(str, 64); err == nil {
						return val
					}
				}
			case v["$oid"] != nil:
				if str, ok := v["$oid"].(string); ok {
					return str
				}
			case v["$date"] != nil:
				switch dateVal := v["$date"].(type) {
				case string:
					if t, err := time.Parse(time.RFC3339, dateVal); err == nil {
						return t
					}
					return dateVal
				case map[string]any:
					if numStr, ok := dateVal["$numberLong"].(string); ok {
						if ms, err := strconv.ParseInt(numStr, 10, 64); err == nil {
							return time.UnixMilli(ms)
						}
					}
				}
			case v["$bool"] != nil:
				if b, ok := v["$bool"].(bool); ok {
					return b
				}
			case v["$undefined"] != nil:
				return nil
			case v["$timestamp"] != nil:
				if ts, ok := v["$timestamp"].(map[string]any); ok {
					tVal, _ := ts["t"].(float64)
					iVal, _ := ts["i"].(float64)
					return map[string]uint32{
						"t": uint32(tVal),
						"i": uint32(iVal),
					}
				}
			case v["$regularExpression"] != nil:
				if re, ok := v["$regularExpression"].(map[string]any); ok {
					pattern, _ := re["pattern"].(string)
					options, _ := re["options"].(string)
					return fmt.Sprintf("regex(%s, %s)", pattern, options)
				}
			case v["$binary"] != nil:
				if bin, ok := v["$binary"].(map[string]any); ok {
					base64Str, _ := bin["base64"].(string)
					bytes, err := base64.StdEncoding.DecodeString(base64Str)
					if err == nil {
						return bytes
					}
				}
			case v["$minKey"] != nil:
				return "__MinKey__"
			case v["$maxKey"] != nil:
				return "__MaxKey__"
			}
		}

		// 不是特殊类型，递归处理 map
		inner := make(map[string]any)
		for k, val := range v {
			inner[k] = transformValue(val)
		}
		return inner

	case []any:
		// 递归处理数组元素
		newArr := make([]any, len(v))
		for i, item := range v {
			newArr[i] = transformValue(item)
		}
		return newArr

	default:
		return v
	}
}
