package dynamoDao

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"reflect"
	"strings"
	"code.bluesoftdev.com/v1/repos/dynamoDao/uuid"
)

func mapFieldTypeToScalarType(f reflect.StructField) string {
	return mapToScalarType(f.Type)
}

func mapToScalarType(t reflect.Type) string {
	switch t.Kind() {
	case reflect.Ptr:
		return mapToScalarType(t.Elem())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64, reflect.Bool:
		return "N"
	case reflect.String:
		return "S"
	case reflect.Struct:
		if t.AssignableTo(reflect.TypeOf(uuid.Nil)) {
			return "B"
		}
		return ""
	case reflect.Array, reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			return "B"
		}
		fallthrough
	default:
		return ""
	}
}

func attributeDefinitions(t interface{}, allKeyAttrNames map[string]interface{}) ([]*dynamodb.AttributeDefinition, error) {
	return attributeDefinitionsForType(getStructType(t), allKeyAttrNames)
}

func attributeDefinitionsForType(structType reflect.Type, allKeyAttrNames map[string]interface{}) ([]*dynamodb.AttributeDefinition, error) {
	attrDefs := make([]*dynamodb.AttributeDefinition, 0, structType.NumField())
	for f := 0; f < structType.NumField(); f++ {
		field := structType.Field(f)
		name := field.Name
		awsType := mapFieldTypeToScalarType(field)
		if dynamodbav, ok := field.Tag.Lookup("dynamodbav"); ok {
			values := strings.Split(dynamodbav, ",")
			if values[0] != "" {
				name = values[0]
			}
			if _, ok := allKeyAttrNames[name]; !ok {
				name = "-" // Ignore this field for the attr list.
			}
			if len(values) > 1 && values[1] != "" {
				typ := values[1]
				if values[1] == "omitempty" && len(values) > 2 && values[2] != "" {
					typ = values[2]
				}
				switch typ {
				case "string":
					awsType = "S"
				case "unixtime":
					awsType = "N"
				}
			}
		}
		if name != "-" && awsType != "" {
			attrDef := dynamodb.AttributeDefinition{
				AttributeName: aws.String(name),
				AttributeType: aws.String(awsType),
			}
			attrDefs = append(attrDefs, &attrDef)
		}
	}
	return attrDefs, nil
}
