package dynamoDao

import (
	"code.bluesoftdev.com/v1/repos/dynamoDao/uuid"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"reflect"
	"strings"
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

func attributeDefinitions(t interface{}, allKeyAttrNames map[string]interface{}) ([]*dynamodb.AttributeDefinition, map[string]*reflect.StructField, error) {
	return attributeDefinitionsForType(getStructType(t), allKeyAttrNames)
}

func attributeDefinitionsForType(structType reflect.Type, allKeyAttrNames map[string]interface{}) ([]*dynamodb.AttributeDefinition,map[string]*reflect.StructField, error) {
	return attributeDefinitionsForTypeWithBaseName("", structType, allKeyAttrNames)
}

func attributeDefinitionsForTypeWithBaseName(baseName string, structType reflect.Type, allKeyAttrNames map[string]interface{}) ([]*dynamodb.AttributeDefinition, map[string]*reflect.StructField,error) {
	attrDefs := make([]*dynamodb.AttributeDefinition, 0, structType.NumField())
	attrToField := make(map[string]*reflect.StructField)
	for f := 0; f < structType.NumField(); f++ {
		field := structType.Field(f)
		name, awsType := parseAttrDef(field, baseName, allKeyAttrNames)
		if name != "-" && awsType != "" {
			attrDef := dynamodb.AttributeDefinition{
				AttributeName: aws.String(name),
				AttributeType: aws.String(awsType),
			}
			attrDefs = append(attrDefs, &attrDef)
			attrToField[name] = &field
		} else if name != "-" && (field.Type.Kind() == reflect.Struct ||
			(field.Type.Kind() == reflect.Ptr && field.Type.Elem().Kind() == reflect.Struct)) {
			structType := field.Type
			if structType.Kind() == reflect.Ptr {
				structType = field.Type.Elem()
			}
			subTypeAttrDefs, subAttrToField, err := attributeDefinitionsForTypeWithBaseName(name+".", structType, allKeyAttrNames)
			if err != nil {
				return nil, nil, err
			}
			if subTypeAttrDefs != nil {
				for _, attrDef := range subTypeAttrDefs {
					attrDefs = append(attrDefs, attrDef)
				}
			}
			if subAttrToField != nil {
				for k, v := range subAttrToField {
					attrToField[k] = v
				}
			}
		}
	}
	return attrDefs, attrToField, nil
}

func parseAttrDef(field reflect.StructField, baseName string, allKeyAttrNames map[string]interface{}) (string, string) {
	name := baseName + field.Name
	awsType := mapFieldTypeToScalarType(field)
	if dynamodbav, ok := field.Tag.Lookup("dynamodbav"); ok {
		values := strings.Split(dynamodbav, ",")
		if values[0] != "" {
			if values[0] == "-" {
				name = "-"
			} else {
				name = baseName + values[0]
			}
		}
		if len(values) > 1 && values[1] != "" {
			typ := values[1]
			if values[1] == "omitempty" && len(values) > 2 {
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
	if _, ok := allKeyAttrNames[name]; awsType != "" && !ok {
		name = "-" // Ignore this field for the attr list.
	}
	return name, awsType
}
