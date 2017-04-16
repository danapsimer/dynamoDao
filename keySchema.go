package dynamoDao

import (
	"errors"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"reflect"
	"strings"
)

func keySchema(t interface{}) ([]*dynamodb.KeySchemaElement, *dynamodb.ProvisionedThroughput, error) {
	return keySchemaForType(getStructType(t))
}

func keySchemaForType(structType reflect.Type) ([]*dynamodb.KeySchemaElement, *dynamodb.ProvisionedThroughput, error) {
	return keySchemaForTypeWithBaseName("", structType)
}

func keySchemaForTypeWithBaseName(baseName string, structType reflect.Type) ([]*dynamodb.KeySchemaElement,
	*dynamodb.ProvisionedThroughput, error) {
	keySchema := make([]*dynamodb.KeySchemaElement, 0, 2)
	var thruput *dynamodb.ProvisionedThroughput
	for f := 0; f < structType.NumField(); f++ {
		field := structType.Field(f)
		name := getFieldName(baseName, field)
		if name == "-" {
			continue
		}
		var keyType string
		if dynamoDbDaoKey, ok := field.Tag.Lookup(keySchemaTag); ok {
			fieldKeyType, fieldThruput, err := parseDynamoKeyTag(dynamoDbDaoKey, structType, field)
			if err != nil {
				return nil, nil, err
			}
			if thruput != nil && fieldThruput != nil {
				return nil, nil, errors.New("multiple throughput specifications!")
			}
			keyType = fieldKeyType
			if thruput == nil {
				thruput = fieldThruput
			}
		}
		if keyType != "" {
			keySchemaElem := dynamodb.KeySchemaElement{
				AttributeName: aws.String(name),
				KeyType:       aws.String(keyType),
			}
			keySchema = addKeySchemaElement(keySchema, &keySchemaElem)
		} else if field.Type.Kind() == reflect.Struct ||
			(field.Type.Kind() == reflect.Ptr && field.Type.Elem().Kind() == reflect.Struct) {
			structType := field.Type
			if structType.Kind() == reflect.Ptr {
				structType = field.Type.Elem()
			}
			subKeySchema, subThruput, err := keySchemaForTypeWithBaseName(name+".", structType)
			if err != nil {
				return nil, nil, err
			}
			if thruput != nil && subThruput != nil {
				return nil, nil, errors.New("multiple throughput specifications!")
			}
			thruput = subThruput
			if subKeySchema != nil {
				for _, kse := range subKeySchema {
					keySchema = addKeySchemaElement(keySchema, kse)
				}
			}
		}
	}
	return keySchema, thruput, nil
}

func getFieldName(baseName string, field reflect.StructField) string {
	name := baseName + field.Name
	if dynamodbav, ok := field.Tag.Lookup("dynamodbav"); ok {
		elements := strings.Split(dynamodbav, ",")
		if elements[0] != "" {
			if elements[0] == "-" {
				name = "-"
			} else {
				name = baseName + elements[0]
			}
		}
	}
	return name
}

func parseDynamoKeyTag(dynamoDbDaoKey string, structType reflect.Type, field reflect.StructField) (keyType string,
	thruput *dynamodb.ProvisionedThroughput, err error) {
	keyType = ""
	thruput = nil
	err = nil
	keyDef := strings.Split(dynamoDbDaoKey, ",")
	switch keyDef[0] {
	case "hash":
		keyType = dynamodb.KeyTypeHash
		if len(keyDef) > 2 {
			// parse R/W Capacity
			thruput = new(dynamodb.ProvisionedThroughput)
			var readCap, writeCap int64
			readCap, err = parseCapacity(keyDef[1], 5)
			if err != nil {
				return "", nil, errors.New(structType.Name() + "." + field.Name + ": Error parsing read capacity unit: " +
					keyDef[1] + ": " + err.Error())
			}
			thruput.ReadCapacityUnits = aws.Int64(int64(readCap))
			writeCap, err = parseCapacity(keyDef[2], 1)
			if err != nil {
				return "", nil, errors.New(structType.Name() + "." + field.Name + ": Error parsing write capacity unit: " +
					keyDef[2] + ": " + err.Error())
			}
			thruput.WriteCapacityUnits = aws.Int64(int64(writeCap))
		}
	case "range":
		keyType = dynamodb.KeyTypeRange
	}
	return
}

func addKeySchemaElement(keySchema []*dynamodb.KeySchemaElement,
	keySchemaElem *dynamodb.KeySchemaElement) []*dynamodb.KeySchemaElement {
	if len(keySchema) > 0 && *keySchemaElem.KeyType == dynamodb.KeyTypeHash {
		keySchema = append(keySchema, nil)
		copy(keySchema[1:], keySchema[0:])
		keySchema[0] = keySchemaElem
	} else {
		keySchema = append(keySchema, keySchemaElem)
	}
	return keySchema
}
