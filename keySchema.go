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
	keySchema := make([]*dynamodb.KeySchemaElement, 0, 2)
	var thruput *dynamodb.ProvisionedThroughput
	for f := 0; f < structType.NumField(); f++ {
		field := structType.Field(f)
		name := field.Name
		var keyType string
		if dynamoDbDaoKey, ok := field.Tag.Lookup(keySchemaTag); ok {
			keyDef := strings.Split(dynamoDbDaoKey, ",")
			switch keyDef[0] {
			case "hash":
				keyType = dynamodb.KeyTypeHash
			case "range":
				keyType = dynamodb.KeyTypeRange
			}
			if len(keyDef) > 2 {
				// parse R/W Capacity
				thruput = new(dynamodb.ProvisionedThroughput)
				readCap, err := parseCapacity(keyDef[1], 5)
				if err != nil {
					return nil, nil, errors.New(structType.Name() + "." + field.Name + ": Error parsing read capacity unit: " + keyDef[1])
				}
				thruput.ReadCapacityUnits = aws.Int64(int64(readCap))
				writeCap, err := parseCapacity(keyDef[2], 1)
				if err != nil {
					return nil, nil, errors.New(structType.Name() + "." + field.Name + ": Error parsing write capacity unit: " + keyDef[2])
				}
				thruput.WriteCapacityUnits = aws.Int64(int64(writeCap))
			}
			// only check for the alias if there was a keySchemaTag.
			if dynamodbav, ok := field.Tag.Lookup("dynamodbav"); ok {
				elements := strings.Split(dynamodbav, ",")
				if elements[0] != "" {
					name = elements[0]
				}
			}
		}
		if keyType != "" {
			keySchemaElem := dynamodb.KeySchemaElement{
				AttributeName: aws.String(name),
				KeyType:       aws.String(keyType),
			}
			if len(keySchema) > 0 && *keySchemaElem.KeyType == dynamodb.KeyTypeHash {
				keySchema = append(keySchema, nil)
				copy(keySchema[1:], keySchema[0:])
				keySchema[0] = &keySchemaElem
			} else {
				keySchema = append(keySchema, &keySchemaElem)
			}
		}
	}
	return keySchema, thruput, nil
}
