package dynamoDao

import (
	"errors"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"reflect"
	"strings"
)

func globalIndexes(t interface{}) ([]*dynamodb.GlobalSecondaryIndex, error) {
	return globalIndexesForType(getStructType(t))
}

func globalIndexesForType(structType reflect.Type) ([]*dynamodb.GlobalSecondaryIndex, error) {
	GSIs := make(map[string]*dynamodb.GlobalSecondaryIndex)
	err := extractGlobalSecondaryIndexes("", structType, GSIs)
	if err != nil {
		return nil, err
	}
	gsiArray := make([]*dynamodb.GlobalSecondaryIndex, 0, len(GSIs))
	for _, gsi := range GSIs {
		if gsi.ProvisionedThroughput == nil {
			gsi.ProvisionedThroughput = &dynamodb.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(5),
				WriteCapacityUnits: aws.Int64(1),
			}
		}
		if gsi.Projection == nil {
			gsi.Projection = &dynamodb.Projection{ProjectionType: aws.String(dynamodb.ProjectionTypeAll)}
		} else if gsi.Projection.ProjectionType == nil {
			if gsi.Projection.NonKeyAttributes != nil && len(gsi.Projection.NonKeyAttributes) > 0 {
				gsi.Projection.ProjectionType = aws.String(dynamodb.ProjectionTypeInclude)
			} else {
				gsi.Projection.ProjectionType = aws.String(dynamodb.ProjectionTypeAll)
			}
		}
		gsiArray = append(gsiArray, gsi)
	}
	if len(gsiArray) == 0 {
		return nil, nil
	} else {
		return gsiArray, nil
	}
}

func extractGlobalSecondaryIndexes(baseName string, structType reflect.Type, GSIs map[string]*dynamodb.GlobalSecondaryIndex) error {
	for f := 0; f < structType.NumField(); f++ {
		field := structType.Field(f)
		fieldName := getFieldName(baseName, field)
		if fieldName == "-" {
			continue
		}

		if dynamodDbDaoGSI, ok := field.Tag.Lookup(globalIndexTag); ok {
			for _, gsiStr := range strings.Split(dynamodDbDaoGSI, ";") {
				nameAndRole := strings.Split(gsiStr, ",")
				indexName := nameAndRole[0]
				role := nameAndRole[1]
				gsi, ok := GSIs[indexName]
				if !ok {
					gsi = new(dynamodb.GlobalSecondaryIndex)
					gsi.IndexName = aws.String(indexName)
					GSIs[indexName] = gsi
				}
				switch role {
				case "hash":
					err := parseGSIHashKey(fieldName, nameAndRole, gsi)
					if err != nil {
						return err
					}
				case "range":
					err := parseGSIRangeKey(fieldName, gsi)
					if err != nil {
						return err
					}
				case "project":
					err := parseGSIProjectedField(fieldName, gsi)
					if err != nil {
						return err
					}
				}
			}
		} else if field.Type.Kind() == reflect.Struct ||
			(field.Type.Kind() == reflect.Ptr && field.Type.Elem().Kind() == reflect.Struct) {
			structType := field.Type
			if structType.Kind() == reflect.Ptr {
				structType = field.Type.Elem()
			}
			err := extractGlobalSecondaryIndexes(fieldName+".", structType, GSIs)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func parseGSIProjectedField(fieldName string, gsi *dynamodb.GlobalSecondaryIndex) error {
	if gsi.Projection == nil {
		gsi.Projection = new(dynamodb.Projection)
		gsi.Projection.NonKeyAttributes = make([]*string, 0, 20)
	}
	if gsi.Projection.ProjectionType != nil && *gsi.Projection.ProjectionType == dynamodb.ProjectionTypeKeysOnly {
		return errors.New(fieldName + ": Projection is specified as keys_only but there are projected fields specified")
	}
	if gsi.Projection.ProjectionType != nil && *gsi.Projection.ProjectionType == dynamodb.ProjectionTypeAll {
		return errors.New(fieldName + ": Projection is specified as all but there are projected fields specified")
	}
	gsi.Projection.ProjectionType = aws.String(dynamodb.ProjectionTypeInclude)
	gsi.Projection.NonKeyAttributes = append(gsi.Projection.NonKeyAttributes, aws.String(fieldName))
	return nil
}

func parseGSIRangeKey(fieldName string, gsi *dynamodb.GlobalSecondaryIndex) error {
	if gsi.KeySchema == nil {
		gsi.KeySchema = make([]*dynamodb.KeySchemaElement, 0, 2)
	}
	gsi.KeySchema = append(gsi.KeySchema, &dynamodb.KeySchemaElement{
		KeyType:       aws.String(dynamodb.KeyTypeRange),
		AttributeName: aws.String(fieldName),
	})
	return nil
}

func parseGSIHashKey(fieldName string, nameAndRole []string, gsi *dynamodb.GlobalSecondaryIndex) error {
	if gsi.KeySchema == nil {
		gsi.KeySchema = make([]*dynamodb.KeySchemaElement, 0, 2)
		gsi.KeySchema = append(gsi.KeySchema, &dynamodb.KeySchemaElement{
			KeyType:       aws.String(dynamodb.KeyTypeHash),
			AttributeName: aws.String(fieldName),
		})
	} else {
		gsi.KeySchema = append(gsi.KeySchema, nil)
		copy(gsi.KeySchema[1:], gsi.KeySchema[0:])
		gsi.KeySchema[0] = &dynamodb.KeySchemaElement{
			KeyType:       aws.String(dynamodb.KeyTypeHash),
			AttributeName: aws.String(fieldName),
		}
	}
	if len(nameAndRole) > 3 {
		// parse R/W Capacity
		gsi.ProvisionedThroughput = new(dynamodb.ProvisionedThroughput)
		readCap, err := parseCapacity(nameAndRole[2], 5)
		if err != nil {
			return errors.New(fieldName + ": Error parsing read capacity unit for index " + nameAndRole[0] + ": " + nameAndRole[2])
		}
		gsi.ProvisionedThroughput.ReadCapacityUnits = aws.Int64(int64(readCap))
		writeCap, err := parseCapacity(nameAndRole[3], 1)
		if err != nil {
			return errors.New(fieldName + ": Error parsing write capacity unit for index " + nameAndRole[0] + ": " + nameAndRole[3])
		}
		gsi.ProvisionedThroughput.WriteCapacityUnits = aws.Int64(int64(writeCap))

		if len(nameAndRole) > 4 {
			// Parse projection type
			if gsi.Projection == nil {
				gsi.Projection = new(dynamodb.Projection)
			}
			projectionType := nameAndRole[4]
			switch projectionType {
			case "keys_only":
				if len(gsi.Projection.NonKeyAttributes) > 0 {
					return errors.New(fieldName + ": Projection is specified as keys_only but there are projected fields specified")
				}
				gsi.Projection.ProjectionType = aws.String(dynamodb.ProjectionTypeKeysOnly)
			case "all":
				if len(gsi.Projection.NonKeyAttributes) > 0 {
					return errors.New(fieldName + ": Projection is specified as all but there are projected fields specified")
				}
				gsi.Projection.ProjectionType = aws.String(dynamodb.ProjectionTypeAll)
			case "include":
				gsi.Projection.ProjectionType = aws.String(dynamodb.ProjectionTypeInclude)
				gsi.Projection.NonKeyAttributes = make([]*string, 0, 20)
			case "":
			// do nothing.
			default:
				return errors.New(fieldName + ": invalid projection type specified: " + projectionType)
			}
		}
	}
	return nil
}
