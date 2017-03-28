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
	for f := 0; f < structType.NumField(); f++ {
		field := structType.Field(f)
		fieldName := field.Name
		if dyanamodbav, ok := field.Tag.Lookup("dynamodbav"); ok {
			elements := strings.Split(dyanamodbav, ",")
			if elements[0] != "" {
				fieldName = elements[0]
			}
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
							return nil, errors.New(structType.Name() + "." + field.Name + ": Error parsing read capacity unit for index " + indexName + ": " + nameAndRole[2])
						}
						gsi.ProvisionedThroughput.ReadCapacityUnits = aws.Int64(int64(readCap))
						writeCap, err := parseCapacity(nameAndRole[3], 1)
						if err != nil {
							return nil, errors.New(structType.Name() + "." + field.Name + ": Error parsing write capacity unit for index " + indexName + ": " + nameAndRole[3])
						}
						gsi.ProvisionedThroughput.WriteCapacityUnits = aws.Int64(int64(writeCap))

						if len(nameAndRole) > 4 {
							// Parse projection type
							if gsi.Projection == nil {
								gsi.Projection = new(dynamodb.Projection)
								gsi.Projection.NonKeyAttributes = make([]*string, 0, 20)
							}
							projectionType := nameAndRole[4]
							switch projectionType {
							case "keys_only":
								if len(gsi.Projection.NonKeyAttributes) > 0 {
									return nil, errors.New(structType.Name() + "." + field.Name + ": Projection is specified as keys_only but there are projected fields specified")
								}
								gsi.Projection.ProjectionType = aws.String(dynamodb.ProjectionTypeKeysOnly)
							case "all":
								if len(gsi.Projection.NonKeyAttributes) > 0 {
									return nil, errors.New(structType.Name() + "." + field.Name + ": Projection is specified as all but there are projected fields specified")
								}
								gsi.Projection.ProjectionType = aws.String(dynamodb.ProjectionTypeAll)
							case "include":
								gsi.Projection.ProjectionType = aws.String(dynamodb.ProjectionTypeInclude)
							case "":
							// do nothing.
							default:
								return nil, errors.New(structType.Name() + "." + field.Name + ": invalid projection type specified: " + projectionType)
							}
						}
					}
				case "range":
					if gsi.KeySchema == nil {
						gsi.KeySchema = make([]*dynamodb.KeySchemaElement, 0, 2)
					}
					gsi.KeySchema = append(gsi.KeySchema, &dynamodb.KeySchemaElement{
						KeyType:       aws.String(dynamodb.KeyTypeRange),
						AttributeName: aws.String(fieldName),
					})
				case "project":
					if gsi.Projection == nil {
						gsi.Projection = new(dynamodb.Projection)
						gsi.Projection.NonKeyAttributes = make([]*string, 0, 20)
					}
					if gsi.Projection.ProjectionType != nil && *gsi.Projection.ProjectionType == dynamodb.ProjectionTypeKeysOnly {
						return nil, errors.New(structType.Name() + "." + field.Name + ": Projection is specified as keys_only but there are projected fields specified")
					}
					if gsi.Projection.ProjectionType != nil && *gsi.Projection.ProjectionType == dynamodb.ProjectionTypeAll {
						return nil, errors.New(structType.Name() + "." + field.Name + ": Projection is specified as all but there are projected fields specified")
					}
					gsi.Projection.ProjectionType = aws.String(dynamodb.ProjectionTypeInclude)
					gsi.Projection.NonKeyAttributes = append(gsi.Projection.NonKeyAttributes, aws.String(fieldName))
				}
			}

		}
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
