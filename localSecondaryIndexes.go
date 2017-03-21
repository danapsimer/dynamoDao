package dynamoDao

import (
	"errors"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"strings"
)

func localIndexes(t interface{}, baseHashKey *dynamodb.KeySchemaElement) ([]*dynamodb.LocalSecondaryIndex, error) {
	LSIs := make(map[string]*dynamodb.LocalSecondaryIndex)
	structType := getStructType(t)
	for f := 0; f < structType.NumField(); f++ {
		field := structType.Field(f)
		fieldName := field.Name
		if dynamodDbDaoLSI, ok := field.Tag.Lookup(localIndexTag); ok {
			if dyanamodbav, ok := field.Tag.Lookup("dynamodbav"); ok {
				elements := strings.Split(dyanamodbav, ",")
				if elements[0] != "" {
					fieldName = elements[0]
				}
			}
			for _, gsiStr := range strings.Split(dynamodDbDaoLSI, ";") {
				nameAndRole := strings.Split(gsiStr, ",")
				indexName := nameAndRole[0]
				role := nameAndRole[1]
				lsi, ok := LSIs[indexName]
				if !ok {
					lsi = new(dynamodb.LocalSecondaryIndex)
					lsi.IndexName = aws.String(indexName)
					LSIs[indexName] = lsi
					lsi.KeySchema = make([]*dynamodb.KeySchemaElement, 0, 2)
					lsi.KeySchema = append(lsi.KeySchema, baseHashKey)
				}
				switch role {
				case "range":
					lsi.KeySchema = append(lsi.KeySchema, &dynamodb.KeySchemaElement{
						KeyType:       aws.String(dynamodb.KeyTypeRange),
						AttributeName: aws.String(fieldName),
					})
					if len(nameAndRole) > 2 {
						// Parse projection type
						if lsi.Projection == nil {
							lsi.Projection = new(dynamodb.Projection)
							lsi.Projection.NonKeyAttributes = make([]*string, 0, 20)
						}
						projectionType := nameAndRole[2]
						switch projectionType {
						case "keys_only":
							if len(lsi.Projection.NonKeyAttributes) > 0 {
								return nil, errors.New(structType.Name() + "." + field.Name + ": Projection is specified as keys_only but there are projected fields specified")
							}
							lsi.Projection.ProjectionType = aws.String(dynamodb.ProjectionTypeKeysOnly)
						case "all":
							if len(lsi.Projection.NonKeyAttributes) > 0 {
								return nil, errors.New(structType.Name() + "." + field.Name + ": Projection is specified as all but there are projected fields specified")
							}
							lsi.Projection.ProjectionType = aws.String(dynamodb.ProjectionTypeAll)
						case "include":
							lsi.Projection.ProjectionType = aws.String(dynamodb.ProjectionTypeInclude)
						case "":
						// do nothing.
						default:
							return nil, errors.New(structType.Name() + "." + field.Name + ": invalid projection type specified: " + projectionType)
						}
					}
				case "project":
					if lsi.Projection == nil {
						lsi.Projection = new(dynamodb.Projection)
						lsi.Projection.NonKeyAttributes = make([]*string, 0, 20)
					}
					if lsi.Projection.ProjectionType != nil && *lsi.Projection.ProjectionType == dynamodb.ProjectionTypeKeysOnly {
						return nil, errors.New(structType.Name() + "." + field.Name + ": Projection is specified as keys_only but there are projected fields specified")
					}
					if lsi.Projection.ProjectionType != nil && *lsi.Projection.ProjectionType == dynamodb.ProjectionTypeAll {
						return nil, errors.New(structType.Name() + "." + field.Name + ": Projection is specified as all but there are projected fields specified")
					}
					lsi.Projection.ProjectionType = aws.String(dynamodb.ProjectionTypeInclude)
					lsi.Projection.NonKeyAttributes = append(lsi.Projection.NonKeyAttributes, aws.String(fieldName))
				}
			}

		}
	}
	lsiArray := make([]*dynamodb.LocalSecondaryIndex, 0, len(LSIs))
	for _, lsi := range LSIs {
		if lsi.Projection == nil {
			lsi.Projection = &dynamodb.Projection{ProjectionType: aws.String(dynamodb.ProjectionTypeAll)}
		} else if lsi.Projection.ProjectionType == nil {
			if lsi.Projection.NonKeyAttributes != nil && len(lsi.Projection.NonKeyAttributes) > 0 {
				lsi.Projection.ProjectionType = aws.String(dynamodb.ProjectionTypeInclude)
			} else {
				lsi.Projection.ProjectionType = aws.String(dynamodb.ProjectionTypeAll)
			}
		}
		lsiArray = append(lsiArray, lsi)
	}
	if len(lsiArray) == 0 {
		return nil, nil
	} else {
		return lsiArray, nil
	}
}
