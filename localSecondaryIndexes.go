package dynamoDao

import (
	"errors"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"reflect"
	"strings"
)

func localIndexes(t interface{}, baseHashKey *dynamodb.KeySchemaElement) ([]*dynamodb.LocalSecondaryIndex, error) {
	return localIndexesForType(getStructType(t), baseHashKey)
}

func localIndexesForType(structType reflect.Type, baseHashKey *dynamodb.KeySchemaElement) ([]*dynamodb.LocalSecondaryIndex, error) {
	LSIs := make(map[string]*dynamodb.LocalSecondaryIndex)
	if err := extractLocalSecondaryIndexes("", structType, baseHashKey, LSIs); err != nil {
		return nil, err
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

func extractLocalSecondaryIndexes(baseName string, structType reflect.Type, baseHashKey *dynamodb.KeySchemaElement, LSIs map[string]*dynamodb.LocalSecondaryIndex) error {
	for f := 0; f < structType.NumField(); f++ {
		field := structType.Field(f)
		fieldName := getFieldName(baseName, field)
		if fieldName == "-" {
			continue
		}
		if dynamodDbDaoLSI, ok := field.Tag.Lookup(localIndexTag); ok {
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
					if err := processRangeKeyForLSI(structType, field, fieldName, nameAndRole, lsi); err != nil {
						return err
					}
				case "project":
					if err := processProjectFieldForLSI(structType, field, fieldName, lsi); err != nil {
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
			err := extractLocalSecondaryIndexes(fieldName+".", structType, baseHashKey, LSIs)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func processRangeKeyForLSI(structType reflect.Type, field reflect.StructField, fieldName string, nameAndRole []string, lsi *dynamodb.LocalSecondaryIndex) error {
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
				return errors.New(structType.Name() + "." + field.Name + ": Projection is specified as keys_only but there are projected fields specified")
			}
			lsi.Projection.ProjectionType = aws.String(dynamodb.ProjectionTypeKeysOnly)
		case "all":
			if len(lsi.Projection.NonKeyAttributes) > 0 {
				return errors.New(structType.Name() + "." + field.Name + ": Projection is specified as all but there are projected fields specified")
			}
			lsi.Projection.ProjectionType = aws.String(dynamodb.ProjectionTypeAll)
		case "include":
			lsi.Projection.ProjectionType = aws.String(dynamodb.ProjectionTypeInclude)
		case "":
		// do nothing.
		default:
			return errors.New(structType.Name() + "." + field.Name + ": invalid projection type specified: " + projectionType)
		}
	}
	return nil
}

func processProjectFieldForLSI(structType reflect.Type, field reflect.StructField, fieldName string, lsi *dynamodb.LocalSecondaryIndex) error {
	if lsi.Projection == nil {
		lsi.Projection = new(dynamodb.Projection)
		lsi.Projection.NonKeyAttributes = make([]*string, 0, 20)
	}
	if lsi.Projection.ProjectionType != nil && *lsi.Projection.ProjectionType == dynamodb.ProjectionTypeKeysOnly {
		return errors.New(structType.Name() + "." + field.Name + ": Projection is specified as keys_only but there are projected fields specified")
	}
	if lsi.Projection.ProjectionType != nil && *lsi.Projection.ProjectionType == dynamodb.ProjectionTypeAll {
		return errors.New(structType.Name() + "." + field.Name + ": Projection is specified as all but there are projected fields specified")
	}
	lsi.Projection.ProjectionType = aws.String(dynamodb.ProjectionTypeInclude)
	lsi.Projection.NonKeyAttributes = append(lsi.Projection.NonKeyAttributes, aws.String(fieldName))
	return nil
}
