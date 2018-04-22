package dynamoDao

import (
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"log"
	"reflect"
	"strconv"
	"time"
)

func (dao *DynamoDBDao) CreateOrUpdateTable(t interface{}) chan error {
	return dao.CreateOrUpdateTableForType(getStructType(t))
}

// Creates or Updates a table's schema based on the schema found in dynamodb and the schema extracted from the struct
// tags within the given structure.  The struct tags supported by the Marshal function of the
// github.com/aws/aws-sdk-go/service/dynamodb/dynamodbav package are supported.  These are used to find any naming
// aliases that are there and construct a list of all scalar attributes in the root object (we do not descend into
// structs or arrays).
//
// The CreateTableInput structure used for creating a table in DyanamoDB has 4 major parts: Attributes, KeySchema,
// GlobalSecondaryIndexes, LocalSecondaryIndexes, and ProvisionedThroughput.
//
// Attributes:
// ===========
// The attributes are determined by scanning the struct type and creating AttributeDefinition objects for each one that
// has a scalar value.  It handles []byte (e.g. []uint8) as 'B' (Binary) data, strings as 'S' (String) data, and int,
// int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, and bool as 'N' (Number) data.
// Note: if a time.Time field has the 'unixtime' type in the 'dynamodbav' tag, that field will be mapped as a scalar 'N'
// (Number).  The name of the attribute is either the name of the field or the alias given in the 'dynamodbav' tag.
//
// KeySchema:
// ==========
// The create table function only allows 2 schema fields: one hash and one range. To mark your hash and range attributes
// add a 'dynamoKey' tag to your struct's attribute:
//
// FieldA string `dynamoKey:"hash"`  // Marks this as the hash key, there can be only one
// FieldB string `dynamoKey:"range"` // Marks this as the range key, there can be only one
//
// You can also specify the default provisioned throughput for the table by adding 2 integers to the hash key tag:
//
// FieldA string `dynamoKey:"hash,25,10"` // Sets the default provisioned through put for read and write respectively.
//
// As with AttributeDefinitions, the 'dyanamodbav' tag is checked for field aliases.
//
// GlobalSecondaryIndexes:
// =======================
// These are defined by the 'dynamoGSI' tag.  The format for the tag is as follows:
//
// gsi-tag:=gsi-spec[;gsi-tag]
// gsi-spec:=name,role[,[read-cap,write-cap][,projection-type]]
// name:=[0-9A-Za-z_.-]+
// role:=hash|range|project
// read-cap:=''|integer
// write-cap:=''|integer
// integer:=[0-9]+
// projection-type:=keys_only,all,include
//
// The readCap, writeCap, and projection-type elements are ignored unless the role is 'hash'
//
// The projection-type defaults to "all" if there are no fields with the 'project' role.
// The projection-type defaults to "include" if there are fields with the 'project' role.
//
// As with AttributeDefinitions, the 'dyanamodbav' tag is checked for field aliases.
//
// For example the following struct would define 3 GSIs:
//   1. IndexA with 2 projected attributes and R/W capacity of 10 & 5 respectively.
//   2. IndexB with keys_only and the default R/W capacity (5, 1)
//   3. IndexC with all attributes projected and the default R/W capacity (5, 1)
//
// type MyStruct struct {
//   A string `dynamoGSI:"IndexA,hash,10,5"`
//   B string `dynamoGSI:"IndexA,range;IndexB,hash,,,keys_only;IndexC,hash"`
//   C string `dynamoGSI:"IndexA,project;IndexB,range"`
//   D string `dynamoGSI:'IndexA,project;IndexC,range"`
// }
//
// LocalSecondaryIndexes:
// ======================
// These are defined by the 'dynamoLSI' tag.  The format for the tag is as follows:
//
// lsi-tag:=lsi-spec[;gsi-tag]
// lsi-spec:=name,role[,projection-type]
// name:=[0-9A-Za-z_.-]+
// role:=range|project
// projection-type:=keys_only,all,include
//
// Since the local indexes must have the same hash as the base table, the hash element is taken from the KeySchema.
//
// The projection-type is ignored unless it is provided on the 'range' field.
// The projection-type defaults to "all" if there are no fields with the 'project' role.
// The projection-type defaults to "include" if there are fields with the 'project' role.
//
// As with AttributeDefinitions, the 'dyanamodbav' tag is checked for field aliases.
func (dao *DynamoDBDao) CreateOrUpdateTableForType(structType reflect.Type) chan error {
	promise := make(chan error, 1)
	go func() {
		dao.structType = structType
		dao.extractTableDescription()
		dao.createOrUpdateTable(dao.tableDescription, promise)
	}()
	return promise
}

func (dao *DynamoDBDao) extractTableDescription() error {
	keySchema, thruput, err := keySchemaForType(dao.structType)
	if err != nil {
		return err
	}
	dao.keyAttrNames = make([]string, 0, 2)
	dao.keyAttrNames = append(dao.keyAttrNames, *keySchema[0].AttributeName)
	if len(keySchema) > 1 {
		dao.keyAttrNames = append(dao.keyAttrNames, *keySchema[1].AttributeName)
	}
	if dao.readCapacity != 0 && dao.writeCapacity != 0 {
		thruput = &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(dao.readCapacity),
			WriteCapacityUnits: aws.Int64(dao.writeCapacity),
		}
	} else if thruput == nil {
		thruput = &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(1),
		}
	}
	globalIndexes, err := globalIndexesForType(dao.structType)
	if err != nil {
		return err
	}
	localIndexes, err := localIndexesForType(dao.structType, keySchema[0])
	if err != nil {
		return err
	}
	allKeyAttrNames := collectUniqueKeyNames(keySchema, globalIndexes, localIndexes)
	attributes, attrToField, err := attributeDefinitionsForType(dao.structType, allKeyAttrNames)
	if err != nil {
		return err
	}
	dao.tableDescription = &dynamodb.CreateTableInput{
		AttributeDefinitions:   attributes,
		KeySchema:              keySchema,
		GlobalSecondaryIndexes: globalIndexes,
		LocalSecondaryIndexes:  localIndexes,
		TableName:              aws.String(dao.TableName),
		ProvisionedThroughput:  thruput,
	}
	if dao.enableStreaming {
		dao.tableDescription.StreamSpecification = &dynamodb.StreamSpecification{
			StreamEnabled:  aws.Bool(dao.enableStreaming),
			StreamViewType: aws.String(dao.streamViewType),
		}
	}
	dao.attrToField = attrToField
	return nil
}

func collectUniqueKeyNames(key []*dynamodb.KeySchemaElement, gsi []*dynamodb.GlobalSecondaryIndex, lsi []*dynamodb.LocalSecondaryIndex) map[string]interface{} {
	uniqueKeyNames := make(map[string]interface{})
	for _, keyAttr := range key {
		uniqueKeyNames[*keyAttr.AttributeName] = nil
	}
	for _, gsidx := range gsi {
		for _, gsikey := range gsidx.KeySchema {
			uniqueKeyNames[*gsikey.AttributeName] = nil
		}
	}
	for _, lsidx := range lsi {
		for _, lsikey := range lsidx.KeySchema {
			uniqueKeyNames[*lsikey.AttributeName] = nil
		}
	}
	return uniqueKeyNames
}

func (dao *DynamoDBDao) createOrUpdateTable(createTableInput *dynamodb.CreateTableInput, promise chan error) chan error {
	go func() {
		exists := true
		describeTableRequest := new(dynamodb.DescribeTableInput).SetTableName(*createTableInput.TableName)
		describeTableResponse, err := dao.Client.DescribeTable(describeTableRequest)
		if err != nil {
			if awsErr, ok := err.(awserr.Error); ok {
				if awsErr.Code() != "ResourceNotFoundException" {
					log.Printf("error from Describe Table call: %s", err.Error())
					promise <- err
					return
				}
				exists = false
			} else {
				log.Printf("non-aws error from describe table call: %s", err.Error())
				promise <- err
				return
			}
		}
		if !exists {
			err = dao.createTable(createTableInput, promise)
			if err != nil {
				// Already sent error
				return
			}
		} else {
			err = dao.updateTable(createTableInput, describeTableResponse, promise)
			if err != nil {
				return
			}
		}
		promise <- nil
	}()
	return promise
}

func (dao *DynamoDBDao) createTable(createTableInput *dynamodb.CreateTableInput, promise chan error) error {
	_, err := dao.Client.CreateTable(createTableInput)
	if err != nil {
		promise <- errors.New(fmt.Sprintf("error occurred while creating table: %+v: %s",
			createTableInput.GoString(), err.Error()))
		return err
	}
	return dao.awaitTableStatusActive(*createTableInput.TableName, promise)
}

func (dao *DynamoDBDao) updateTable(newSchema *dynamodb.CreateTableInput, currentSchema *dynamodb.DescribeTableOutput,
	promise chan error) error {
	err := dao.updateProvisionedThroughputIfNeeded(newSchema, currentSchema, promise)
	if err != nil {
		return err
	}
	err = dao.updateStreamingSpecIfNeeded(newSchema, currentSchema, promise)
	if err != nil {
		return err
	}
	return dao.updateGlobalSecondaryIndexesIfNeeded(newSchema, currentSchema, promise)
}

func (dao *DynamoDBDao) updateProvisionedThroughputIfNeeded(newSchema *dynamodb.CreateTableInput, currentSchema *dynamodb.DescribeTableOutput, promise chan error) error {
	if *newSchema.ProvisionedThroughput.ReadCapacityUnits != *currentSchema.Table.ProvisionedThroughput.ReadCapacityUnits ||
		*newSchema.ProvisionedThroughput.WriteCapacityUnits != *currentSchema.Table.ProvisionedThroughput.WriteCapacityUnits {
		updateTableInput := new(dynamodb.UpdateTableInput).
			SetAttributeDefinitions(newSchema.AttributeDefinitions).
			SetTableName(*newSchema.TableName)
		updateTableInput = updateTableInput.SetProvisionedThroughput(newSchema.ProvisionedThroughput)
		_, err := dao.Client.UpdateTable(updateTableInput)
		if err != nil {
			err = errors.New(fmt.Sprintf("error occurred while updating table: %+v: %s", newSchema, err.Error()))
			promise <- err
			return err
		}
		err = dao.awaitTableStatusActive(*newSchema.TableName, promise)
		if err != nil {
			// error already sent to promise channel
			return err
		}
	}
	return nil
}

func (dao *DynamoDBDao) updateStreamingSpecIfNeeded(newSchema *dynamodb.CreateTableInput, currentSchema *dynamodb.DescribeTableOutput, promise chan error) error {

	if streamSpecChanged(newSchema.StreamSpecification, currentSchema.Table.StreamSpecification) {
		updateTableInput := new(dynamodb.UpdateTableInput).
			SetAttributeDefinitions(newSchema.AttributeDefinitions).
			SetTableName(*newSchema.TableName)
		streamSpec := newSchema.StreamSpecification
		if streamSpec == nil {
			streamSpec = &dynamodb.StreamSpecification{StreamEnabled: aws.Bool(false)}
		}
		updateTableInput = updateTableInput.SetStreamSpecification(streamSpec)
		_, err := dao.Client.UpdateTable(updateTableInput)
		if err != nil {
			promise <- errors.New(fmt.Sprintf("error occurred while updating table: %+v: %s", newSchema, err.Error()))
			return err
		}
		err = dao.awaitTableStatusActive(*newSchema.TableName, promise)
		if err != nil {
			// error already sent to promise channel
			return err
		}
	}
	return nil
}

func (dao *DynamoDBDao) updateGlobalSecondaryIndexesIfNeeded(newSchema *dynamodb.CreateTableInput, currentSchema *dynamodb.DescribeTableOutput, promise chan error) error {
	actions := extractIndexChanges(newSchema, currentSchema)
	updateTableInput := new(dynamodb.UpdateTableInput).
		SetAttributeDefinitions(newSchema.AttributeDefinitions).
		SetTableName(*newSchema.TableName)
	for _, action := range actions {
		updateTableInput = updateTableInput.SetGlobalSecondaryIndexUpdates([]*dynamodb.GlobalSecondaryIndexUpdate{action})
		_, err := dao.Client.UpdateTable(updateTableInput)
		if err != nil {
			promise <- errors.New(fmt.Sprintf("error occurred while updating table: %+v: %s", actions, err.Error()))
			return err
		}
		var indexName string
		if action.Create != nil {
			indexName = *action.Create.IndexName
		} else if action.Update != nil {
			indexName = *action.Update.IndexName
		} else if action.Delete != nil {
			indexName = *action.Delete.IndexName
		}
		err = dao.awaitTableIndexStatusActive(*newSchema.TableName, indexName, promise)
		if err != nil {
			// error already sent to promise channel
			return err
		}
	}
	return nil
}

func extractIndexChanges(newSchema *dynamodb.CreateTableInput, currentSchema *dynamodb.DescribeTableOutput) []*dynamodb.GlobalSecondaryIndexUpdate {
	actions := make([]*dynamodb.GlobalSecondaryIndexUpdate, 0,
		len(currentSchema.Table.GlobalSecondaryIndexes)+len(newSchema.GlobalSecondaryIndexes))
	toCreateIndexes := make([]bool, len(newSchema.GlobalSecondaryIndexes))
	for i := range toCreateIndexes {
		toCreateIndexes[i] = true
	}
	for _, gsi := range currentSchema.Table.GlobalSecondaryIndexes {
		var match *dynamodb.GlobalSecondaryIndex = nil
		var matchIndex int
		for i, ctgsi := range newSchema.GlobalSecondaryIndexes {
			if *ctgsi.IndexName == *gsi.IndexName {
				match = ctgsi
				matchIndex = i
				break
			}
		}
		if match != nil {
			toCreateIndexes[matchIndex] = false
			if *match.ProvisionedThroughput.WriteCapacityUnits != *gsi.ProvisionedThroughput.WriteCapacityUnits ||
				*match.ProvisionedThroughput.ReadCapacityUnits != *gsi.ProvisionedThroughput.ReadCapacityUnits {
				actions = append(actions, new(dynamodb.GlobalSecondaryIndexUpdate).
					SetUpdate(new(dynamodb.UpdateGlobalSecondaryIndexAction).
						SetIndexName(*gsi.IndexName).
						SetProvisionedThroughput(match.ProvisionedThroughput)))
			}
		} else {
			actions = append(actions, new(dynamodb.GlobalSecondaryIndexUpdate).
				SetDelete(new(dynamodb.DeleteGlobalSecondaryIndexAction).SetIndexName(*gsi.IndexName)))
		}
	}
	for idx, create := range toCreateIndexes {
		if create {
			gsi := newSchema.GlobalSecondaryIndexes[idx]
			actions = append(actions, new(dynamodb.GlobalSecondaryIndexUpdate).
				SetCreate(new(dynamodb.CreateGlobalSecondaryIndexAction).
					SetIndexName(*gsi.IndexName).
					SetProvisionedThroughput(gsi.ProvisionedThroughput).
					SetKeySchema(gsi.KeySchema).
					SetProjection(gsi.Projection)))
		}
	}
	return actions
}

func streamSpecChanged(newSchema *dynamodb.StreamSpecification, currentSchema *dynamodb.StreamSpecification) bool {
	if newSchema == nil && currentSchema != nil {
		return true
	}
	if newSchema != nil && currentSchema == nil {
		return true
	}
	return newSchema != nil && currentSchema != nil &&
		(*newSchema.StreamEnabled != *currentSchema.StreamEnabled ||
			*newSchema.StreamEnabled && *newSchema.StreamViewType != *currentSchema.StreamViewType)
}

func (dao *DynamoDBDao) awaitTableStatusActive(tableName string, promise chan error) error {
	status := ""
	tick := time.Tick(tableStatusCheckInterval)
	timeout := time.After(tableCreateActiveTimeout)
	for status != dynamodb.TableStatusActive {
		select {
		case <-tick:
		case <-timeout:
			err := errors.New(fmt.Sprintf("timeout while waiting for created table to become active: %s", tableName))
			promise <- err
			return err
		}
		describeTableRequest := new(dynamodb.DescribeTableInput).SetTableName(tableName)
		describeTableResponse, err := dao.Client.DescribeTable(describeTableRequest)
		if err != nil {
			if awsErr, ok := err.(awserr.Error); ok {
				if awsErr.Code() != "ResourceNotFoundException" {
					log.Printf("error from Describe Table call for verification: %s", err.Error())
					promise <- err
					return err
				}
			} else {
				promise <- err
				return err
			}
		}
		status = *describeTableResponse.Table.TableStatus
	}
	return nil
}

func (dao *DynamoDBDao) awaitTableIndexStatusActive(tableName string, indexName string, promise chan error) error {
	status := ""
	tick := time.Tick(tableStatusCheckInterval)
	timeout := time.After(tableCreateActiveTimeout)
	for status != dynamodb.IndexStatusActive {
		select {
		case <-tick:
		case <-timeout:
			err := errors.New(fmt.Sprintf("timeout while waiting for created table to become active: %s", tableName))
			promise <- err
			return err
		}
		describeTableRequest := new(dynamodb.DescribeTableInput).SetTableName(tableName)
		describeTableResponse, err := dao.Client.DescribeTable(describeTableRequest)
		if err != nil {
			if awsErr, ok := err.(awserr.Error); ok {
				if awsErr.Code() != "ResourceNotFoundException" {
					log.Printf("error from Describe Table call for verification: %s", err.Error())
					promise <- err
					return err
				}
			} else {
				promise <- err
				return err
			}
		}
		if describeTableResponse != nil {
			newStatus := ""
			for _, gsi := range describeTableResponse.Table.GlobalSecondaryIndexes {
				if *gsi.IndexName == indexName {
					newStatus = *gsi.IndexStatus
					break
				}
			}
			//if newStatus == "" {
			//	err := errors.New(fmt.Sprintf("could not find index in describe table response: %s/%s", tableName, indexName))
			//	promise <- err
			//	return err
			//}
			if newStatus == "" {
				newStatus = "PENDING"
			}
			status = newStatus
		}
	}
	return nil
}

func parseCapacity(capStr string, def int64) (int64, error) {
	if capStr == "" {
		return def, nil
	} else {
		cap, err := strconv.Atoi(capStr)
		return int64(cap), err
	}
}
