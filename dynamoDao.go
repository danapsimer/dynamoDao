package dynamoDao

import (
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"log"
	"reflect"
	"time"
)

const (
	tableStatusCheckInterval = time.Duration(50 * time.Millisecond)
	tableCreateActiveTimeout = time.Duration(5 * time.Minute)

	keySchemaTag   = "dynamoKey"
	globalIndexTag = "dynamoGSI"
	localIndexTag  = "dynamoLSI"
)

type DynamoDBDao struct {
	Client           *dynamodb.DynamoDB
	TableName        string
	structType       reflect.Type
	readCapacity     int64
	writeCapacity    int64
	enableStreaming  bool
	streamViewType   string
	keyAttrNames     []string
	attrToField      map[string]*reflect.StructField
	tableDescription *dynamodb.CreateTableInput
}

func NewDynamoDBDao(sess *session.Session, tableName string, readCapacity, writeCapacity int64, enableStreaming bool, streamViewType string, structType reflect.Type) (*DynamoDBDao, error) {
	dao := &DynamoDBDao{
		Client:          dynamodb.New(sess),
		TableName:       tableName,
		readCapacity:    readCapacity,
		writeCapacity:   writeCapacity,
		enableStreaming: enableStreaming,
		streamViewType:  streamViewType,
		structType:      structType,
	}
	err := dao.extractTableDescription()
	if err != nil {
		return nil, err
	}
	return dao, nil
}

func NewDynamoDBDaoForType(sess *session.Session, typ reflect.Type) (*DynamoDBDao, error) {
	dao, err := NewDynamoDBDao(sess, typ.Name(), 0, 0, false, "", typ)
	if err != nil {
		return nil, err
	}
	promise := dao.CreateOrUpdateTableForType(typ)
	err = <-promise
	if err != nil {
		return nil, err
	}
	return dao, nil
}

func getStructType(t interface{}) reflect.Type {
	structType := reflect.TypeOf(t)
	if structType.Kind() == reflect.Ptr {
		structType = reflect.Indirect(reflect.ValueOf(t)).Type()
	}
	return structType
}

func (dao *DynamoDBDao) PutItem(t interface{}) (interface{}, error) {
	attrVals, err := dao.MarshalAttributes(t)
	if err != nil {
		return nil, err
	}

	putItem := new(dynamodb.PutItemInput).SetItem(attrVals).SetTableName(dao.TableName).
		SetReturnValues(dynamodb.ReturnValueNone)

	_, err = dao.Client.PutItem(putItem)
	if err != nil {
		if awserr, ok := err.(awserr.Error); ok {
			log.Printf("ERROR: %+v: %+v", awserr, putItem)
			return nil, awserr
		}
		return nil, err
	}

	return t, nil
}

func (dao *DynamoDBDao) MarshalAttributes(t interface{}) (map[string]*dynamodb.AttributeValue, error) {
	attrVals, err := dynamodbattribute.MarshalMap(t)
	if err != nil {
		return nil, err
	}
	return attrVals, nil
}

func (dao *DynamoDBDao) UnmarshalAttributes(attributes map[string]*dynamodb.AttributeValue) (interface{},error) {
	newT := reflect.New(dao.structType).Elem().Interface()
	ptrT := to_struct_ptr(newT)
	err := dynamodbattribute.UnmarshalMap(attributes, ptrT)
	if err != nil {
		return nil, err
	}
	return ptrT, nil
}

func (dao *DynamoDBDao) MarshalKey(key interface{}) (map[string]*dynamodb.AttributeValue, error) {
	keyAttrs, err := dynamodbattribute.MarshalMap(key)
	if err != nil {
		return nil, err
	}
	if dao.keyAttrNames != nil && len(dao.keyAttrNames) > 0 {
		newKeyAttrs := make(map[string]*dynamodb.AttributeValue)
		for _, k := range dao.keyAttrNames {
			newKeyAttrs[k] = keyAttrs[k]
		}
		keyAttrs = newKeyAttrs
	}
	return keyAttrs, nil
}

func (dao *DynamoDBDao) UpdateItem(t interface{}) (interface{}, error) {
	itemVals, err := dynamodbattribute.MarshalMap(t)
	if err != nil {
		return nil, err
	}
	keyVals := make(map[string]*dynamodb.AttributeValue)
	for _, k := range dao.keyAttrNames {
		keyVals[k] = itemVals[k]
		delete(itemVals, k)
	}
	itemUpdates := make(map[string]*dynamodb.AttributeValueUpdate)
	for k, a := range itemVals {
		itemUpdates[k] = new(dynamodb.AttributeValueUpdate).SetValue(a).SetAction(dynamodb.AttributeActionPut)
	}
	updateItem := new(dynamodb.UpdateItemInput).SetKey(keyVals).SetTableName(dao.TableName).
		SetAttributeUpdates(itemUpdates).SetReturnValues(dynamodb.ReturnValueAllNew)

	updateItemResponse, err := dao.Client.UpdateItem(updateItem)
	if err != nil {
		log.Printf("ERROR: %+v: %+v", err, updateItemResponse)
		return nil, err
	}
	ptrT, err := dao.UnmarshalAttributes(updateItemResponse.Attributes)
	if err != nil {
		log.Printf("ERROR: %+v: %+v", err, updateItemResponse)
		return nil, err
	}
	return ptrT, nil
}

func (dao *DynamoDBDao) GetItem(key interface{}) (interface{}, error) {

  keyAttrs, err := dao.MarshalKey(key)
	if err != nil {
		return nil, err
	}

	getItem := new(dynamodb.GetItemInput).SetTableName(dao.TableName).SetKey(keyAttrs)

	response, err := dao.Client.GetItem(getItem)
	if err != nil {
		log.Printf("ERROR: %+v: %+v", err, getItem)
		return nil, err
	}
	if len(response.Item) == 0 {
		return nil, nil
	}
	ptrT, err := dao.UnmarshalAttributes(response.Item)
	if err != nil {
		log.Printf("ERROR: %+v: %+v", err, response)
		return nil, err
	}
	return ptrT, nil
}

func (dao *DynamoDBDao) DeleteItem(key interface{}) (interface{}, error) {

	keyAttrs, err := dao.MarshalKey(key)
	if err != nil {
		log.Printf("ERROR: %+v: %+v", err, key)
		return nil, err
	}

	deleteItem := new(dynamodb.DeleteItemInput).SetTableName(dao.TableName).SetKey(keyAttrs).
		SetReturnValues(dynamodb.ReturnValueAllOld)

	response, err := dao.Client.DeleteItem(deleteItem)
	if err != nil {
		log.Printf("ERROR: %+v: %+v", err, response)
		return nil, err
	}
	if len(response.Attributes) > 0 {
		ptrT, err := dao.UnmarshalAttributes(response.Attributes)
		if err != nil {
			log.Printf("ERROR: %+v: %+v", err, response)
			return nil, err
		}
		return ptrT, nil
	}
	return nil, nil
}

func to_struct_ptr(obj interface{}) interface{} {
	vp := reflect.New(reflect.TypeOf(obj))
	vp.Elem().Set(reflect.ValueOf(obj))
	return vp.Interface()
}
