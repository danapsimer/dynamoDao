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
	client          *dynamodb.DynamoDB
	tableName       string
	readCapacity    int64
	writeCapacity   int64
	enableStreaming bool
	streamViewType  string
	keyAttrNames    []string
}

func NewDynamoDBDao(sess *session.Session, tableName string, readCapacity, writeCapacity int64, enableStreaming bool, streamViewType string) (*DynamoDBDao, error) {
	return &DynamoDBDao{
		client:          dynamodb.New(sess),
		tableName:       tableName,
		readCapacity:    readCapacity,
		writeCapacity:   writeCapacity,
		enableStreaming: enableStreaming,
		streamViewType:  streamViewType,
	}, nil
}

func NewDynamoDBDaoForType(sess *session.Session, typ reflect.Type) (*DynamoDBDao, error) {
	dao, err := NewDynamoDBDao(sess, typ.Name(), 0, 0, false, "")
	if err != nil {
		return nil, err
	}
	promise := dao.CreateOrUpdateTable(typ)
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
	attrVals, err := dynamodbattribute.MarshalMap(t)
	if err != nil {
		return nil, err
	}

	putItem := new(dynamodb.PutItemInput).SetItem(attrVals).SetTableName(dao.tableName).
		SetReturnValues(dynamodb.ReturnValueNone)

	_, err = dao.client.PutItem(putItem)
	if err != nil {
		if awserr, ok := err.(awserr.Error); ok {
			log.Printf("ERROR: %+v", awserr)
			return nil, awserr
		}
		return nil, err
	}

	return t, nil
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
	updateItem := new(dynamodb.UpdateItemInput).SetKey(keyVals).SetTableName(dao.tableName).
		SetAttributeUpdates(itemUpdates).SetReturnValues(dynamodb.ReturnValueAllNew)

	updateItemResponse, err := dao.client.UpdateItem(updateItem)
	if err != nil {
		log.Printf("ERROR: %+v: %+v", err, updateItemResponse)
		return nil, err
	}
	typ := reflect.ValueOf(t).Elem().Type()
	newT := reflect.New(typ).Elem().Interface()
	ptrT := to_struct_ptr(newT)
	err = dynamodbattribute.UnmarshalMap(updateItemResponse.Attributes, ptrT)
	if err != nil {
		log.Printf("ERROR: %+v: %+v", err, updateItemResponse)
		return nil, err
	}
	return ptrT, nil
}

func (dao *DynamoDBDao) GetItem(key interface{}) (interface{}, error) {

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

	getItem := new(dynamodb.GetItemInput).SetTableName(dao.tableName).SetKey(keyAttrs)

	response, err := dao.client.GetItem(getItem)
	if err != nil {
		log.Printf("ERROR: %+v: %+v", err, getItem)
		return nil, err
	}
	typ := reflect.ValueOf(key).Elem().Type()
	newT := reflect.New(typ).Elem().Interface()
	ptrT := to_struct_ptr(newT)
	err = dynamodbattribute.UnmarshalMap(response.Item, ptrT)
	if err != nil {
		log.Printf("ERROR: %+v: %+v", err, response)
		return nil, err
	}
	return ptrT, nil
}

func (dao *DynamoDBDao) DeleteItem(key interface{}) (interface{}, error) {

	keyAttrs, err := dynamodbattribute.MarshalMap(key)
	if err != nil {
		log.Printf("ERROR: %+v: %+v", err, key)
		return nil, err
	}
	if dao.keyAttrNames != nil && len(dao.keyAttrNames) > 0 {
		newKeyAttrs := make(map[string]*dynamodb.AttributeValue)
		for _, k := range dao.keyAttrNames {
			newKeyAttrs[k] = keyAttrs[k]
		}
		keyAttrs = newKeyAttrs
	}

	deleteItem := new(dynamodb.DeleteItemInput).SetTableName(dao.tableName).SetKey(keyAttrs).
		SetReturnValues(dynamodb.ReturnValueAllOld)

	response, err := dao.client.DeleteItem(deleteItem)
	if err != nil {
		log.Printf("ERROR: %+v: %+v", err, response)
		return nil, err
	}
	typ := reflect.ValueOf(key).Elem().Type()
	newT := reflect.New(typ).Elem().Interface()
	ptrT := to_struct_ptr(newT)

	err = dynamodbattribute.UnmarshalMap(response.Attributes, ptrT)
	if err != nil {
		log.Printf("ERROR: %+v: %+v", err, response)
		return nil, err
	}
	return ptrT, nil
}

func to_struct_ptr(obj interface{}) interface{} {
	vp := reflect.New(reflect.TypeOf(obj))
	vp.Elem().Set(reflect.ValueOf(obj))
	return vp.Interface()
}
