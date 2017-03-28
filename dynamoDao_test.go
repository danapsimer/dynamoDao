package dynamoDao

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
	"code.bluesoftdev.com/v1/repos/dynamoDao/uuid"
)

var awsConfig = aws.NewConfig().
	WithCredentials(credentials.NewStaticCredentials("DUMMY_ACCESS_ID", "DUMMY_SECRET", "")).
	WithDisableSSL(true).
	WithEndpoint("http://127.0.0.1:8000").
	WithRegion("local")

func resetTable(t *testing.T) {
	session := session.New(awsConfig)
	client := dynamodb.New(session)
	_, err := client.DescribeTable(&dynamodb.DescribeTableInput{TableName: aws.String("DynamoDao.Test")})
	if err == nil {
		_, err := client.DeleteTable(&dynamodb.DeleteTableInput{TableName: aws.String("DynamoDao.Test")})
		if err != nil {
			t.Fatal(err)
		}
	}
}

type Struct1 struct {
	Id          *uuid.UUID `dynamodbav:"person_id" dynamoKey:"hash"`
	OrgId       uuid.UUID  `dynamodbav:"organization_id" dynamoGSI:"PhoneNumberIdx,hash"`
	Name        string     `dynamodbav:"name"`
	PhoneNumber string     `dynamodbav:"phone_number" dynamoGSI:"PhoneNumberIdx,range"`
}

type Struct2 struct {
	Id          *uuid.UUID `dynamodbav:"person_id" dynamoKey:"hash"`
	OrgId       uuid.UUID  `dynamodbav:"organization_id" dynamoGSI:"PhoneNumberIdx,hash"`
	Name        string     `dynamodbav:"name" dynamoKey:"range" dynamoGSI:"NameDOBIdx,range"`
	PhoneNumber string     `dynamodbav:"phone_number" dynamoGSI:"PhoneNumberIdx,range"`
	DateOfBirth time.Time  `dynamodbav:"date_of_birth,unixtime" dynamoGSI:"NameDOBIdx,hash,8,4"`
}

type TestDao struct {
	DynamoDBDao
}

func TestDynamoDBDao_CreateOrUpdateTable(t *testing.T) {
	resetTable(t)
	session := session.New(awsConfig)
	client := dynamodb.New(session)
	dao := &TestDao{
		DynamoDBDao{
			client:    client,
			tableName: "DynamoDao.Test",
		},
	}

	p := dao.CreateOrUpdateTable(&Struct1{})
	err := <-p
	require.Nil(t, err)
	dt1, err := client.DescribeTable(&dynamodb.DescribeTableInput{TableName: aws.String("DynamoDao.Test")})
	require.Nil(t, err)
	require.NotNil(t, dt1)
	// Check First Describe Table:
	{
		assert.Equal(t, 4, len(dt1.Table.AttributeDefinitions))
		attrMap1 := make(map[string]*dynamodb.AttributeDefinition)
		for _, a := range dt1.Table.AttributeDefinitions {
			attrMap1[*a.AttributeName] = a
		}
		personId1, ok := attrMap1["person_id"]
		require.True(t, ok)
		require.NotNil(t, personId1)
		assert.Equal(t, "B", *personId1.AttributeType)
		orgId1, ok := attrMap1["organization_id"]
		require.True(t, ok)
		require.NotNil(t, orgId1)
		assert.Equal(t, "B", *orgId1.AttributeType)
		name1, ok := attrMap1["name"]
		require.True(t, ok)
		require.NotNil(t, name1)
		assert.Equal(t, "S", *name1.AttributeType)
		phoneNumber1, ok := attrMap1["phone_number"]
		require.True(t, ok)
		require.NotNil(t, phoneNumber1)
		assert.Equal(t, "S", *phoneNumber1.AttributeType)

		assert.Equal(t, 1, len(dt1.Table.GlobalSecondaryIndexes))
		assert.Equal(t, "PhoneNumberIdx", *dt1.Table.GlobalSecondaryIndexes[0].IndexName)
		assert.Equal(t, "organization_id", *dt1.Table.GlobalSecondaryIndexes[0].KeySchema[0].AttributeName)
		assert.Equal(t, dynamodb.KeyTypeHash, *dt1.Table.GlobalSecondaryIndexes[0].KeySchema[0].KeyType)
		assert.Equal(t, "phone_number", *dt1.Table.GlobalSecondaryIndexes[0].KeySchema[1].AttributeName)
		assert.Equal(t, dynamodb.KeyTypeRange, *dt1.Table.GlobalSecondaryIndexes[0].KeySchema[1].KeyType)

		assert.Nil(t, dt1.Table.LocalSecondaryIndexes)

		assert.EqualValues(t, 5, *dt1.Table.ProvisionedThroughput.ReadCapacityUnits)
		assert.EqualValues(t, 1, *dt1.Table.ProvisionedThroughput.WriteCapacityUnits)

		assert.Nil(t, dt1.Table.StreamSpecification)
		//assert.False(t, *dt1.Table.StreamSpecification.StreamEnabled)
	}

	p = dao.CreateOrUpdateTable(&Struct2{})
	err = <-p
	require.Nil(t, err)
	dt2, err := client.DescribeTable(&dynamodb.DescribeTableInput{TableName: aws.String("DynamoDao.Test")})
	require.Nil(t, err)
	require.NotNil(t, dt2)
	{
		assert.Equal(t, 5, len(dt2.Table.AttributeDefinitions))
		attrMap2 := make(map[string]*dynamodb.AttributeDefinition)
		for _, a := range dt2.Table.AttributeDefinitions {
			attrMap2[*a.AttributeName] = a
		}
		dateOfBirth2, ok := attrMap2["date_of_birth"]
		require.True(t, ok)
		require.NotNil(t, dateOfBirth2)
		assert.Equal(t, "N", *dateOfBirth2.AttributeType)

		assert.Equal(t, 2, len(dt2.Table.GlobalSecondaryIndexes))
		gsiMap2 := make(map[string]*dynamodb.GlobalSecondaryIndexDescription)
		for _, gsi := range dt2.Table.GlobalSecondaryIndexes {
			gsiMap2[*gsi.IndexName] = gsi
		}
		nameDobIdx2, ok := gsiMap2["NameDOBIdx"]
		require.True(t, ok)
		require.NotNil(t, nameDobIdx2)

		assert.Equal(t, "date_of_birth", *nameDobIdx2.KeySchema[0].AttributeName)
		assert.Equal(t, dynamodb.KeyTypeHash, *nameDobIdx2.KeySchema[0].KeyType)
		assert.Equal(t, "name", *nameDobIdx2.KeySchema[1].AttributeName)
		assert.Equal(t, dynamodb.KeyTypeRange, *nameDobIdx2.KeySchema[1].KeyType)

		assert.EqualValues(t, 8, *nameDobIdx2.ProvisionedThroughput.ReadCapacityUnits)
		assert.EqualValues(t, 4, *nameDobIdx2.ProvisionedThroughput.WriteCapacityUnits)
	}

	dao.readCapacity = 10
	dao.writeCapacity = 5

	p = dao.CreateOrUpdateTable(&Struct2{})
	err = <-p
	require.Nil(t, err)
	dt3, err := client.DescribeTable(&dynamodb.DescribeTableInput{TableName: aws.String("DynamoDao.Test")})
	require.Nil(t, err)
	require.NotNil(t, dt3)
	assert.EqualValues(t, 10, *dt3.Table.ProvisionedThroughput.ReadCapacityUnits)
	assert.EqualValues(t, 5, *dt3.Table.ProvisionedThroughput.WriteCapacityUnits)

	dao.enableStreaming = true
	dao.streamViewType = dynamodb.StreamViewTypeNewImage

	p = dao.CreateOrUpdateTable(&Struct2{})
	err = <-p
	require.Nil(t, err)
	dt4, err := client.DescribeTable(&dynamodb.DescribeTableInput{TableName: aws.String("DynamoDao.Test")})
	require.Nil(t, err)
	require.NotNil(t, dt4)
	assert.True(t, *dt4.Table.StreamSpecification.StreamEnabled)
	assert.Equal(t, dynamodb.StreamViewTypeNewImage, *dt4.Table.StreamSpecification.StreamViewType)

	dao.enableStreaming = false
	p = dao.CreateOrUpdateTable(&Struct2{})
	err = <-p
	require.Nil(t, err)
	dt5, err := client.DescribeTable(&dynamodb.DescribeTableInput{TableName: aws.String("DynamoDao.Test")})
	require.Nil(t, err)
	require.NotNil(t, dt5)
	assert.Nil(t, dt5.Table.StreamSpecification)

	dao.enableStreaming = true
	dao.streamViewType = dynamodb.StreamViewTypeKeysOnly
	p = dao.CreateOrUpdateTable(&Struct2{})
	err = <-p
	require.Nil(t, err)
	dt6, err := client.DescribeTable(&dynamodb.DescribeTableInput{TableName: aws.String("DynamoDao.Test")})
	require.Nil(t, err)
	require.NotNil(t, dt6)

	assert.True(t, *dt6.Table.StreamSpecification.StreamEnabled)
	assert.Equal(t, dynamodb.StreamViewTypeKeysOnly, *dt6.Table.StreamSpecification.StreamViewType)
}

func setup(t *testing.T) *TestDao {
	resetTable(t)
	session := session.New(awsConfig)
	client := dynamodb.New(session)
	dao := &TestDao{
		DynamoDBDao{
			client:    client,
			tableName: "DynamoDao.Test",
		},
	}

	p := dao.CreateOrUpdateTable(&Struct1{})
	err := <-p
	require.Nil(t, err)
	return dao
}

func TestDynamoDBDao_PutItem(t *testing.T) {
	dao := setup(t)

	id := uuid.NewV4()
	orgId := uuid.NewV4()
	saved, err := dao.PutItem(&Struct1{
		Id:          &id,
		OrgId:       orgId,
		Name:        "Joe Blow",
		PhoneNumber: "4045551212",
	})
	require.Nil(t, err)
	require.NotNil(t, saved)

	savedStruct1, ok := saved.(*Struct1)
	require.True(t, ok)
	assert.Equal(t, id, *savedStruct1.Id)
	assert.Equal(t, orgId, savedStruct1.OrgId)
	assert.Equal(t, "Joe Blow", savedStruct1.Name)
	assert.Equal(t, "4045551212", savedStruct1.PhoneNumber)

	retrieved, err := dao.GetItem(&Struct1{
		Id: savedStruct1.Id,
	})
	require.Nil(t, err)
	require.NotNil(t, retrieved)
	retrievedStruct1, ok := retrieved.(*Struct1)
	require.True(t, ok)
	assert.Equal(t, *savedStruct1.Id, *retrievedStruct1.Id)
	assert.Equal(t, savedStruct1.OrgId, retrievedStruct1.OrgId)
	assert.Equal(t, savedStruct1.Name, retrievedStruct1.Name)
	assert.Equal(t, savedStruct1.PhoneNumber, retrievedStruct1.PhoneNumber)
}
