package dynamoDao

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestKeySchema(t *testing.T) {
	testStruct := &TestStruct{}
	keySchema, thruput, err := keySchema(testStruct)
	require.Nil(t, err, "Error occurred in keySchema")
	require.NotNil(t, keySchema)
	require.Equal(t, 2, len(keySchema))
	assert.Equal(t, "a", *keySchema[0].AttributeName)
	assert.Equal(t, dynamodb.KeyTypeHash, *keySchema[0].KeyType)
	assert.Equal(t, "B", *keySchema[1].AttributeName)
	assert.Equal(t, dynamodb.KeyTypeRange, *keySchema[1].KeyType)
	assert.Nil(t, thruput)
}

type TestStructWithProvisionedThroughput struct {
	A string  `dynamoKey:"hash,8,4" dynamodbav:"a" dynamoGSI:"Foo,hash;Bar,hash,10,10,keys_only"`
	B int64   `dynamoKey:"range" dynamodbav:",omitempty" dynamoGSI:"Foo,range;Snafu,hash,8,8;FooBar,hash"`
	C float32 `dynamodbav:"c" dynamoGSI:"Snafu,range;FooBar,range"`
	D bool    `dynamoGSI:"Bar,range;Snafu,project"`
	E []byte  `dynamodbav:"e" dynamoGSI:"Snafu,project"`
	M string  `dynamodbav:"-"`
}

func TestKeySchemaWithProvisionedThroughput(t *testing.T) {
	testStruct := &TestStructWithProvisionedThroughput{}
	keySchema, thruput, err := keySchema(testStruct)
	require.Nil(t, err, "Error occurred in keySchema")
	require.NotNil(t, keySchema)
	require.Equal(t, 2, len(keySchema))
	assert.Equal(t, "a", *keySchema[0].AttributeName)
	assert.Equal(t, dynamodb.KeyTypeHash, *keySchema[0].KeyType)
	assert.Equal(t, "B", *keySchema[1].AttributeName)
	assert.Equal(t, dynamodb.KeyTypeRange, *keySchema[1].KeyType)
	require.NotNil(t, thruput)
	assert.EqualValues(t, 8, *thruput.ReadCapacityUnits)
	assert.EqualValues(t, 4, *thruput.WriteCapacityUnits)
}
