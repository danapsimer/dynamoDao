package dynamoDao

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

type TestSubStruct struct {
	G string
	H float64
	I uint `dynamoGSI:"Snafu,project"`
	K uint32
	L uint64
}

type TestStruct struct {
	A string  `dynamoKey:"hash" dynamodbav:"a" dynamoGSI:"Foo,hash"`
	B int64   `dynamoKey:"range" dynamodbav:",omitempty" dynamoGSI:"Foo,range;Snafu,hash,8,8;FooBar,hash"`
	C float32 `dynamodbav:"c" dynamoGSI:"Snafu,range;FooBar,range"`
	D bool
	E []byte        `dynamodbav:"e" dynamoGSI:"Snafu,project"`
	F TestSubStruct `dynamodbav:"f"`
	M string        `dynamodbav:"-"`
	N string        `dynamodbav:"n"`
}

func TestGlobalSecondaryIndexes(t *testing.T) {
	testStruct := &TestStruct{}
	gsi, err := globalIndexes(testStruct)
	require.Nil(t, err, "error in globalIndexes")
	require.NotNil(t, gsi)
	assert.Equal(t, 3, len(gsi))
	// Convert array to map
	gsiMap := make(map[string]*dynamodb.GlobalSecondaryIndex)
	for _, x := range gsi {
		gsiMap[*x.IndexName] = x
	}

	foo, ok := gsiMap["Foo"]
	require.True(t, ok)
	require.NotNil(t, foo)
	require.Equal(t, 2, len(foo.KeySchema))
	assert.Equal(t, dynamodb.KeyTypeHash, *foo.KeySchema[0].KeyType)
	assert.Equal(t, "a", *foo.KeySchema[0].AttributeName)
	assert.Equal(t, dynamodb.KeyTypeRange, *foo.KeySchema[1].KeyType)
	assert.Equal(t, "B", *foo.KeySchema[1].AttributeName)
	assert.Empty(t, foo.Projection.NonKeyAttributes)
	assert.Equal(t, dynamodb.ProjectionTypeAll, *foo.Projection.ProjectionType)
	assert.EqualValues(t, 5, *foo.ProvisionedThroughput.ReadCapacityUnits)
	assert.EqualValues(t, 1, *foo.ProvisionedThroughput.WriteCapacityUnits)

	snafu, ok := gsiMap["Snafu"]
	require.True(t, ok)
	require.NotNil(t, snafu)
	require.Equal(t, 2, len(snafu.KeySchema))
	assert.Equal(t, dynamodb.KeyTypeHash, *snafu.KeySchema[0].KeyType)
	assert.Equal(t, "B", *snafu.KeySchema[0].AttributeName)
	assert.Equal(t, dynamodb.KeyTypeRange, *snafu.KeySchema[1].KeyType)
	assert.Equal(t, "c", *snafu.KeySchema[1].AttributeName)
	require.Equal(t, 2, len(snafu.Projection.NonKeyAttributes))
	assert.Equal(t, "e", *snafu.Projection.NonKeyAttributes[0])
	assert.Equal(t, "f.I", *snafu.Projection.NonKeyAttributes[1])
	assert.Equal(t, dynamodb.ProjectionTypeInclude, *snafu.Projection.ProjectionType)
	assert.EqualValues(t, 8, *snafu.ProvisionedThroughput.ReadCapacityUnits)
	assert.EqualValues(t, 8, *snafu.ProvisionedThroughput.WriteCapacityUnits)

	foobar, ok := gsiMap["FooBar"]
	require.True(t, ok)
	require.NotNil(t, foobar)
	require.Equal(t, 2, len(foobar.KeySchema))
	assert.Equal(t, dynamodb.KeyTypeHash, *foobar.KeySchema[0].KeyType)
	assert.Equal(t, "B", *foobar.KeySchema[0].AttributeName)
	assert.Equal(t, dynamodb.KeyTypeRange, *foobar.KeySchema[1].KeyType)
	assert.Equal(t, "c", *foobar.KeySchema[1].AttributeName)
	assert.Empty(t, foo.Projection.NonKeyAttributes)
	assert.Equal(t, dynamodb.ProjectionTypeAll, *foobar.Projection.ProjectionType)
	assert.EqualValues(t, 5, *foobar.ProvisionedThroughput.ReadCapacityUnits)
	assert.EqualValues(t, 1, *foobar.ProvisionedThroughput.WriteCapacityUnits)
}
