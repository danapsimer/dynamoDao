package dynamoDao

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

type TestSubStructWithLSI struct {
	G string `dynamoLSI:"bozo,range"`
	H float64
	I uint
	K uint32 `dynamoLSI:"bozo,project"`
	L uint64
}

type TestStructWithLSI struct {
	A string               `dynamoKey:"hash" dynamodbav:"a"`
	B int64                `dynamoKey:"range" dynamodbav:",omitempty"`
	C float32              `dynamodbav:"c" dynamoLSI:"fubar,range;fu,range,keys_only;snafu,range"`
	D bool                 `dynamoLSI:"snafu,project"`
	E []byte               `dynamodbav:"e" dynamoLSI:"snafu,project"`
	F TestSubStructWithLSI `dynamodbav:"f"`
	M string               `dynamodbav:"-"`
	N string               `dynamodbav:"n" dynamoLSI:"bozo,project"`
}

func TestLocalSecondaryIndexes(t *testing.T) {
	testStruct := &TestStructWithLSI{}
	gsi, err := localIndexes(testStruct, &dynamodb.KeySchemaElement{
		AttributeName: aws.String("a"),
		KeyType:       aws.String(dynamodb.KeyTypeHash),
	})
	require.Nil(t, err, "error in globalIndexes")
	require.NotNil(t, gsi)
	assert.Equal(t, 4, len(gsi))
	// Convert array to map
	lsiMap := make(map[string]*dynamodb.LocalSecondaryIndex)
	for _, x := range gsi {
		lsiMap[*x.IndexName] = x
	}

	fubar, ok := lsiMap["fubar"]
	require.True(t, ok)
	require.NotNil(t, fubar)
	require.Equal(t, 2, len(fubar.KeySchema))
	assert.Equal(t, dynamodb.KeyTypeHash, *fubar.KeySchema[0].KeyType)
	assert.Equal(t, "a", *fubar.KeySchema[0].AttributeName)
	assert.Equal(t, dynamodb.KeyTypeRange, *fubar.KeySchema[1].KeyType)
	assert.Equal(t, "c", *fubar.KeySchema[1].AttributeName)
	assert.Empty(t, fubar.Projection.NonKeyAttributes)
	assert.Equal(t, dynamodb.ProjectionTypeAll, *fubar.Projection.ProjectionType)

	fu, ok := lsiMap["fu"]
	require.True(t, ok)
	require.NotNil(t, fu)
	require.Equal(t, 2, len(fu.KeySchema))
	assert.Equal(t, dynamodb.KeyTypeHash, *fu.KeySchema[0].KeyType)
	assert.Equal(t, "a", *fu.KeySchema[0].AttributeName)
	assert.Equal(t, dynamodb.KeyTypeRange, *fu.KeySchema[1].KeyType)
	assert.Equal(t, "c", *fu.KeySchema[1].AttributeName)
	assert.Empty(t, fu.Projection.NonKeyAttributes)
	assert.Equal(t, dynamodb.ProjectionTypeKeysOnly, *fu.Projection.ProjectionType)

	snafu, ok := lsiMap["snafu"]
	require.True(t, ok)
	require.NotNil(t, snafu)
	require.Equal(t, 2, len(snafu.KeySchema))
	assert.Equal(t, dynamodb.KeyTypeHash, *snafu.KeySchema[0].KeyType)
	assert.Equal(t, "a", *snafu.KeySchema[0].AttributeName)
	assert.Equal(t, dynamodb.KeyTypeRange, *snafu.KeySchema[1].KeyType)
	assert.Equal(t, "c", *snafu.KeySchema[1].AttributeName)
	require.Equal(t, 2, len(snafu.Projection.NonKeyAttributes))
	assert.Equal(t, "D", *snafu.Projection.NonKeyAttributes[0])
	assert.Equal(t, "e", *snafu.Projection.NonKeyAttributes[1])
	assert.Equal(t, dynamodb.ProjectionTypeInclude, *snafu.Projection.ProjectionType)

	bozo, ok := lsiMap["bozo"]
	require.True(t, ok)
	require.NotNil(t, bozo)
	require.Equal(t, 2, len(bozo.KeySchema))
	assert.Equal(t, dynamodb.KeyTypeHash, *bozo.KeySchema[0].KeyType)
	assert.Equal(t, "a", *bozo.KeySchema[0].AttributeName)
	assert.Equal(t, dynamodb.KeyTypeRange, *bozo.KeySchema[1].KeyType)
	assert.Equal(t, "f.G", *bozo.KeySchema[1].AttributeName)
	require.Equal(t, 2, len(bozo.Projection.NonKeyAttributes))
	assert.Equal(t, "f.K", *bozo.Projection.NonKeyAttributes[0])
	assert.Equal(t, "n", *bozo.Projection.NonKeyAttributes[1])
	assert.Equal(t, dynamodb.ProjectionTypeInclude, *bozo.Projection.ProjectionType)
}
