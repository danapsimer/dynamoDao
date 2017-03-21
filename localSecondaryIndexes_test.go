package dynamoDao

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/require"
	"testing"
	"github.com/stretchr/testify/assert"
	"github.com/aws/aws-sdk-go/aws"
)

func TestLocalSecondaryIndexes(t *testing.T) {
	testStruct := &TestStruct{}
	gsi, err := localIndexes(testStruct, &dynamodb.KeySchemaElement{
		AttributeName: aws.String("a"),
		KeyType: aws.String(dynamodb.KeyTypeHash),
	})
	require.Nil(t, err, "error in globalIndexes")
	require.NotNil(t, gsi)
	assert.Equal(t, 3, len(gsi))
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
}