package dynamoDao

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestAttributeDefinitions(t *testing.T) {
	testStruct := &TestStruct{}
	attrDefs, err := attributeDefinitions(testStruct,map[string]interface{}{
		"a": nil,
		"B": nil,
		"c": nil,
		"D": nil,
		"e": nil,
	})
	require.Nil(t, err, "Error occurred in attributeDefinitions")
	require.NotNil(t, attrDefs)
	assert.Equal(t, 5, len(attrDefs))
	attrMap := make(map[string]*dynamodb.AttributeDefinition)
	for _, a := range attrDefs {
		attrMap[*a.AttributeName] = a
	}

	a, ok := attrMap["a"]
	require.True(t, ok)
	require.NotNil(t, a)
	assert.Equal(t, "S", *a.AttributeType)

	b, ok := attrMap["B"]
	require.True(t, ok)
	require.NotNil(t, b)
	assert.Equal(t, "N", *b.AttributeType)

	c, ok := attrMap["c"]
	require.True(t, ok)
	require.NotNil(t, c)
	assert.Equal(t, "N", *c.AttributeType)

	d, ok := attrMap["D"]
	require.True(t, ok)
	require.NotNil(t, d)
	assert.Equal(t, "N", *d.AttributeType)

	e, ok := attrMap["e"]
	require.True(t, ok)
	require.NotNil(t, e)
	assert.Equal(t, "B", *e.AttributeType)
}
