package dynamoDao

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestAttributeDefinitions(t *testing.T) {
	testStruct := &TestStruct{}
	attrDefs, _, err := attributeDefinitions(testStruct, map[string]interface{}{
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

type TestSubStructWithAttrDefs struct {
	D string `dynamodbav:"d"`
	E int64
	F float64
}

type TestStructWithSubStructs struct {
	A TestSubStructWithAttrDefs  `dynamodbav:"a"`
	B *TestSubStructWithAttrDefs `dynamodbav:",omitempty"`
	C string
}

func TestAttributeDefinitionsWithSubStructs(t *testing.T) {
	testStruct := TestStructWithSubStructs{
		A: TestSubStructWithAttrDefs{D: "foo", E: int64(5522), F: 3.14159},
		B: &TestSubStructWithAttrDefs{D: "bar", E: int64(2255), F: 2.71828},
		C: "snafu",
	}
	attrNames := make(map[string]interface{})
	attrNames["a.d"] = nil
	attrNames["a.E"] = nil
	attrNames["B.d"] = nil
	attrNames["B.E"] = nil
	attrDefs, _, err := attributeDefinitions(&testStruct, attrNames)
	require.Nil(t, err)
	require.NotNil(t, attrDefs)
	require.EqualValues(t, 4, len(attrDefs))
	assert.EqualValues(t, "a.d", *attrDefs[0].AttributeName)
	assert.EqualValues(t, "S", *attrDefs[0].AttributeType)
	assert.EqualValues(t, "a.E", *attrDefs[1].AttributeName)
	assert.EqualValues(t, "N", *attrDefs[1].AttributeType)
	assert.EqualValues(t, "B.d", *attrDefs[2].AttributeName)
	assert.EqualValues(t, "S", *attrDefs[2].AttributeType)
	assert.EqualValues(t, "B.E", *attrDefs[3].AttributeName)
	assert.EqualValues(t, "N", *attrDefs[3].AttributeType)
}
