package dynamoDao

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"reflect"
	"testing"
)

type TestStructDao struct {
	dao *DynamoDBDao
}

func (tsd *TestStructDao) SearchByFoo(a string, b int64) (*SearchPage, error) {
	queryValues := make(map[string]interface{})
	queryValues[":a"] = a
	queryValues[":b"] = b
	return tsd.dao.PagedQuery("Foo", "{a} = :a and {B} > :b", "", queryValues, nil, 0, 50)
}

func resetTestStructTable(t *testing.T) *TestStructDao {
	sess := session.New(awsConfig)
	client := dynamodb.New(sess)
	_, err := client.DescribeTable(&dynamodb.DescribeTableInput{TableName: aws.String("TestStruct")})
	if err == nil {
		_, err := client.DeleteTable(&dynamodb.DeleteTableInput{TableName: aws.String("TestStruct")})
		if err != nil {
			t.Fatal(err)
		}
	}
	dao, err := NewDynamoDBDaoForType(sess, reflect.TypeOf(TestStruct{}))
	if err != nil {
		t.Fatalf("Error creating dao: %s", err.Error())
	}
	return &TestStructDao{dao}
}

func TestDynamoDBDao_PagedQuery(t *testing.T) {
	dao := resetAndFillTable(t)

	searchPage, err := dao.SearchByFoo("3", -56)
	require.NoError(t, err)
	require.NotNil(t, searchPage)
	t.Logf("searchPage = %+v", searchPage)
	assert.EqualValues(t, int64(1), searchPage.TotalSize)
	assert.EqualValues(t, int(1), len(searchPage.Data))
	assert.EqualValues(t, int64(0), searchPage.PageOffset)
	assert.EqualValues(t, int64(50), searchPage.PageSize)
	assert.Nil(t, searchPage.LastItemToken)
}
