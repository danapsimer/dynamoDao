package dynamoDao

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/require"
	"reflect"
	"testing"
)

type TestStructDao struct {
	dao *DynamoDBDao
}

func (tsd *TestStructDao) SearchByBozo(f_G string, nStartsWith string) (*SearchPage, error) {
	queryValues := make(map[string]interface{})
	queryValues[":f_G"] = f_G
	queryValues[":nStartsWith"] = nStartsWith
	return tsd.dao.PagedQuery("bozo", "{f.G} = :f_G", "begins_with({N}, :nStartsWith)", queryValues, nil, 0, 50)
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
	dao := resetTestStructTable(t)

	searchPage, err := dao.SearchByBozo("foo", "bar")
	require.NoError(t, err)
	require.NotNil(t, searchPage)

}
