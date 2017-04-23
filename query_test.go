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

func (tsd *TestStructDao) SearchByBozo(f_G string, nStartsWith string) (*SearchPage, error) {
	queryValues := make(map[string]interface{})
	queryValues[":f_G"] = f_G
	queryValues[":nStartsWith"] = nStartsWith
	return tsd.dao.PagedQuery("bozo", "{f.G} = :f_G AND begins_with({n}, :nStartsWith)", "", queryValues, nil, 0, 50)
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
	_, err := dao.dao.PutItem(TestStruct{
		A:"1", B: 32, C: 3.14159, D: true, E: []byte{0x01, 0x02, 0x03},
		F: TestSubStruct{G: "foo", H: 2.1356, I: 55, K: 78, L: 1 },
		M: "ignored", N: "bar"})
	require.NoError(t,err)
	_, err = dao.dao.PutItem(TestStruct{
		A:"2", B: 23, C: 3.14159, D: true, E: []byte{0x01, 0x02, 0x03},
		F: TestSubStruct{G: "foo", H: 2.1356, I: 55, K: 78, L: 123456789012345 },
		M: "ignored", N: "barbell"})
	require.NoError(t,err)
	_, err = dao.dao.PutItem(TestStruct{
		A:"3", B: -55, C: 3.14159, D: true, E: []byte{0x01, 0x02, 0x03},
		F: TestSubStruct{G: "foo", H: 2.1356, I: 55, K: 78, L: 123456789012345 },
		M: "ignored", N: "snafubar"})
	require.NoError(t,err)
	_, err = dao.dao.PutItem(TestStruct{
		A:"4", B: -123456, C: 3.14159, D: true, E: []byte{0x01, 0x02, 0x03},
		F: TestSubStruct{G: "fu", H: 2.1356, I: 55, K: 78, L: 123456789012345 },
		M: "ignored", N: "bar"})
	require.NoError(t,err)

	searchPage, err := dao.SearchByBozo("foo", "bar")
	require.NoError(t, err)
	require.NotNil(t, searchPage)
	t.Logf("searchPage = %+v",searchPage)
	assert.EqualValues(t, 2, searchPage.TotalSize)
	assert.EqualValues(t, 2, len(searchPage.Data))
	assert.EqualValues(t, 0, searchPage.PageOffset)
	assert.EqualValues(t, 50, searchPage.PageSize)
	assert.NotNil(t, searchPage.LastItemToken)
}
