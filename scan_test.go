package dynamoDao

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func (tsd *TestStructDao) ScanByFoo(pageOffset, pageSize int64) (*SearchPage, error) {
	return tsd.dao.PagedScan("Foo", pageOffset, pageSize)
}

func TestDynamoDBDao_PagedScan(t *testing.T) {
	dao := resetAndFillTable(t)

	searchPage, err := dao.ScanByFoo(0, 50)
	require.NoError(t, err)
	require.NotNil(t, searchPage)
	t.Logf("searchPage = %+v", searchPage)
	assert.EqualValues(t, int64(4), searchPage.TotalSize)
	assert.EqualValues(t, int(4), len(searchPage.Data))
	assert.EqualValues(t, int64(0), searchPage.PageOffset)
	assert.EqualValues(t, int64(50), searchPage.PageSize)
	assert.Nil(t, searchPage.LastItemToken)
}

func TestDynamoDBDao_PagedScanFirstPage(t *testing.T) {
	dao := resetAndFillTable(t)

	searchPage, err := dao.ScanByFoo(0, 2)
	require.NoError(t, err)
	require.NotNil(t, searchPage)
	t.Logf("searchPage = %+v", searchPage)
	assert.EqualValues(t, int64(4), searchPage.TotalSize)
	assert.EqualValues(t, int(2), len(searchPage.Data))
	assert.EqualValues(t, int64(0), searchPage.PageOffset)
	assert.EqualValues(t, int64(2), searchPage.PageSize)
	assert.NotNil(t, searchPage.LastItemToken)
}

func TestDynamoDBDao_PagedScanSecondPage(t *testing.T) {
	dao := resetAndFillTable(t)

	searchPage, err := dao.ScanByFoo(1, 2)
	require.NoError(t, err)
	require.NotNil(t, searchPage)
	t.Logf("searchPage = %+v", searchPage)
	assert.EqualValues(t, int64(4), searchPage.TotalSize)
	assert.EqualValues(t, int(2), len(searchPage.Data))
	assert.EqualValues(t, int64(1), searchPage.PageOffset)
	assert.EqualValues(t, int64(2), searchPage.PageSize)
	assert.NotNil(t, searchPage.LastItemToken)
}

func resetAndFillTable(t *testing.T) *TestStructDao {
	dao := resetTestStructTable(t)
	_, err := dao.dao.PutItem(TestStruct{
		A: "1", B: 32, C: 3.14159, D: true, E: []byte{0x01, 0x02, 0x03},
		F: TestSubStruct{G: "foo", H: 2.1356, I: 55, K: 78, L: 1},
		M: "ignored", N: "bar"})
	require.NoError(t, err)
	_, err = dao.dao.PutItem(TestStruct{
		A: "2", B: 23, C: 3.14159, D: true, E: []byte{0x01, 0x02, 0x03},
		F: TestSubStruct{G: "foo", H: 2.1356, I: 55, K: 78, L: 123456789012345},
		M: "ignored", N: "barbell"})
	require.NoError(t, err)
	_, err = dao.dao.PutItem(TestStruct{
		A: "3", B: -55, C: 3.14159, D: true, E: []byte{0x01, 0x02, 0x03},
		F: TestSubStruct{G: "foo", H: 2.1356, I: 55, K: 78, L: 123456789012345},
		M: "ignored", N: "snafubar"})
	require.NoError(t, err)
	_, err = dao.dao.PutItem(TestStruct{
		A: "4", B: -123456, C: 3.14159, D: true, E: []byte{0x01, 0x02, 0x03},
		F: TestSubStruct{G: "fu", H: 2.1356, I: 55, K: 78, L: 123456789012345},
		M: "ignored", N: "bar"})
	require.NoError(t, err)
	return dao
}
