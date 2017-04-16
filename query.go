package dynamoDao

import (
	"bytes"
	"compress/lzw"
	"encoding/base64"
	"encoding/json"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"errors"
)

type SearchPage struct {
	PageOffset    int64
	PageSize      int64
	TotalSize     int64
	LastItemToken *string
	Data          []interface{}
}

/*
 * Searches the given index name with the given query.  The query can only use operations supported by DynamoDb Queries.
 * see http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.html
 * The field names may be mapped by terms surrounded by {} (e.g. {Name}), and they will be mapped to the corresponding
 * fields.  However you can refer to field names directly.  Keep in mind that Dynamo has a large number of reserved
 * words so if your field names conflict with any of those, you must enclose them in {}.  See:
 * http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ReservedWords.html
 * The input parameter names will be mapped the same way as described in the above referenced documentation with the
 * value set to the values defined in the queryValues parameter.
 */
func (dao *DynamoDBDao) Search(indexName, keyExpression, filterExpression string, queryValues map[string]interface{}, lastItemToken *string, pageOffset, pageSize int64) (*SearchPage, error){

	return nil, errors.New("Not implemented yet.")

/*
	countQuery := new(dynamodb.QueryInput).
		SetTableName(dao.tableName).
		SetIndexName("GSI-Name").
		SetConsistentRead(false).
		SetExpressionAttributeNames(map[string]*string{"#T": aws.String("type")}).
		SetKeyConditionExpression("#T = :grainType").
		SetExpressionAttributeValues(map[string]*dynamodb.AttributeValue{":grainType": {S: aws.String("Grain")}}).
		SetSelect("COUNT")
	countResult, err := dao.client.Query(countQuery)
	if err != nil {
		return
	}
	query := new(dynamodb.QueryInput).
		SetTableName(dao.tableName).
		SetIndexName("GSI-Name").
		SetConsistentRead(false).
		SetLimit(int64(pageSize)).
		SetExpressionAttributeNames(map[string]*string{"#T": aws.String("type")}).
		SetKeyConditionExpression("#T = :grainType").
		SetExpressionAttributeValues(map[string]*dynamodb.AttributeValue{":grainType": {S: aws.String("Grain")}})
	firstItemToProcess := 0
	if lastItemKey != nil {
		query.SetExclusiveStartKey(lastItemKey)
	} else {
		firstItemToProcess = pageSize * pageOffset
	}

	grains = &grain.Grains{
		PageSize: pageSize, PageOffset: pageOffset, TotalSize: int(*countResult.Count),
		Grains: make([]*grain.Grain, 0, pageSize),
	}
	itemIndex := 0
	dao.client.QueryPages(query, func(result *dynamodb.QueryOutput, lastPage bool) bool {
		for _, item := range result.Items {
			if itemIndex >= firstItemToProcess {
				newGrain := new(grain.Grain)
				err = dynamodbattribute.ConvertFromMap(item, newGrain)
				if err != nil {
					grains = nil
					return false
				}
				grains.Grains = append(grains.Grains, newGrain)
				if len(grains.Grains) >= pageSize {
					grains.LastItemToken, err = keyToToken(map[string]*dynamodb.AttributeValue{
						"id":   item["id"],
						"type": item["type"],
						"name": item["name"],
					})
					return false
				}
			}
			itemIndex += 1
		}
		return !lastPage && len(grains.Grains) < pageSize
	})

	return
*/
}

func decompress(in string) (out string, err error) {
	decoded, err := base64.StdEncoding.DecodeString(in)
	if err != nil {
		return
	}
	decodedBuf := bytes.NewBuffer(decoded)
	lzwReader := lzw.NewReader(decodedBuf, lzw.LSB, 8)
	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(lzwReader)
	if err != nil {
		return
	}
	lzwReader.Close()
	out = buf.String()
	return
}

func compress(in string) (out string, err error) {
	buf := new(bytes.Buffer)
	lzwWriter := lzw.NewWriter(buf, lzw.LSB, 8)
	_, err = lzwWriter.Write([]byte(in))
	if err != nil {
		return
	}
	lzwWriter.Close()
	out = base64.StdEncoding.EncodeToString(buf.Bytes())
	return
}

func tokenToKey(lastItemToken *string) (map[string]*dynamodb.AttributeValue, error) {
	var lastItemKey map[string]*dynamodb.AttributeValue
	if lastItemToken != nil {
		startingPointJson, err := decompress(*lastItemToken)
		if err != nil {
			return nil, err
		}
		json.Unmarshal([]byte(startingPointJson), &lastItemKey)
	}
	return lastItemKey, nil
}

func keyToToken(lastItemKey map[string]*dynamodb.AttributeValue) (*string, error) {
	lastItemKeyJson, err := json.Marshal(lastItemKey)
	if err != nil {
		return nil, err
	}
	lastItemToken, err := compress(string(lastItemKeyJson))
	if err != nil {
		return nil, err
	}
	return &lastItemToken, nil
}
