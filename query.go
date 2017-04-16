package dynamoDao

import (
	"bytes"
	"compress/lzw"
	"encoding/base64"
	"encoding/json"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"reflect"
	"regexp"
	"strings"
)

type SearchPage struct {
	PageOffset    int64
	PageSize      int64
	TotalSize     int64
	LastItemToken *string
	Data          []interface{}
}

var (
	attrNameTokenRegex = regexp.MustCompile("\\{[^}]+\\}")
)

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
func (dao *DynamoDBDao) PagedQuery(indexName, keyExpression, filterExpression string, queryValues map[string]interface{}, lastItemToken *string, pageOffset, pageSize int64) (*SearchPage, error) {
	attrNames := make(map[string]*string)
	keyExpression = extractAttrNameAliasesFromExpression(keyExpression, attrNames)
	filterExpression = extractAttrNameAliasesFromExpression(filterExpression, attrNames)
	paramValues, err := dynamodbattribute.MarshalMap(queryValues)
	if err != nil {
		return nil, err
	}
	countQuery := new(dynamodb.QueryInput).
		SetTableName(dao.TableName).
		SetConsistentRead(false).
		SetExpressionAttributeNames(attrNames).
		SetKeyConditionExpression(keyExpression).
		SetExpressionAttributeValues(paramValues).
		SetSelect("COUNT")
	if indexName != "" {
		countQuery = countQuery.SetIndexName(indexName)
	}
	if filterExpression != "" {
		countQuery = countQuery.SetFilterExpression(filterExpression)
	}
	countResult, err := dao.Client.Query(countQuery)
	if err != nil {
		return nil, err
	}
	query := new(dynamodb.QueryInput).
		SetTableName(dao.TableName).
		SetConsistentRead(false).
		SetLimit(int64(pageSize)).
		SetExpressionAttributeNames(attrNames).
		SetKeyConditionExpression(keyExpression).
		SetExpressionAttributeValues(paramValues)
	if indexName != "" {
		query = query.SetIndexName(indexName)
	}
	if filterExpression != "" {
		query = query.SetFilterExpression(filterExpression)
	}
	firstItemToProcess := int64(0)
	lastItemKey, err := tokenToKey(lastItemToken)
	if err != nil {
		return nil, err
	}

	if lastItemKey != nil {
		query.SetExclusiveStartKey(lastItemKey)
	} else {
		firstItemToProcess = pageSize * pageOffset
	}
	page := &SearchPage{
		PageSize: pageSize, PageOffset: pageOffset, TotalSize: int64(*countResult.Count),
		Data: make([]interface{}, 0, pageSize),
	}

	keyAttrs := dao.keyAttrNames
	if indexName != "" {
		for _, gsi := range dao.tableDescription.GlobalSecondaryIndexes {
			if *gsi.IndexName == indexName {
				keyAttrs := make([]string, 0, 2)
				for _, ks := range gsi.KeySchema {
					keyAttrs = append(keyAttrs, *ks.AttributeName)
				}
			}
		}
	}
	itemIndex := int64(0)
	dao.Client.QueryPages(query, func(result *dynamodb.QueryOutput, lastPage bool) bool {
		for _, item := range result.Items {
			if itemIndex >= firstItemToProcess {
				data := reflect.New(dao.structType)
				err = dynamodbattribute.UnmarshalMap(item, data)
				if err != nil {
					return false
				}
				page.Data = append(page.Data, data)
				if int64(len(page.Data)) >= pageSize {
					keyAttrValues := make(map[string]*dynamodb.AttributeValue)
					for _, name := range keyAttrs {
						keyAttrValues[name] = item[name]
					}
					page.LastItemToken, err = keyToToken(keyAttrValues)

					return false
				}
			}
			itemIndex += 1
		}
		return !lastPage && int64(len(page.Data)) < pageSize
	})
	return page, err
}

func extractAttrNameAliasesFromExpression(expression string, attrNames map[string]*string) string {
	attrNamesFound := attrNameTokenRegex.FindAllString(expression, -1)
	if attrNamesFound != nil {
		nextSubstituteNameChar := uint8(65) // Start with A
		for k, _ := range attrNames {
			if k[1] >= nextSubstituteNameChar {
				nextSubstituteNameChar = k[1] + 1
			}
		}
		for _, attrNameToken := range attrNamesFound {
			attrName := attrNameToken[1 : len(attrNameToken)-1]
			substituteName := string([]byte{0x23, nextSubstituteNameChar})
			for subName, attrNamePtr := range attrNames {
				if *attrNamePtr == attrName {
					substituteName = subName
					break
				}
			}
			attrNames[substituteName] = &attrName

			expression = strings.Replace(expression, attrNameToken, substituteName, -1)
		}
	}
	return expression

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
