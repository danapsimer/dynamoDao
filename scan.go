package dynamoDao

import "github.com/aws/aws-sdk-go/service/dynamodb"

func (dod *DynamoDBDao) PagedScan(indexName string, pageOffset, pageSize int64) (*SearchPage, error) {
	countScan := new(dynamodb.ScanInput).
		SetTableName(dod.TableName).
		SetConsistentRead(false).
		SetIndexName(indexName).
		SetSelect("COUNT")
	countResult, err := dod.Client.Scan(countScan)
	if err != nil {
		return nil, err
	}
	scan := new(dynamodb.ScanInput).
		SetTableName(dod.TableName).
		SetIndexName(indexName).
		SetConsistentRead(false).
		SetLimit(pageSize)
	itemIndex := int64(0)
	firstItemToProcess := pageSize * pageOffset
	page := &SearchPage{
		PageSize: pageSize, PageOffset: pageOffset, TotalSize: int64(*countResult.Count),
		Data: make([]interface{}, 0, pageSize),
	}
	// Don't run the scan if it cannot possibly bare fruit.
	if firstItemToProcess >= *countResult.Count {
		return page, nil
	}
	err = dod.Client.ScanPages(scan, func(result *dynamodb.ScanOutput, lastPage bool) bool {
		for _, item := range result.Items {
			if itemIndex >= firstItemToProcess {
				ptrT, err := dod.UnmarshalAttributes(item)
				if err != nil {
					return false
				}
				page.Data = append(page.Data, ptrT)
				if int64(len(page.Data)) >= pageSize {
					keyAttrValues := make(map[string]*dynamodb.AttributeValue)
					for _, name := range dod.keyAttrNames {
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
