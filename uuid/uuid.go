package uuid

import (
	"errors"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	base "github.com/google/uuid"
)

type UUID struct {
	base.UUID
}

var (
	Nil              = UUID{base.Nil}
	UnrecognizedUUID = errors.New("attribute value is not a UUID")
)

func NewFromBytes(bytes []byte) (UUID, error) {
	baseUUID, err := base.FromBytes(bytes)
	if err != nil {
		return Nil, err
	}
	return UUID{baseUUID}, nil
}

func NewV4() UUID {
	baseUUID := base.New()
	return UUID{baseUUID}
}

func NewV1() UUID {
	baseUUID := base.Must(base.NewUUID())
	return UUID{baseUUID}
}

func (uuid *UUID) MarshalDynamoDBAttributeValue(av *dynamodb.AttributeValue) error {
	if *uuid == Nil {
		av.SetNULL(true)
	} else {
		bytes, err := uuid.MarshalBinary()
		if err != nil {
			return err
		}
		av.SetB(bytes)
	}
	return nil
}

func (uuid *UUID) UnmarshalDynamoDBAttributeValue(av *dynamodb.AttributeValue) error {
	if av.NULL != nil && *av.NULL {
		*uuid = Nil
	} else if len(av.B) == 16 {
		uuidFromBytes, err := base.FromBytes(av.B)
		if err != nil {
			return err
		}
		*uuid = UUID{uuidFromBytes}
	} else {
		return UnrecognizedUUID
	}
	return nil
}
