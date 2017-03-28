package uuid

import (
	"errors"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	base "github.com/satori/go.uuid"
)

type UUID struct {
	base.UUID
}

var (
	Nil              = UUID{base.Nil}
	UnrecognizedUUID = errors.New("attribute value is not a UUID")
)

func NewV5(ns UUID, name string) UUID {
	return UUID{base.NewV5(ns.UUID, name)}
}

func NewV4() UUID {
	return UUID{base.NewV4()}
}

func NewV3(ns UUID, name string) UUID {
	return UUID{base.NewV3(ns.UUID, name)}
}

func NewV2(domain byte) UUID {
	return UUID{base.NewV2(domain)}
}

func NewV1() UUID {
	return UUID{base.NewV1()}
}

func (uuid *UUID) MarshalDynamoDBAttributeValue(av *dynamodb.AttributeValue) error {
	if *uuid == Nil {
		av.SetNULL(true)
	} else {
		av.SetB(uuid.Bytes())
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
