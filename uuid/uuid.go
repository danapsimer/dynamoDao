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

func NewFromBytes(bytes [16]byte) UUID {
	return UUID{base.UUID(bytes)}
}

func NewV5(ns UUID, name string) UUID {
	return UUID{base.NewV5(ns.UUID, name)}
}

func NewV4() UUID {
	baseUUID, err := base.NewV4()
	if err != nil {
		panic("satori NewV4 returned error: " + err.Error())
	}
	return UUID{baseUUID}
}

func NewV3(ns UUID, name string) UUID {
	return UUID{base.NewV3(ns.UUID, name)}
}

func NewV2(domain byte) UUID {
	baseUUID, err := base.NewV2(domain)
	if err != nil {
		panic("satori NewV2 returned error: " + err.Error())
	}
	return UUID{baseUUID}
}

func NewV1() UUID {
	baseUUID, err := base.NewV1()
	if err != nil {
		panic("satori NewV1 returned error: " + err.Error())
	}
	return UUID{baseUUID}
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
