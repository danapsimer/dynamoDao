package uuid_test

import (
	"encoding/base64"
	"github.com/danapsimer/dynamoDao/uuid"
	"github.com/stretchr/testify/assert"
	"testing"
)

var (
	testV4 = []byte{0x54, 0x31, 0x65, 0x71, 0xd4, 0xdc, 0x46, 0x55, 0xa8, 0x22, 0xb2, 0xb7, 0xdb, 0x77, 0x4e, 0x36}
)

func TestNewFromBytes(t *testing.T) {
	v4, err := uuid.NewFromBytes(testV4)
	if assert.NoError(t, err) {
		assert.Equal(t, "54316571-d4dc-4655-a822-b2b7db774e36", v4.String())
	}
}

func TestParse(t *testing.T) {
	parsed, err := uuid.Parse("54316571-d4dc-4655-a822-b2b7db774e36")
	if assert.NoError(t, err) {
		assert.Equal(t, testV4, parsed.UUID[:]);
	}
}

func TestNewV4(t *testing.T) {
	v4 := uuid.NewV4()
	if assert.NotNil(t, v4) {
		t.Logf("v4 = %v", v4)
	}
}


func TestNewV1(t *testing.T) {
	v1 := uuid.NewV1()
	if assert.NotNil(t, v1) {
		t.Logf("v1 = %v", v1)
	}
}

func TestBase64Encode(t *testing.T) {
	parsed, err := uuid.Parse("342b613f-d4d5-4a22-91bc-f6530e907cdf")
	if assert.NoError(t,err) {
		t.Logf("base64 = %s",base64.StdEncoding.EncodeToString(parsed.UUID[:]))
	}
}
