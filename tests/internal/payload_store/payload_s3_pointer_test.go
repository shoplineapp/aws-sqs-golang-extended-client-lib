package tests

import (
	"testing"

	"github.com/shoplineapp/aws-sqs-golang-extended-client-lib/internal/payload_store"

	"github.com/stretchr/testify/assert"
)

func Test_PayloadPointer_ToJson_Success(t *testing.T) {
	pointer := payload_store.PayloadS3Pointer{
		S3BucketName: "test-bucket",
		S3Key:        "test-key",
	}

	json_str, err := pointer.ToJson()
	assert.Nil(t, err)
	assert.Equal(t, "[\"software.amazon.payloadoffloading.PayloadS3Pointer\",{\"s3BucketName\":\"test-bucket\",\"s3Key\":\"test-key\"}]", json_str)
}

func Test_PayloadPointer_FromJson_Success(t *testing.T) {
	pointerStr := "[\"software.amazon.payloadoffloading.PayloadS3Pointer\",{\"s3BucketName\":\"test-bucket\",\"s3Key\":\"test-key\"}]"

	pointer, err := payload_store.FromJson(pointerStr)
	assert.Nil(t, err)
	assert.NotNil(t, pointer)
	assert.Equal(t, "test-bucket", pointer.S3BucketName)
	assert.Equal(t, "test-key", pointer.S3Key)
}

func Test_PayloadPointer_FromJson_Invalid_Format(t *testing.T) {
	pointerStr := "{\"s3BucketName\":\"test-bucket\",\"s3Key\":\"test-key\"}"

	pointer, err := payload_store.FromJson(pointerStr)
	assert.Nil(t, pointer)
	assert.NotNil(t, err)
}

func Test_PayloadPointer_FromJson_Invalid_Json_Syntax(t *testing.T) {
	pointerStr := "[\"software.amazon.payloadoffloading.PayloadS3Pointer\",{\"s3BucketName\":\"test-bucket\",\"s3Key\":\"test-key\"]"

	pointer, err := payload_store.FromJson(pointerStr)
	assert.Nil(t, pointer)
	assert.NotNil(t, err)
}
