package tests

import (
	"fmt"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/shoplineapp/aws-sqs-golang-extended-client-lib/internal/payload_store"
	. "github.com/shoplineapp/aws-sqs-golang-extended-client-lib/tests/internal/payload_store/mock"

	"github.com/aws/aws-sdk-go/aws/awserr"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func Test_PayloadStore_StoreOriginalPayload_Success(t *testing.T) {
	mockS3 := new(MockS3)

	mockS3.On("PutObjectWithContext", mock.Anything, mock.Anything).Return(&aws_s3.PutObjectOutput{}, nil)

	s3BucketName := "test-bucket"
	originalPayload := "test-body"
	payloadStore := payload_store.NewPayloadStore(mockS3, s3BucketName)

	pointerStr, err := payloadStore.StoreOriginalPayload(originalPayload)

	assert.Nil(t, err)
	assert.NotNil(t, pointerStr)
	assert.Contains(t, pointerStr, fmt.Sprintf("\"s3BucketName\":\"%s\"", s3BucketName))
}

func Test_PayloadStore_StoreOriginalPayload_Failed_S3_Error(t *testing.T) {
	mockS3 := new(MockS3)

	mockS3.On("PutObjectWithContext", mock.Anything, mock.Anything).Return(
		&aws_s3.PutObjectOutput{},
		awserr.New(aws_s3.ErrCodeNoSuchBucket, "The specified bucket does not exist", nil),
	)

	s3BucketName := "test-bucket"
	originalPayload := "test-body"
	payloadStore := payload_store.NewPayloadStore(mockS3, s3BucketName)

	pointerStr, err := payloadStore.StoreOriginalPayload(originalPayload)

	assert.NotNil(t, err)
	assert.Empty(t, pointerStr)
}

func Test_PayloadStore_GetOriginalPayload_Success(t *testing.T) {
	mockS3 := new(MockS3)

	originalPayload := "test-body"

	mockS3.On("GetObjectWithContext", mock.Anything, mock.Anything).Return(&aws_s3.GetObjectOutput{
		Body: ioutil.NopCloser(strings.NewReader(originalPayload)),
	}, nil)

	s3BucketName := "test-bucket"
	messagePointer := "[\"software.amazon.payloadoffloading.PayloadS3Pointer\",{\"s3BucketName\":\"test-bucket\",\"s3Key\":\"test-key\"}]"
	payloadStore := payload_store.NewPayloadStore(mockS3, s3BucketName)

	payload, err := payloadStore.GetOriginalPayload(messagePointer)

	assert.Nil(t, err)
	assert.NotNil(t, payload)
	assert.Equal(t, originalPayload, payload)
}

func Test_PayloadStore_GetOriginalPayload_Failed_S3_Error(t *testing.T) {
	mockS3 := new(MockS3)

	mockS3.On("GetObjectWithContext", mock.Anything, mock.Anything).Return(
		&aws_s3.GetObjectOutput{},
		awserr.New(aws_s3.ErrCodeNoSuchBucket, "The specified bucket does not exist", nil),
	)

	s3BucketName := "test-bucket"
	messagePointer := "[\"software.amazon.payloadoffloading.PayloadS3Pointer\",{\"s3BucketName\":\"test-bucket\",\"s3Key\":\"test-key\"}]"
	payloadStore := payload_store.NewPayloadStore(mockS3, s3BucketName)

	payload, err := payloadStore.GetOriginalPayload(messagePointer)

	assert.NotNil(t, err)
	assert.Empty(t, payload)
}

func Test_PayloadStore_DeleteOriginalPayload_Success(t *testing.T) {
	mockS3 := new(MockS3)

	mockS3.On("DeleteObjectWithContext", mock.Anything, mock.Anything).Return(&aws_s3.DeleteObjectOutput{}, nil)

	s3BucketName := "test-bucket"
	messagePointer := "[\"software.amazon.payloadoffloading.PayloadS3Pointer\",{\"s3BucketName\":\"test-bucket\",\"s3Key\":\"test-key\"}]"
	payloadStore := payload_store.NewPayloadStore(mockS3, s3BucketName)

	err := payloadStore.DeleteOriginalPayload(messagePointer)

	assert.Nil(t, err)
}

func Test_PayloadStore_DeleteOriginalPayload_Failed_S3_Error(t *testing.T) {
	mockS3 := new(MockS3)

	mockS3.On("DeleteObjectWithContext", mock.Anything, mock.Anything).Return(
		&aws_s3.DeleteObjectOutput{},
		awserr.New(aws_s3.ErrCodeNoSuchBucket, "The specified bucket does not exist", nil),
	)

	s3BucketName := "test-bucket"
	messagePointer := "[\"software.amazon.payloadoffloading.PayloadS3Pointer\",{\"s3BucketName\":\"test-bucket\",\"s3Key\":\"test-key\"}]"
	payloadStore := payload_store.NewPayloadStore(mockS3, s3BucketName)

	err := payloadStore.DeleteOriginalPayload(messagePointer)

	assert.NotNil(t, err)
}
