package payload_store

import (
	"bytes"
	"context"
	"io"
	"strings"

	aws_extended_sqsiface "github.com/shoplineapp/aws-sqs-golang-extended-client-lib/interfaces"
	payload_store_constants "github.com/shoplineapp/aws-sqs-golang-extended-client-lib/internal/payload_store/constants"

	"github.com/aws/aws-sdk-go/aws"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	aws_s3iface "github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/google/uuid"
)

type PayloadStore struct {
	aws_extended_sqsiface.PayloadStoreInterface
	s3           aws_s3iface.S3API
	s3BucketName string
}

func NewPayloadStore(s3Client aws_s3iface.S3API, s3BucketName string) *PayloadStore {
	payloadStore := &PayloadStore{
		s3:           s3Client,
		s3BucketName: s3BucketName,
	}

	return payloadStore
}

func (p *PayloadStore) StoreOriginalPayload(originalPayload string) (string, error) {
	s3Key := uuid.NewString()

	payloadPointer, err := p.storeTextInS3(originalPayload, p.s3BucketName, s3Key)

	if err != nil {
		return "", err
	}

	messagePointer, err := payloadPointer.ToJson()

	if err != nil {
		return "", err
	}

	return messagePointer, nil
}

func (p *PayloadStore) GetOriginalPayload(messagePointer string) (string, error) {
	payloadPointer, err := FromJson(messagePointer)
	if err != nil {
		return "", err
	}

	payload, err := p.getTextFromS3(payloadPointer.S3BucketName, payloadPointer.S3Key)

	if err != nil {
		return "", err
	}

	return payload, err
}

func (p *PayloadStore) DeleteOriginalPayload(messagePointer string) error {
	payloadPointer, err := FromJson(messagePointer)

	if err != nil {
		return err
	}

	return p.deletePayloadFromS3(payloadPointer.S3BucketName, payloadPointer.S3Key)
}

func (p *PayloadStore) storeTextInS3(payload string, s3BucketName string, s3Key string) (*PayloadS3Pointer, error) {
	ctx, cancel := context.WithTimeout(context.Background(), payload_store_constants.S3_CONTEXT_TIMEOUT)
	defer cancel()

	reader := strings.NewReader(payload)

	_, err := p.s3.PutObjectWithContext(ctx, &aws_s3.PutObjectInput{
		Bucket: aws.String(s3BucketName),
		Key:    aws.String(s3Key),
		Body:   reader,
	})

	if err != nil {
		return nil, err
	}

	return &PayloadS3Pointer{
		S3BucketName: p.s3BucketName,
		S3Key:        s3Key,
	}, nil
}

func (p *PayloadStore) getTextFromS3(s3BucketName string, s3Key string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), payload_store_constants.S3_CONTEXT_TIMEOUT)
	defer cancel()

	rawObject, err := p.s3.GetObjectWithContext(ctx, &aws_s3.GetObjectInput{
		Bucket: aws.String(s3BucketName),
		Key:    aws.String(s3Key),
	})

	if err != nil {
		return "", err
	}

	defer rawObject.Body.Close()

	var objectBuffer bytes.Buffer
	_, err = io.Copy(&objectBuffer, rawObject.Body)

	if err != nil {
		return "", err
	}

	return objectBuffer.String(), nil
}

func (p *PayloadStore) deletePayloadFromS3(s3BucketName string, s3Key string) error {
	ctx, cancel := context.WithTimeout(context.Background(), payload_store_constants.S3_CONTEXT_TIMEOUT)
	defer cancel()

	_, err := p.s3.DeleteObjectWithContext(ctx, &aws_s3.DeleteObjectInput{
		Bucket: aws.String(s3BucketName),
		Key:    aws.String(s3Key),
	})

	return err
}
