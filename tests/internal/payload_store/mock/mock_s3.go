package tests

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	aws_s3iface "github.com/aws/aws-sdk-go/service/s3/s3iface"

	"github.com/stretchr/testify/mock"
)

type MockS3 struct {
	aws_s3iface.S3API
	mock.Mock
}

func (m *MockS3) GetObjectWithContext(ctx aws.Context, input *aws_s3.GetObjectInput, option ...request.Option) (*aws_s3.GetObjectOutput, error) {
	args := m.Called(ctx, input)
	return args.Get(0).(*aws_s3.GetObjectOutput), args.Error(1)
}

func (m *MockS3) PutObjectWithContext(ctx aws.Context, input *aws_s3.PutObjectInput, option ...request.Option) (*aws_s3.PutObjectOutput, error) {
	args := m.Called(ctx, input)
	return args.Get(0).(*aws_s3.PutObjectOutput), args.Error(1)
}

func (m *MockS3) DeleteObjectWithContext(ctx aws.Context, input *aws_s3.DeleteObjectInput, option ...request.Option) (*aws_s3.DeleteObjectOutput, error) {
	args := m.Called(ctx, input)
	return args.Get(0).(*aws_s3.DeleteObjectOutput), args.Error(1)
}
