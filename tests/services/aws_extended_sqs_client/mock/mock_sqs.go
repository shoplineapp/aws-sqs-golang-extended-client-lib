package tests

import (
	"github.com/stretchr/testify/mock"

	aws_sqs "github.com/aws/aws-sdk-go/service/sqs"
	aws_sqsiface "github.com/aws/aws-sdk-go/service/sqs/sqsiface"
)

type MockSqs struct {
	aws_sqsiface.SQSAPI
	mock.Mock
}

func (m *MockSqs) SendMessage(input *aws_sqs.SendMessageInput) (*aws_sqs.SendMessageOutput, error) {
	args := m.Called(input)
	return args.Get(0).(*aws_sqs.SendMessageOutput), args.Error(1)
}

func (m *MockSqs) ReceiveMessage(input *aws_sqs.ReceiveMessageInput) (*aws_sqs.ReceiveMessageOutput, error) {
	args := m.Called(input)
	return args.Get(0).(*aws_sqs.ReceiveMessageOutput), args.Error(1)
}

func (m *MockSqs) DeleteMessage(input *aws_sqs.DeleteMessageInput) (*aws_sqs.DeleteMessageOutput, error) {
	args := m.Called(input)
	return args.Get(0).(*aws_sqs.DeleteMessageOutput), args.Error(1)
}
