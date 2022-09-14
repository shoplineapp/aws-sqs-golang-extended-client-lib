package aws_extended_sqsiface

import (
	aws_sqs "github.com/aws/aws-sdk-go/service/sqs"
)

type ExtendedSQSClientInterface interface {
	SendMessage(*aws_sqs.SendMessageInput) (*aws_sqs.SendMessageOutput, error)
	ReceiveMessage(*aws_sqs.ReceiveMessageInput) (*aws_sqs.ReceiveMessageOutput, error)
	DeleteMessage(*aws_sqs.DeleteMessageInput) (*aws_sqs.DeleteMessageOutput, error)
}
