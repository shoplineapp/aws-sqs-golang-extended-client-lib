package tests

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"testing"

	aws_extended_sqsiface "github.com/shoplineapp/aws-sqs-golang-extended-client-lib/interfaces"
	"github.com/shoplineapp/aws-sqs-golang-extended-client-lib/services/aws_extended_sqs_client"
	sqs_configs_constants "github.com/shoplineapp/aws-sqs-golang-extended-client-lib/services/aws_extended_sqs_client/constants"

	. "github.com/shoplineapp/aws-sqs-golang-extended-client-lib/tests/internal/payload_store/mock"
	. "github.com/shoplineapp/aws-sqs-golang-extended-client-lib/tests/services/aws_extended_sqs_client/mock"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	aws_sqs "github.com/aws/aws-sdk-go/service/sqs"
	"github.com/google/uuid"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type ExtendedSqsClientTestSuite struct {
	suite.Suite

	sqsClient aws_extended_sqsiface.AwsExtendedSqsClientInterface
	config    aws_extended_sqsiface.AwsExtendedSqsClientConfigurationInterface

	mockSqs *MockSqs
	mockS3  *MockS3

	S3_BUCKET_NAME string
	S3_KEY         string
	BODY           string
	LARGE_BODY     string
	MESSAGE_ID     string
	RECEIPT_HANDLE string
}

func (suite *ExtendedSqsClientTestSuite) SetupTest() {
	suite.mockSqs = new(MockSqs)
	suite.mockS3 = new(MockS3)

	suite.S3_BUCKET_NAME = "test-bucket"
	suite.S3_KEY = uuid.New().String()
	suite.BODY = "test"
	suite.LARGE_BODY = strings.Repeat(suite.BODY, 65537)
	suite.MESSAGE_ID = "test-message-id"
	suite.RECEIPT_HANDLE = uuid.New().String()

	config := aws_extended_sqs_client.NewExtendedSQSClientConfiguration()
	config.WithPayloadSupportEnabled(suite.mockS3, suite.S3_BUCKET_NAME)

	suite.config = config

	suite.sqsClient = aws_extended_sqs_client.NewExtendedSQSClient(suite.mockSqs, config)
}

func (s *ExtendedSqsClientTestSuite) Test_ExtendedSqsClient_SendMessage_Success_Large_Payload() {
	s.mockSqs.On("SendMessage", mock.Anything).Return(&aws_sqs.SendMessageOutput{
		MessageId: &s.MESSAGE_ID,
	}, nil).Once()
	s.mockS3.On("PutObjectWithContext", mock.Anything, mock.Anything).Return(&aws_s3.PutObjectOutput{}, nil).Once()

	output, err := s.sqsClient.SendMessage(&aws_sqs.SendMessageInput{
		MessageBody: &s.LARGE_BODY,
	})

	s.mockSqs.AssertExpectations(s.T())
	s.mockS3.AssertExpectations(s.T())

	assert.Nil(s.T(), err)
	assert.NotNil(s.T(), output)
	assert.Equal(s.T(), s.MESSAGE_ID, *output.MessageId)
}

func (s *ExtendedSqsClientTestSuite) Test_ExtendedSqsClient_SendMessage_Success_Small_Payload() {
	s.mockSqs.On("SendMessage", mock.Anything).Return(&aws_sqs.SendMessageOutput{
		MessageId: &s.MESSAGE_ID,
	}, nil).Once()

	output, err := s.sqsClient.SendMessage(&aws_sqs.SendMessageInput{
		MessageBody: &s.BODY,
	})

	s.mockSqs.AssertExpectations(s.T())
	s.mockS3.AssertExpectations(s.T())

	assert.Nil(s.T(), err)
	assert.NotNil(s.T(), output)
	assert.Equal(s.T(), s.MESSAGE_ID, *output.MessageId)
}

func (s *ExtendedSqsClientTestSuite) Test_ExtendedSqsClient_SendMessage_Success_Always_Through_S3() {
	s.config.SetAlwaysThroughS3(true)

	s.mockSqs.On("SendMessage", mock.Anything).Return(&aws_sqs.SendMessageOutput{
		MessageId: &s.MESSAGE_ID,
	}, nil).Once()
	s.mockS3.On("PutObjectWithContext", mock.Anything, mock.Anything).Return(&aws_s3.PutObjectOutput{}, nil).Once()

	output, err := s.sqsClient.SendMessage(&aws_sqs.SendMessageInput{
		MessageBody: &s.BODY,
	})

	s.mockSqs.AssertExpectations(s.T())
	s.mockS3.AssertExpectations(s.T())

	assert.Nil(s.T(), err)
	assert.NotNil(s.T(), output)
	assert.Equal(s.T(), s.MESSAGE_ID, *output.MessageId)
}

func (s *ExtendedSqsClientTestSuite) Test_ExtendedSqsClient_SendMessage_Failed_S3_Error() {
	s.mockS3.On("PutObjectWithContext", mock.Anything, mock.Anything).Return(
		&aws_s3.PutObjectOutput{},
		awserr.New(aws_s3.ErrCodeNoSuchBucket, "The specified bucket does not exist", nil),
	).Once()

	_, err := s.sqsClient.SendMessage(&aws_sqs.SendMessageInput{
		MessageBody: &s.LARGE_BODY,
	})

	s.mockSqs.AssertExpectations(s.T())
	s.mockS3.AssertExpectations(s.T())

	assert.NotNil(s.T(), err)
}

func (s *ExtendedSqsClientTestSuite) Test_ExtendedSqsClient_SendMessage_Failed_SQS_Error() {
	s.mockSqs.On("SendMessage", mock.Anything).Return(
		&aws_sqs.SendMessageOutput{},
		awserr.New(aws_sqs.ErrCodeQueueDoesNotExist, "The specified queue does not exist", nil),
	).Once()
	s.mockS3.On("PutObjectWithContext", mock.Anything, mock.Anything).Return(&aws_s3.PutObjectOutput{}, nil).Once()

	_, err := s.sqsClient.SendMessage(&aws_sqs.SendMessageInput{
		MessageBody: &s.LARGE_BODY,
	})

	s.mockSqs.AssertExpectations(s.T())
	s.mockS3.AssertExpectations(s.T())

	assert.NotNil(s.T(), err)
}

func (s *ExtendedSqsClientTestSuite) Test_ExtendedSqsClient_SendMessage_Failed_Input_Empty() {
	_, err := s.sqsClient.SendMessage(nil)

	s.mockSqs.AssertExpectations(s.T())
	s.mockS3.AssertExpectations(s.T())

	assert.NotNil(s.T(), err)
}

func (s *ExtendedSqsClientTestSuite) Test_ExtendedSqsClient_SendMessage_Failed_Message_Body_Empty() {
	_, err := s.sqsClient.SendMessage(&aws_sqs.SendMessageInput{
		MessageBody: nil,
	})

	s.mockSqs.AssertExpectations(s.T())
	s.mockS3.AssertExpectations(s.T())

	assert.NotNil(s.T(), err)
}

func (s *ExtendedSqsClientTestSuite) Test_ExtendedSqsClient_SendMessage_Failed_Message_Attributes_Too_Large() {
	attributes := make(map[string]*aws_sqs.MessageAttributeValue)

	attributes["LargeAttribute1"] = &aws_sqs.MessageAttributeValue{
		DataType:    aws.String("String"),
		StringValue: aws.String(strings.Repeat("x", sqs_configs_constants.DEFAULT_MESSAGE_SIZE_THRESHOLD/2)),
	}
	attributes["LargeAttribute2"] = &aws_sqs.MessageAttributeValue{
		DataType:    aws.String("String"),
		StringValue: aws.String(strings.Repeat("y", sqs_configs_constants.DEFAULT_MESSAGE_SIZE_THRESHOLD/2)),
	}

	_, err := s.sqsClient.SendMessage(&aws_sqs.SendMessageInput{
		MessageBody:       &s.LARGE_BODY,
		MessageAttributes: attributes,
	})

	s.mockSqs.AssertExpectations(s.T())
	s.mockS3.AssertExpectations(s.T())

	assert.NotNil(s.T(), err)
}

func (s *ExtendedSqsClientTestSuite) Test_ExtendedSqsClient_SendMessage_Failed_Message_Attributes_Too_Many() {
	attributes := make(map[string]*aws_sqs.MessageAttributeValue)

	for i := 0; i <= sqs_configs_constants.MAX_ALLOWED_ATTRIBUTES; i++ {
		attributes[fmt.Sprintf("Attribute%d", i)] = &aws_sqs.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: aws.String("test"),
		}
	}

	_, err := s.sqsClient.SendMessage(&aws_sqs.SendMessageInput{
		MessageBody:       &s.LARGE_BODY,
		MessageAttributes: attributes,
	})

	s.mockSqs.AssertExpectations(s.T())
	s.mockS3.AssertExpectations(s.T())

	assert.NotNil(s.T(), err)
}

func (s *ExtendedSqsClientTestSuite) Test_ExtendedSqsClient_SendMessage_Failed_Reserved_Message_Attribute_Used() {
	attributes := make(map[string]*aws_sqs.MessageAttributeValue)

	attributes[sqs_configs_constants.RESERVED_ATTRIBUTE_NAME] = &aws_sqs.MessageAttributeValue{
		DataType:    aws.String("String"),
		StringValue: aws.String("test"),
	}

	_, err := s.sqsClient.SendMessage(&aws_sqs.SendMessageInput{
		MessageBody:       &s.LARGE_BODY,
		MessageAttributes: attributes,
	})

	s.mockSqs.AssertExpectations(s.T())
	s.mockS3.AssertExpectations(s.T())

	assert.NotNil(s.T(), err)
}

func (s *ExtendedSqsClientTestSuite) Test_ExtendedSqsClient_ReceiveMessage_Success_Large_Payload() {
	expectedReceiptHandle := fmt.Sprintf("%s%s%s%s%s%s%s",
		sqs_configs_constants.S3_BUCKET_NAME_MARKER, s.S3_BUCKET_NAME, sqs_configs_constants.S3_BUCKET_NAME_MARKER,
		sqs_configs_constants.S3_KEY_MARKER, s.S3_KEY, sqs_configs_constants.S3_KEY_MARKER,
		s.RECEIPT_HANDLE,
	)

	largePayloadMessage := createLargePayloadMessage(s.MESSAGE_ID, s.S3_BUCKET_NAME, s.S3_KEY, s.LARGE_BODY, s.RECEIPT_HANDLE)
	messages := make([]*aws_sqs.Message, 1)
	messages[0] = largePayloadMessage

	s.mockSqs.On("ReceiveMessage", mock.Anything).Return(&aws_sqs.ReceiveMessageOutput{
		Messages: messages,
	}, nil).Once()
	s.mockS3.On("GetObjectWithContext", mock.Anything, mock.Anything).Return(&aws_s3.GetObjectOutput{
		Body: io.NopCloser(strings.NewReader(s.LARGE_BODY)),
	}, nil).Once()

	output, err := s.sqsClient.ReceiveMessage(&aws_sqs.ReceiveMessageInput{
		AttributeNames: []*string{
			aws.String(aws_sqs.MessageSystemAttributeNameSentTimestamp),
		},
		MessageAttributeNames: []*string{
			aws.String(aws_sqs.QueueAttributeNameAll),
		},
		QueueUrl:            aws.String("test-queue"),
		MaxNumberOfMessages: aws.Int64(1),
	})

	s.mockSqs.AssertExpectations(s.T())
	s.mockS3.AssertExpectations(s.T())

	assert.Nil(s.T(), err)
	assert.NotNil(s.T(), output)
	assert.NotEmpty(s.T(), output.Messages)
	assert.Equal(s.T(), expectedReceiptHandle, *output.Messages[0].ReceiptHandle)
	assert.Equal(s.T(), len(s.LARGE_BODY), len(*output.Messages[0].Body))
}

func (s *ExtendedSqsClientTestSuite) Test_ExtendedSqsClient_ReceiveMessage_Success_Small_Payload() {
	message := &aws_sqs.Message{
		MessageId:     &s.MESSAGE_ID,
		Body:          &s.BODY,
		ReceiptHandle: &s.RECEIPT_HANDLE,
	}
	messages := make([]*aws_sqs.Message, 1)
	messages[0] = message

	s.mockSqs.On("ReceiveMessage", mock.Anything).Return(&aws_sqs.ReceiveMessageOutput{
		Messages: messages,
	}, nil).Once()

	output, err := s.sqsClient.ReceiveMessage(&aws_sqs.ReceiveMessageInput{
		AttributeNames: []*string{
			aws.String(aws_sqs.MessageSystemAttributeNameSentTimestamp),
		},
		MessageAttributeNames: []*string{
			aws.String(aws_sqs.QueueAttributeNameAll),
		},
		QueueUrl:            aws.String("test-queue"),
		MaxNumberOfMessages: aws.Int64(1),
	})

	s.mockSqs.AssertExpectations(s.T())

	assert.Nil(s.T(), err)
	assert.NotNil(s.T(), output)
	assert.NotEmpty(s.T(), output.Messages)
	assert.Equal(s.T(), s.RECEIPT_HANDLE, *output.Messages[0].ReceiptHandle)
	assert.Equal(s.T(), s.BODY, *output.Messages[0].Body)
}

func (s *ExtendedSqsClientTestSuite) Test_ExtendedSqsClient_ReceiveMessage_Failed_Input_Empty() {
	_, err := s.sqsClient.ReceiveMessage(nil)

	s.mockSqs.AssertExpectations(s.T())
	s.mockS3.AssertExpectations(s.T())

	assert.NotNil(s.T(), err)
}

func (s *ExtendedSqsClientTestSuite) Test_ExtendedSqsClient_DeleteMessage_Success_Large_Payload() {
	largePayloadReceiptHandle := fmt.Sprintf("%s%s%s%s%s%s%s",
		sqs_configs_constants.S3_BUCKET_NAME_MARKER, s.S3_BUCKET_NAME, sqs_configs_constants.S3_BUCKET_NAME_MARKER,
		sqs_configs_constants.S3_KEY_MARKER, s.S3_KEY, sqs_configs_constants.S3_KEY_MARKER,
		s.RECEIPT_HANDLE,
	)

	s.mockSqs.On("DeleteMessage", mock.Anything).Return(&aws_sqs.DeleteMessageOutput{}, nil).Once()
	s.mockS3.On("DeleteObjectWithContext", mock.Anything, mock.Anything).Return(&aws_s3.DeleteObjectOutput{}, nil).Once()

	_, err := s.sqsClient.DeleteMessage(&aws_sqs.DeleteMessageInput{
		ReceiptHandle: &largePayloadReceiptHandle,
	})

	s.mockSqs.AssertExpectations(s.T())
	s.mockS3.AssertExpectations(s.T())

	assert.Nil(s.T(), err)
}

func (s *ExtendedSqsClientTestSuite) Test_ExtendedSqsClient_DeleteMessage_Success_Small_Payload() {
	s.mockSqs.On("DeleteMessage", mock.Anything).Return(&aws_sqs.DeleteMessageOutput{}, nil).Once()

	_, err := s.sqsClient.DeleteMessage(&aws_sqs.DeleteMessageInput{
		ReceiptHandle: &s.RECEIPT_HANDLE,
	})

	s.mockSqs.AssertExpectations(s.T())
	s.mockS3.AssertExpectations(s.T())

	assert.Nil(s.T(), err)
}

func (s *ExtendedSqsClientTestSuite) Test_ExtendedSqsClient_DeleteMessage_Success_No_Cleanup_S3() {
	s.config.SetCleanupS3Payload(false)

	largePayloadReceiptHandle := fmt.Sprintf("%s%s%s%s%s%s%s",
		sqs_configs_constants.S3_BUCKET_NAME_MARKER, s.S3_BUCKET_NAME, sqs_configs_constants.S3_BUCKET_NAME_MARKER,
		sqs_configs_constants.S3_KEY_MARKER, s.S3_KEY, sqs_configs_constants.S3_KEY_MARKER,
		s.RECEIPT_HANDLE,
	)
	s.mockSqs.On("DeleteMessage", mock.Anything).Return(&aws_sqs.DeleteMessageOutput{}, nil).Once()

	_, err := s.sqsClient.DeleteMessage(&aws_sqs.DeleteMessageInput{
		ReceiptHandle: &largePayloadReceiptHandle,
	})

	s.mockSqs.AssertExpectations(s.T())
	s.mockS3.AssertExpectations(s.T())

	assert.Nil(s.T(), err)
}

func (s *ExtendedSqsClientTestSuite) Test_ExtendedSqsClient_DeleteMessage_Failed_Input_Empty() {
	_, err := s.sqsClient.DeleteMessage(nil)

	s.mockSqs.AssertExpectations(s.T())
	s.mockS3.AssertExpectations(s.T())

	assert.NotNil(s.T(), err)
}

func TestExtendedSqsClient(t *testing.T) {
	suite.Run(t, new(ExtendedSqsClientTestSuite))
}

func createLargePayloadMessage(messageId string, s3BucketName string, s3Key string, body string, receiptHandle string) *aws_sqs.Message {
	messagePointer := fmt.Sprintf("[\"software.amazon.payloadoffloading.PayloadS3Pointer\",{\"s3BucketName\":\"%s\",\"s3Key\":\"%s\"}]", s3BucketName, s3Key)
	payloadSize := strconv.Itoa(len(body))

	messageAttributes := make(map[string]*aws_sqs.MessageAttributeValue)
	messageAttributes[sqs_configs_constants.RESERVED_ATTRIBUTE_NAME] = &aws_sqs.MessageAttributeValue{
		DataType:    aws.String("Number"),
		StringValue: &payloadSize,
	}
	message := &aws_sqs.Message{
		MessageId:         &messageId,
		Body:              &messagePointer,
		MessageAttributes: messageAttributes,
		ReceiptHandle:     &receiptHandle,
	}

	return message
}
