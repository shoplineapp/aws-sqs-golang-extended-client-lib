package aws_extended_sqs_client

import (
	"fmt"
	"strconv"
	"strings"

	aws_extended_sqsiface "github.com/shoplineapp/aws-sqs-golang-extended-client-lib/interfaces"
	"github.com/shoplineapp/aws-sqs-golang-extended-client-lib/internal/payload_store"
	sqs_configs_constants "github.com/shoplineapp/aws-sqs-golang-extended-client-lib/services/aws_extended_sqs_client/constants"
	"github.com/sirupsen/logrus"

	"github.com/shoplineapp/aws-sqs-golang-extended-client-lib/errors"

	"github.com/aws/aws-sdk-go/aws"
	aws_sqs "github.com/aws/aws-sdk-go/service/sqs"
	aws_sqsiface "github.com/aws/aws-sdk-go/service/sqs/sqsiface"
)

type AwsExtendedSQSClient struct {
	aws_sqsiface.SQSAPI
	config       aws_extended_sqsiface.AwsExtendedSqsClientConfigurationInterface
	payloadStore aws_extended_sqsiface.PayloadStoreInterface
	opts         *awsExtendedSQSClientOptions
}

type awsExtendedSQSClientOptions struct {
	logger logrus.FieldLogger
}

type AwsExtendedSQSClientOption func(*awsExtendedSQSClientOptions)

func newClientOptions() *awsExtendedSQSClientOptions {
	return &awsExtendedSQSClientOptions{
		logger: logrus.New(),
	}
}

func WithLogger(logger logrus.FieldLogger) AwsExtendedSQSClientOption {
	return func(opts *awsExtendedSQSClientOptions) {
		opts.logger = logger
	}
}

func NewExtendedSQSClient(sqs aws_sqsiface.SQSAPI, config *AwsExtendedSQSClientConfiguration, opts ...AwsExtendedSQSClientOption) *AwsExtendedSQSClient {
	payloadStore := payload_store.NewPayloadStore(config.s3, config.s3BucketName)

	client := &AwsExtendedSQSClient{
		SQSAPI:       sqs,
		config:       config,
		payloadStore: payloadStore,
		opts:         newClientOptions(),
	}

	for _, opt := range opts {
		opt(client.opts)
	}

	return client
}

func (c *AwsExtendedSQSClient) SendMessage(input *aws_sqs.SendMessageInput) (*aws_sqs.SendMessageOutput, error) {
	logger := c.opts.logger.WithField("method", "SendMessage")

	if input == nil {
		logger.WithField("uploaded_to_s3", "false").Infoln("Handled by original sqs sdk")

		// let parent handle the error
		return c.SQSAPI.SendMessage(input)
	}

	logger = logger.WithFields(c.getLoggingFields(input.MessageAttributes))

	if !c.config.IsPayloadSupportEnabled() {
		logger.WithField("uploaded_to_s3", "false").Infoln("Handled by original sqs sdk")

		return c.SQSAPI.SendMessage(input)
	}

	if input.MessageBody == nil {
		logger.WithField("uploaded_to_s3", "false").Infoln("Handled by original sqs sdk")

		// let parent handle the error
		return c.SQSAPI.SendMessage(input)
	}

	destination, err := c.getMessageDestination(input, logger)
	if err != nil {
		return &aws_sqs.SendMessageOutput{}, err
	}

	var sqsInput *aws_sqs.SendMessageInput

	switch destination {
	case "s3":
		var err error
		sqsInput, err = c.storeMessageInS3(input)

		if err != nil {
			logger.WithField("method", "storeMessageInS3").Errorf("Error: %+v\n", err)
			return &aws_sqs.SendMessageOutput{}, err
		}

		logger.WithField("uploaded_to_s3", "true").Infoln("Uploaded to s3")
		break
	case "sqs":
		logger.WithField("uploaded_to_s3", "false").Infoln("Handled by original sqs sdk")

		sqsInput = input
		break
	default:
		errorMessage := "Unknown message destination"
		logger.WithField("destination", destination).Errorln(errorMessage)
		return &aws_sqs.SendMessageOutput{}, errors.SDKError{Message: errorMessage}
	}

	return c.SQSAPI.SendMessage(sqsInput)
}

func (c *AwsExtendedSQSClient) ReceiveMessage(input *aws_sqs.ReceiveMessageInput) (*aws_sqs.ReceiveMessageOutput, error) {
	logger := c.opts.logger.WithField("method", "ReceiveMessage")

	if input == nil {
		logger.Infoln("Handled by original sqs sdk")

		// let parent handle the error
		return c.SQSAPI.ReceiveMessage(input)
	}

	if !c.config.IsPayloadSupportEnabled() {
		logger.Infoln("Handled by original sqs sdk")

		return c.SQSAPI.ReceiveMessage(input)
	}

	reservdAttributeName := sqs_configs_constants.RESERVED_ATTRIBUTE_NAME
	legacyReservedAttributeName := sqs_configs_constants.LEGACY_RESERVED_ATTRIBUTE_NAME
	var updatedMessageAttributeNames []*string
	for _, name := range input.MessageAttributeNames {
		if *name != reservdAttributeName && *name != legacyReservedAttributeName {
			copied_name := *name
			updatedMessageAttributeNames = append(updatedMessageAttributeNames, &copied_name)
		}
	}
	updatedMessageAttributeNames = append(updatedMessageAttributeNames, &reservdAttributeName)
	updatedMessageAttributeNames = append(updatedMessageAttributeNames, &legacyReservedAttributeName)

	updatedInput := &aws_sqs.ReceiveMessageInput{}
	*updatedInput = *input
	updatedInput.MessageAttributeNames = updatedMessageAttributeNames

	output, err := c.SQSAPI.ReceiveMessage(updatedInput)
	if err != nil {
		logger.WithField("method", "ReceiveMessage").Errorf("Error: %+v\n", err)

		return output, err
	}

	messages := output.Messages
	modifiedMessages := make([]*aws_sqs.Message, len(messages))

	for index, message := range messages {
		modifiedMessage := &aws_sqs.Message{}
		*modifiedMessage = *message

		messageAttributes := message.MessageAttributes
		largePayloadAttributeName := getReservedAttributeNameIfPresent(messageAttributes)
		if largePayloadAttributeName != nil {
			loggerWithAttrs := c.opts.logger.WithFields(c.getLoggingFields(messageAttributes))

			loggerWithAttrs.Infoln("Getting payload from s3")

			originalPayload, err := c.payloadStore.GetOriginalPayload(*message.Body)
			if err != nil {
				loggerWithAttrs.WithField("method", "GetOriginalPayload").Errorf("Error: %+v\n", err)

				return &aws_sqs.ReceiveMessageOutput{}, err
			}

			modifiedMessage.Body = &originalPayload

			// Remove the additional attribute before returning the message to user
			modifiedMessageAttributes := copyMessageAttributes(messageAttributes)
			delete(modifiedMessageAttributes, sqs_configs_constants.RESERVED_ATTRIBUTE_NAME)
			delete(modifiedMessageAttributes, sqs_configs_constants.LEGACY_RESERVED_ATTRIBUTE_NAME)
			modifiedMessage.MessageAttributes = modifiedMessageAttributes

			modifiedReceiptHandle, err := c.embedS3PointerInReceiptHandle(message.ReceiptHandle, message.Body)
			if err != nil {
				loggerWithAttrs.WithField("method", "embedS3PointerInReceiptHandle").Errorf("Error: %+v\n", err)

				return &aws_sqs.ReceiveMessageOutput{}, err
			}

			modifiedMessage.ReceiptHandle = modifiedReceiptHandle

			loggerWithAttrs.Infoln("Finished getting payload from s3")
		}

		modifiedMessages[index] = modifiedMessage
	}

	output.Messages = modifiedMessages
	return output, nil
}

func (c *AwsExtendedSQSClient) DeleteMessage(input *aws_sqs.DeleteMessageInput) (*aws_sqs.DeleteMessageOutput, error) {
	logger := c.opts.logger.WithField("method", "DeleteMessage")

	if input == nil {
		logger.Infoln("Handled by original sqs sdk")

		// let parent handle the error
		return c.SQSAPI.DeleteMessage(input)
	}

	if !c.config.IsPayloadSupportEnabled() {
		logger.Infoln("Handled by original sqs sdk")

		return c.SQSAPI.DeleteMessage(input)
	}

	receiptHandle := input.ReceiptHandle
	origReceiptHandle := receiptHandle

	if origReceiptHandle == nil {
		logger.Infoln("Handled by original sqs sdk")

		// let parent handle the error
		return c.SQSAPI.DeleteMessage(input)
	}

	logger = logger.WithField("receipt_handle", *input.ReceiptHandle)

	if isS3ReceiptHandle(*receiptHandle) {
		handle := getOrigReceiptHandle(*receiptHandle)
		origReceiptHandle = &handle

		logger.Infoln("Message is sent with s3 usage")

		if c.config.DoesCleanupS3Payload() {
			logger.Infoln("Deleting message in s3")

			messagePointer, err := getMessagePointerFromModifiedReceiptHandle(*receiptHandle)
			if err != nil {
				logger.WithField("method", "getMessagePointerFromModifiedReceiptHandle").Errorf("Error: %+v\n", err)
				return &aws_sqs.DeleteMessageOutput{}, err
			}

			if err := c.payloadStore.DeleteOriginalPayload(messagePointer); err != nil {
				logger.WithField("method", "DeleteOriginalPayload").Errorf("Error: %+v\n", err)
				return &aws_sqs.DeleteMessageOutput{}, err
			}

			logger.Infoln("Deleted message in s3")
		}
	} else {
		logger.Infoln("Message is sent without s3")
	}

	modifiedInput := &aws_sqs.DeleteMessageInput{}
	*modifiedInput = *input

	modifiedInput.ReceiptHandle = origReceiptHandle

	return c.SQSAPI.DeleteMessage(modifiedInput)
}

func (c *AwsExtendedSQSClient) checkMessageAttributes(attributes map[string]*aws_sqs.MessageAttributeValue, attributeSize int) error {
	sizeThreshold := c.config.GetPayloadSizeThreshold()
	if attributeSize > sizeThreshold {
		errorMessage := fmt.Sprintf("Total size of Message attributes is %s bytes which is larger than the threshold of %s Bytes. Consider including the payload in the message body instead of message attributes.", strconv.Itoa(attributeSize), strconv.Itoa(sizeThreshold))

		return errors.SDKError{Message: errorMessage}
	}

	attributesLen := len(attributes)
	if attributesLen > sqs_configs_constants.MAX_ALLOWED_ATTRIBUTES {
		errorMessage := fmt.Sprintf("Number of message attributes [%s] exceeds the maximum allowed for large-payload messages [%s].", strconv.Itoa(attributesLen), strconv.Itoa(sqs_configs_constants.MAX_ALLOWED_ATTRIBUTES))

		return errors.SDKError{Message: errorMessage}
	}

	reserved_attribute := getReservedAttributeNameIfPresent(attributes)
	if reserved_attribute != nil {
		errorMessage := fmt.Sprintf("Message attribute name %s is reserved for use by SQS extended client.", *reserved_attribute)

		return errors.SDKError{Message: errorMessage}
	}

	return nil
}

func (c *AwsExtendedSQSClient) getMessageDestination(input *aws_sqs.SendMessageInput, logger logrus.FieldLogger) (string, error) {
	attributeSize := getMsgAttributesSize(input.MessageAttributes)
	if err := c.checkMessageAttributes(input.MessageAttributes, attributeSize); err != nil {
		logger.WithField("method", "checkMessageAttributes").Errorf("Error: %+v\n", err)
		return "", err
	}

	if c.config.IsAlwaysThroughS3() {
		return "s3", nil
	}

	bodySize := len(*input.MessageBody)
	totalSize := attributeSize + bodySize

	logger.WithField("message_size", strconv.Itoa(totalSize)).Infoln("Calculated payload size")

	if c.config.IsBreakSendSupportEnabled() && bodySize > c.config.GetBreakSendPayloadSizeThreshold() {
		errorMessage := fmt.Sprintf("Total size of message is %s, exceeds the maximum allowed. Message send process is breaked.")

		logger.WithField("method", "getMessageDestination").Errorf("Error: %+v\n", errorMessage)

		return "", errors.SDKError{Message: errorMessage}
	} else if totalSize > c.config.GetPayloadSizeThreshold() {
		return "s3", nil
	}

	return "sqs", nil
}

func (c *AwsExtendedSQSClient) storeMessageInS3(input *aws_sqs.SendMessageInput) (*aws_sqs.SendMessageInput, error) {
	messageBodySize := len(*input.MessageBody)

	updatedInput := &aws_sqs.SendMessageInput{}
	*updatedInput = *input

	newMessageAttributes := copyMessageAttributes(input.MessageAttributes)

	messageBodySizeStr := strconv.Itoa(messageBodySize)
	// Default use RESERVED_ATTRIBUTE_NAME
	newMessageAttributes[sqs_configs_constants.RESERVED_ATTRIBUTE_NAME] = &aws_sqs.MessageAttributeValue{
		DataType:    aws.String("Number"),
		StringValue: aws.String(messageBodySizeStr),
	}

	updatedInput.MessageAttributes = newMessageAttributes

	messagePointer, err := c.payloadStore.StoreOriginalPayload(*input.MessageBody)
	if err != nil {
		return nil, err
	}

	updatedInput.MessageBody = &messagePointer

	return updatedInput, nil
}

func (c *AwsExtendedSQSClient) embedS3PointerInReceiptHandle(receiptHandle *string, messagePointer *string) (*string, error) {
	s3Pointer, err := payload_store.FromJson(*messagePointer)
	if err != nil {
		return nil, err
	}

	s3MsgBucketName := s3Pointer.S3BucketName
	s3MsgKey := s3Pointer.S3Key

	modifiedReceiptHandle := sqs_configs_constants.S3_BUCKET_NAME_MARKER + s3MsgBucketName + sqs_configs_constants.S3_BUCKET_NAME_MARKER +
		sqs_configs_constants.S3_KEY_MARKER + s3MsgKey + sqs_configs_constants.S3_KEY_MARKER +
		*receiptHandle

	return &modifiedReceiptHandle, nil
}

func (c *AwsExtendedSQSClient) getLoggingFields(attributes map[string]*aws_sqs.MessageAttributeValue) logrus.Fields {
	fields := logrus.Fields{}

	if attributes == nil {
		return fields
	}

	for attributeName, value := range attributes {
		if value != nil && value.StringValue != nil {
			fields[attributeName] = *value.StringValue
		}
	}

	return fields
}

func getReservedAttributeNameIfPresent(attributes map[string]*aws_sqs.MessageAttributeValue) *string {
	var reservedAttributeName string
	if _, ok := attributes[sqs_configs_constants.RESERVED_ATTRIBUTE_NAME]; ok {
		reservedAttributeName = sqs_configs_constants.RESERVED_ATTRIBUTE_NAME
	} else if _, ok := attributes[sqs_configs_constants.LEGACY_RESERVED_ATTRIBUTE_NAME]; ok {
		reservedAttributeName = sqs_configs_constants.LEGACY_RESERVED_ATTRIBUTE_NAME
	} else {
		return nil
	}

	return &reservedAttributeName
}

func isS3ReceiptHandle(receiptHandle string) bool {
	return strings.Contains(receiptHandle, sqs_configs_constants.S3_BUCKET_NAME_MARKER) &&
		strings.Contains(receiptHandle, sqs_configs_constants.S3_KEY_MARKER)
}

func getOrigReceiptHandle(receiptHandle string) string {
	firstOccurrence := strings.Index(receiptHandle, sqs_configs_constants.S3_KEY_MARKER)
	offset := firstOccurrence + 1
	secondOccurrence := strings.Index(receiptHandle[firstOccurrence+1:], sqs_configs_constants.S3_KEY_MARKER) + offset

	return receiptHandle[secondOccurrence+len(sqs_configs_constants.S3_KEY_MARKER):]
}

func getMessagePointerFromModifiedReceiptHandle(receiptHandle string) (string, error) {
	s3MsgBucketName := getFromReceiptHandleByMarker(receiptHandle, sqs_configs_constants.S3_BUCKET_NAME_MARKER)
	s3MsgKey := getFromReceiptHandleByMarker(receiptHandle, sqs_configs_constants.S3_KEY_MARKER)

	payloadS3Pointer := &payload_store.PayloadS3Pointer{
		S3BucketName: s3MsgBucketName,
		S3Key:        s3MsgKey,
	}

	return payloadS3Pointer.ToJson()
}

func getFromReceiptHandleByMarker(receiptHandle string, marker string) string {
	firstOccurrence := strings.Index(receiptHandle, marker)
	offset := firstOccurrence + 1
	secondOccurrence := strings.Index(receiptHandle[offset:], marker) + offset

	return receiptHandle[firstOccurrence+len(marker) : secondOccurrence]
}

func getMsgAttributesSize(attributes map[string]*aws_sqs.MessageAttributeValue) int {
	totalMsgAttributesSize := 0

	for key, value := range attributes {
		totalMsgAttributesSize += len(key)

		if value.DataType != nil {
			totalMsgAttributesSize += len(*value.DataType)
		}

		strVal := value.StringValue
		if strVal != nil {
			totalMsgAttributesSize += len(*strVal)
		}

		binaryVal := value.BinaryValue
		if binaryVal != nil {
			totalMsgAttributesSize += len(binaryVal)
		}
	}

	return totalMsgAttributesSize
}

func copyMessageAttributes(attributes map[string]*aws_sqs.MessageAttributeValue) map[string]*aws_sqs.MessageAttributeValue {
	newMessageAttributes := make(map[string]*aws_sqs.MessageAttributeValue)
	for key := range attributes {
		newMessageAttributes[key] = attributes[key]
	}

	return newMessageAttributes
}
