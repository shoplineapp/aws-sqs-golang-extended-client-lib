package aws_extended_sqs_client

import (
	"fmt"
	"strconv"
	"strings"

	aws_extended_sqsiface "github.com/shoplineapp/aws-sqs-golang-extended-client-lib/interfaces"
	"github.com/shoplineapp/aws-sqs-golang-extended-client-lib/internal/payload_store"
	sqs_configs_constants "github.com/shoplineapp/aws-sqs-golang-extended-client-lib/services/aws_extended_sqs_client/constants"

	"github.com/shoplineapp/aws-sqs-golang-extended-client-lib/errors"

	"github.com/aws/aws-sdk-go/aws"
	aws_sqs "github.com/aws/aws-sdk-go/service/sqs"
	aws_sqsiface "github.com/aws/aws-sdk-go/service/sqs/sqsiface"
)

type AwsExtendedSQSClient struct {
	aws_sqsiface.SQSAPI
	config       aws_extended_sqsiface.AwsExtendedSqsClientConfigurationInterface
	payloadStore aws_extended_sqsiface.PayloadStoreInterface
}

func NewExtendedSQSClient(sqs aws_sqsiface.SQSAPI, config *AwsExtendedSQSClientConfiguration) *AwsExtendedSQSClient {
	payloadStore := payload_store.NewPayloadStore(config.s3, config.s3BucketName)

	return &AwsExtendedSQSClient{
		SQSAPI:       sqs,
		config:       config,
		payloadStore: payloadStore,
	}
}

func (c *AwsExtendedSQSClient) SendMessage(input *aws_sqs.SendMessageInput) (*aws_sqs.SendMessageOutput, error) {
	if input == nil {
		errorMessage := "sendMessageRequest cannot be null."

		return nil, errors.SDKError{Message: errorMessage}
	}

	if !c.config.IsPayloadSupportEnabled() {
		return c.SQSAPI.SendMessage(input)
	}

	if input.MessageBody == nil {
		errorMessage := "messageBody cannot be null or empty."

		return nil, errors.SDKError{Message: errorMessage}
	}

	if err := c.checkMessageAttributes(input.MessageAttributes); err != nil {
		return nil, err
	}

	var sqsInput *aws_sqs.SendMessageInput

	if c.config.IsAlwaysThroughS3() || c.isLarge(input) {
		var err error
		sqsInput, err = c.storeMessageInS3(input)

		if err != nil {
			return nil, err
		}
	} else {
		sqsInput = input
	}

	return c.SQSAPI.SendMessage(sqsInput)
}

func (c *AwsExtendedSQSClient) ReceiveMessage(input *aws_sqs.ReceiveMessageInput) (*aws_sqs.ReceiveMessageOutput, error) {
	if input == nil {
		errorMessage := "receiveMessageRequest cannot be null."

		return nil, errors.SDKError{Message: errorMessage}
	}

	if !c.config.IsPayloadSupportEnabled() {
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
		return nil, err
	}

	messages := output.Messages
	modifiedMessages := make([]*aws_sqs.Message, len(messages))

	for index, message := range messages {
		modifiedMessage := &aws_sqs.Message{}
		*modifiedMessage = *message

		messageAttributes := message.MessageAttributes
		largePayloadAttributeName := getReservedAttributeNameIfPresent(messageAttributes)
		if largePayloadAttributeName != nil {
			originalPayload, err := c.payloadStore.GetOriginalPayload(*message.Body)
			if err != nil {
				return nil, err
			}

			modifiedMessage.Body = &originalPayload

			// Remove the additional attribute before returning the message to user
			modifiedMessageAttributes := copyMessageAttributes(messageAttributes)
			delete(modifiedMessageAttributes, sqs_configs_constants.RESERVED_ATTRIBUTE_NAME)
			delete(modifiedMessageAttributes, sqs_configs_constants.LEGACY_RESERVED_ATTRIBUTE_NAME)
			modifiedMessage.MessageAttributes = modifiedMessageAttributes

			modifiedReceiptHandle, err := c.embedS3PointerInReceiptHandle(message.ReceiptHandle, message.Body)
			if err != nil {
				return nil, err
			}

			modifiedMessage.ReceiptHandle = modifiedReceiptHandle
		}

		modifiedMessages[index] = modifiedMessage
	}

	output.Messages = modifiedMessages
	return output, nil
}

func (c *AwsExtendedSQSClient) DeleteMessage(input *aws_sqs.DeleteMessageInput) (*aws_sqs.DeleteMessageOutput, error) {
	if input == nil {
		errorMessage := "deleteMessageRequest cannot be null."

		return nil, errors.SDKError{Message: errorMessage}
	}

	if !c.config.IsPayloadSupportEnabled() {
		return c.SQSAPI.DeleteMessage(input)
	}

	receiptHandle := input.ReceiptHandle
	origReceiptHandle := receiptHandle

	if isS3ReceiptHandle(*receiptHandle) {
		handle := getOrigReceiptHandle(*receiptHandle)
		origReceiptHandle = &handle

		if c.config.DoesCleanupS3Payload() {
			messagePointer, err := getMessagePointerFromModifiedReceiptHandle(*receiptHandle)
			if err != nil {
				return nil, err
			}

			if err := c.payloadStore.DeleteOriginalPayload(messagePointer); err != nil {
				return nil, err
			}
		}
	}

	modifiedInput := &aws_sqs.DeleteMessageInput{}
	*modifiedInput = *input

	modifiedInput.ReceiptHandle = origReceiptHandle

	return c.SQSAPI.DeleteMessage(modifiedInput)
}

func (c *AwsExtendedSQSClient) checkMessageAttributes(attributes map[string]*aws_sqs.MessageAttributeValue) error {
	attributeSize := getMsgAttributesSize(attributes)
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

func (c *AwsExtendedSQSClient) isLarge(input *aws_sqs.SendMessageInput) bool {
	attributeSize := getMsgAttributesSize(input.MessageAttributes)
	bodySize := len(*input.MessageBody)

	totalSize := attributeSize + bodySize

	return totalSize > c.config.GetPayloadSizeThreshold()
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
