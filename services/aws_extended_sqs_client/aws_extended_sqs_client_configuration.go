package aws_extended_sqs_client

import (
	aws_extended_sqsiface "github.com/shoplineapp/aws-sqs-golang-extended-client-lib/interfaces"
	sqs_configs_constants "github.com/shoplineapp/aws-sqs-golang-extended-client-lib/services/aws_extended_sqs_client/constants"

	aws_s3iface "github.com/aws/aws-sdk-go/service/s3/s3iface"
)

type AwsExtendedSQSClientConfiguration struct {
	aws_extended_sqsiface.AwsExtendedSqsClientConfigurationInterface
	s3             aws_s3iface.S3API
	s3BucketName   string
	payloadSupport bool

	payloadSizeThreshold int
	alwaysThroughS3      bool
	cleanupS3Payload     bool

	breakSendSupport              bool
	breakSendPayloadSizeThreshold int
}

func NewExtendedSQSClientConfiguration() *AwsExtendedSQSClientConfiguration {
	return &AwsExtendedSQSClientConfiguration{
		s3:                            nil,
		s3BucketName:                  "",
		payloadSupport:                false,
		payloadSizeThreshold:          sqs_configs_constants.DEFAULT_MESSAGE_SIZE_THRESHOLD,
		alwaysThroughS3:               false,
		cleanupS3Payload:              true,
		breakSendSupport:              false,
		breakSendPayloadSizeThreshold: sqs_configs_constants.DEFAULT_BREAK_SEND_MESSAGE_SIZE_THRESHOLD,
	}
}

func (config *AwsExtendedSQSClientConfiguration) WithPayloadSupportEnabled(s3 aws_s3iface.S3API, s3BucketName string) {
	config.s3 = s3
	config.s3BucketName = s3BucketName
	config.payloadSupport = true
}

func (config *AwsExtendedSQSClientConfiguration) WithBreakSendSupportEnabled() {
	config.breakSendSupport = true
}

func (config *AwsExtendedSQSClientConfiguration) SetPayloadSizeThreshold(threshold int) {
	config.payloadSizeThreshold = threshold
}

func (config *AwsExtendedSQSClientConfiguration) SetAlwaysThroughS3(alwaysThroughS3 bool) {
	config.alwaysThroughS3 = alwaysThroughS3
}

func (config *AwsExtendedSQSClientConfiguration) SetCleanupS3Payload(cleanupS3Payload bool) {
	config.cleanupS3Payload = cleanupS3Payload
}

func (config *AwsExtendedSQSClientConfiguration) IsPayloadSupportEnabled() bool {
	return config.payloadSupport
}

func (config *AwsExtendedSQSClientConfiguration) GetPayloadSizeThreshold() int {
	return config.payloadSizeThreshold
}

func (config *AwsExtendedSQSClientConfiguration) IsAlwaysThroughS3() bool {
	return config.alwaysThroughS3
}

func (config *AwsExtendedSQSClientConfiguration) DoesCleanupS3Payload() bool {
	return config.cleanupS3Payload
}
