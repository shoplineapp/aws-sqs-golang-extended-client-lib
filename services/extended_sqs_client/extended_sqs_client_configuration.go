package extended_sqs_client

import (
	"github.com/shoplineapp/aws-sqs-golang-extended-client-lib/interfaces"
	sqs_configs_constants "github.com/shoplineapp/aws-sqs-golang-extended-client-lib/services/extended_sqs_client/constants"

	aws_s3iface "github.com/aws/aws-sdk-go/service/s3/s3iface"
)

type ExtendedSQSClientConfiguration struct {
	interfaces.ExtendedSQSClientConfigurationInterface
	s3             aws_s3iface.S3API
	s3BucketName   string
	payloadSupport bool

	payloadSizeThreshold int
	alwaysThroughS3      bool
	cleanupS3Payload     bool
}

func NewExtendedSQSClientConfiguration() *ExtendedSQSClientConfiguration {
	return &ExtendedSQSClientConfiguration{
		s3:                   nil,
		s3BucketName:         "",
		payloadSupport:       false,
		payloadSizeThreshold: sqs_configs_constants.DEFAULT_MESSAGE_SIZE_THRESHOLD,
		alwaysThroughS3:      false,
		cleanupS3Payload:     true,
	}
}

func (config *ExtendedSQSClientConfiguration) WithPayloadSupportEnabled(s3 aws_s3iface.S3API, s3BucketName string) {
	config.s3 = s3
	config.s3BucketName = s3BucketName
	config.payloadSupport = true
}

func (config *ExtendedSQSClientConfiguration) SetPayloadSizeThreshold(threshold int) {
	config.payloadSizeThreshold = threshold
}

func (config *ExtendedSQSClientConfiguration) SetAlwaysThroughS3(alwaysThroughS3 bool) {
	config.alwaysThroughS3 = alwaysThroughS3
}

func (config *ExtendedSQSClientConfiguration) SetCleanupS3Payload(cleanupS3Payload bool) {
	config.cleanupS3Payload = cleanupS3Payload
}

func (config *ExtendedSQSClientConfiguration) IsPayloadSupportEnabled() bool {
	return config.payloadSupport
}

func (config *ExtendedSQSClientConfiguration) GetPayloadSizeThreshold() int {
	return config.payloadSizeThreshold
}

func (config *ExtendedSQSClientConfiguration) IsAlwaysThroughS3() bool {
	return config.alwaysThroughS3
}

func (config *ExtendedSQSClientConfiguration) DoesCleanupS3Payload() bool {
	return config.cleanupS3Payload
}
