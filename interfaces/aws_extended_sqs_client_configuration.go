package aws_extended_sqsiface

import (
	aws_s3iface "github.com/aws/aws-sdk-go/service/s3/s3iface"
)

type AwsExtendedSqsClientConfigurationInterface interface {
	WithPayloadSupportEnabled(s3 aws_s3iface.S3API, s3BucketName string)
	SetPayloadSizeThreshold(threshold int)
	SetAlwaysThroughS3(alwaysThroughS3 bool)
	SetCleanupS3Payload(cleanupS3Payload bool)
	IsPayloadSupportEnabled() bool
	GetPayloadSizeThreshold() int
	IsAlwaysThroughS3() bool
	DoesCleanupS3Payload() bool
}
