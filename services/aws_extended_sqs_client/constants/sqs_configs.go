package sqs_configs_constants

const (
	RESERVED_ATTRIBUTE_NAME        = "ExtendedPayloadSize"
	LEGACY_RESERVED_ATTRIBUTE_NAME = "SQSLargePayloadSize"
	MAX_ALLOWED_ATTRIBUTES         = 10 - 1
	DEFAULT_MESSAGE_SIZE_THRESHOLD = 262144
	S3_BUCKET_NAME_MARKER          = "-..s3BucketName..-"
	S3_KEY_MARKER                  = "-..s3Key..-"
)
