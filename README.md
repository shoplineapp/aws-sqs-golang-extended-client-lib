# aws-sqs-golang-extended-client-lib

Extended SQS SDK in go that enables you to manage AWS SQS message payloads with S3, which is useful for storing and retrieving messages with payload size exceeding SQS limit (> 256 KB).

## Getting Started

- Setup AWS SQS queue and S3 bucket

### Installation

Run the following command to install the package:

```
go get github.com/shoplineapp/aws-sqs-golang-extended-client-lib@[commit id]
```

## Example Usage

```go
awsConfig := &aws.Config{
    Region:           aws.String(AWS_REGION),
    Endpoint:         aws.String(AWS_ENDPOINT), // Used for local dev using localstack
    S3ForcePathStyle: aws.Bool(AWS_S3_FORCE_PATH_STYLE), // Used for local dev using localstack
    LogLevel:         aws.LogLevel(aws.LogDebugWithHTTPBody),   // Used for debugging
}

session := aws_session.Must(aws_session.NewSession(awsConfig))
// Setup SQS client
sqsClient := aws_sqs.New(session)

// Setup S3
s3Client := aws_s3.New(session)
s3BucketName := "sample-bucket"

// Initialize sdk
extendedSqsClientConfig := extended_sqs.NewExtendedSQSClientConfiguration()
extendedSqsClientConfig.WithPayloadSupportEnabled(s3Client, s3BucketName)

extendedSqsClient := extended_sqs.NewExtendedSQSClient(sqsClient, extendedSqsClientConfig)

// Send Message
sendMessageInput := &aws_sqs.SendMessageInput{
    MessageBody:       &large_body,
    QueueUrl:          &QUEUE_URL,
}
output, err := extendedSqsClient.SendMessage(sendMessageInput)

// Receive Message
receiveOutput, err := extendedSqsClient.ReceiveMessage(&aws_sqs.ReceiveMessageInput{
    QueueUrl: &QUEUE_URL,
})

// Delete Message
receiptHandle := "xxx"

output, err := extendedSqsClient.DeleteMessage(&aws_sqs.DeleteMessageInput{
    QueueUrl:      &QUEUE_URL,
    ReceiptHandle: &receiptHandle,
})
```

## Unit test

Files under the tests directory will be executed. A coverage report on all imported packages except for the unit test package will be generated.

```
go test -gcflags=all=-l -coverpkg ./services/...,./internal/... ./tests... -coverprofile=coverage/coverage.out
```

The portion of all imported packages that are tested will be printed.

```
go tool cover -func=coverage/coverage.out
```