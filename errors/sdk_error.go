package errors

import (
	"fmt"

	aws_extended_sqsiface "github.com/shoplineapp/aws-sqs-golang-extended-client-lib/interfaces"
)

type SDKError struct {
	aws_extended_sqsiface.ErrorInterface
	Message string
}

func (e SDKError) Code() string {
	return "AwsSqsGoExtendedClientSDKError"
}

func (e SDKError) Error() string {
	return fmt.Sprintf("%s - %s", e.Code(), e.Message)
}
