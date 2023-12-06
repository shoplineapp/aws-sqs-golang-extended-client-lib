package errors

import (
	"fmt"

	aws_extended_sqsiface "github.com/shoplineapp/aws-sqs-golang-extended-client-lib/interfaces"
)

type OversizeBreakError struct {
	aws_extended_sqsiface.ErrorInterface
	Message string
	Size    int
}

func (e OversizeBreakError) Code() string {
	return "AwsSqsGoExtendedClientOversizeBreakError"
}

func (e OversizeBreakError) Error() string {
	return fmt.Sprintf("%s - %s", e.Code(), e.Message)
}
