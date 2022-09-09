package errors

import (
	"fmt"

	"github.com/shoplineapp/aws-sqs-golang-extended-client-lib/interfaces"
)

type PointerFormatError struct {
	interfaces.ErrorInterface
	Message string
}

func (e PointerFormatError) Code() string {
	return "PointerFormatError"
}

func (e PointerFormatError) Error() string {
	return fmt.Sprintf("%s - %s", e.Code(), e.Message)
}
