package aws_extended_sqsiface

type ErrorInterface interface {
	Code() string
	Error() string
}
