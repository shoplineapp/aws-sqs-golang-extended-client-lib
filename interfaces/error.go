package interfaces

type ErrorInterface interface {
	Code() string
	Error() string
}
