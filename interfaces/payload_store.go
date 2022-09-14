package aws_extended_sqsiface

type PayloadStoreInterface interface {
	StoreOriginalPayload(originalPayload string) (string, error)
	GetOriginalPayload(messagePointer string) (string, error)
	DeleteOriginalPayload(messagePointer string) error
}
