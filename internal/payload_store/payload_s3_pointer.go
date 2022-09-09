package payload_store

import (
	"encoding/json"

	"github.com/shoplineapp/aws-sqs-golang-extended-client-lib/errors"
)

// Used for serializing & deserializing the string in compatible format with the java sdk
// i.e. "[\"software.amazon.payloadoffloading.PayloadS3Pointer\",{\"s3BucketName\":\"xxx\",\"s3Key\":\"xxx\"}]"
type PayloadS3PointerJson struct {
	S3BucketName string `json:"s3BucketName"`
	S3Key        string `json:"s3Key"`
}

type PayloadS3Pointer struct {
	S3BucketName string
	S3Key        string
}

func (p *PayloadS3Pointer) UnmarshalJSON(data []byte) error {
	arr := []PayloadS3PointerJson{}

	err := json.Unmarshal(data, &arr)

	if len(arr) != 2 {
		if err != nil {
			return err
		}
		return errors.PointerFormatError{
			Message: "Invalid pointer format",
		}
	}

	p.S3BucketName = arr[1].S3BucketName
	p.S3Key = arr[1].S3Key

	return nil
}

func (p *PayloadS3Pointer) MarshalJSON() ([]byte, error) {
	pointerJson := PayloadS3PointerJson{
		S3BucketName: p.S3BucketName,
		S3Key:        p.S3Key,
	}

	arr := []interface{}{}
	arr = append(arr, "software.amazon.payloadoffloading.PayloadS3Pointer")
	arr = append(arr, pointerJson)

	return json.Marshal(&arr)
}

func (p *PayloadS3Pointer) ToJson() (string, error) {
	messagePointer, err := json.Marshal(p)

	if err != nil {
		return "", err
	}

	return string(messagePointer), nil
}

func FromJson(pointerStr string) (*PayloadS3Pointer, error) {
	var payloadPointer PayloadS3Pointer
	err := json.Unmarshal([]byte(pointerStr), &payloadPointer)

	if err != nil {
		return nil, err
	}

	return &payloadPointer, nil
}
