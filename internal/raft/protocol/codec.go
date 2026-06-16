package protocol

import "encoding/json"

func ParseMessage[T any](body string) (T, error) {
	var result T

	err := json.Unmarshal([]byte(body), &result)
	if err != nil {
		return result, err
	}

	return result, nil
}

func SerializeMessage[T any](body T) (string, error) {
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return "", err
	}
	return string(bodyBytes), nil
}
