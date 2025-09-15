package util

import (
	"encoding/json"
	"fmt"
)

func PrintObject(v interface{}) error {
	jsonOutput, err := json.MarshalIndent(v, "", "\t")
	if err != nil {
		return err
	}

	fmt.Println(string(jsonOutput))
	return nil
}
