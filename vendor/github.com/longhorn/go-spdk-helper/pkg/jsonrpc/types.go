package jsonrpc

import (
	"fmt"
	"regexp"
	"strings"
)

type Message struct {
	ID      uint32      `json:"id"`
	Version string      `json:"jsonrpc"`
	Method  string      `json:"method"`
	Params  interface{} `json:"params"`
}

func NewMessage(id uint32, method string, params interface{}) *Message {
	return &Message{
		ID:      id,
		Version: "2.0",
		Method:  method,
		Params:  params,
	}
}

type RespErrorMsg string

type RespErrorCode int32

const (
	RespErrorCodeInternalError        = -32603
	RespErrorCodeNoEntry              = -2
	RespErrorCodeNoSuchProcess        = -3
	RespErrorCodeDeviceOrResourceBusy = -16
	RespErrorCodeNoFileExists         = -17
	RespErrorCodeNoSuchDevice         = -19
)

const RespErrorMsgNoSuchFileOrDirectory = "No such file or directory"

type Response struct {
	ID        uint32         `json:"id"`
	Version   string         `json:"jsonrpc"`
	Result    interface{}    `json:"result,omitempty"`
	ErrorInfo *ResponseError `json:"error,omitempty"`
}

func (re ResponseError) Error() string {
	return fmt.Sprintf("{\"code\": %d,\"message\": \"%s\"}", re.Code, re.Message)
}

type ResponseError struct {
	Code    RespErrorCode `json:"code"`
	Message RespErrorMsg  `json:"message"`
}

type JSONClientError struct {
	ID          uint32
	Method      string
	Params      interface{}
	ErrorDetail error
}

func (re JSONClientError) Error() string {
	return fmt.Sprintf("error sending message, id %d, method %s, params %+v: %v",
		re.ID, re.Method, re.Params, re.ErrorDetail)
}

func IsJSONRPCRespErrorNoEntry(err error) bool {
	jsonRPCError, ok := err.(JSONClientError)
	if !ok {
		return false
	}
	responseError, ok := jsonRPCError.ErrorDetail.(*ResponseError)
	if !ok {
		return false
	}
	return responseError.Code == RespErrorCodeNoEntry
}

func IsJSONRPCRespErrorNoSuchFileOrDirectory(err error) bool {
	jsonRPCError, ok := err.(JSONClientError)
	if !ok {
		return false
	}
	responseError, ok := jsonRPCError.ErrorDetail.(*ResponseError)
	if !ok {
		return false
	}

	return responseError.Code == RespErrorCodeInternalError &&
		responseError.Message == RespErrorMsgNoSuchFileOrDirectory
}

func IsJSONRPCRespErrorNoSuchProcess(err error) bool {
	jsonRPCError, ok := err.(JSONClientError)
	if !ok {
		return false
	}
	responseError, ok := jsonRPCError.ErrorDetail.(*ResponseError)
	if !ok {
		return false
	}

	return responseError.Code == RespErrorCodeNoSuchProcess
}

func IsJSONRPCRespErrorNoSuchDevice(err error) bool {
	jsonRPCError, ok := err.(JSONClientError)
	if !ok {
		return false
	}
	responseError, ok := jsonRPCError.ErrorDetail.(*ResponseError)
	if !ok {
		return false
	}

	return responseError.Code == RespErrorCodeNoSuchDevice
}

func IsJSONRPCRespErrorDeviceOrResourceBusy(err error) bool {
	jsonRPCError, ok := err.(JSONClientError)
	if !ok {
		return false
	}
	responseError, ok := jsonRPCError.ErrorDetail.(*ResponseError)
	if !ok {
		return false
	}
	// EBUSY may be returned directly as errno -16, or wrapped by SPDK as a
	// generic JSON-RPC internal error (-32603) whose message is the EBUSY
	// strerror string "Device or resource busy" (e.g. ublk_create_target when
	// the singleton target already exists).
	if responseError.Code == RespErrorCodeDeviceOrResourceBusy {
		return true
	}
	return strings.Contains(strings.ToLower(string(responseError.Message)), "device or resource busy")
}

func IsJSONRPCRespErrorFileExists(err error) bool {
	jsonRPCError, ok := err.(JSONClientError)
	if !ok {
		return false
	}
	responseError, ok := jsonRPCError.ErrorDetail.(*ResponseError)
	if !ok {
		return false
	}

	return responseError.Code == RespErrorCodeNoFileExists
}

func IsJSONRPCRespErrorBrokenPipe(err error) bool {
	jsonRPCError, ok := err.(JSONClientError)
	if !ok {
		return false
	}
	_, ok = jsonRPCError.ErrorDetail.(*ResponseError)
	return !ok && strings.Contains(jsonRPCError.ErrorDetail.Error(), "broken pipe")
}

func IsJSONRPCRespErrorInvalidCharacter(err error) bool {
	jsonRPCError, ok := err.(JSONClientError)
	if !ok {
		return false
	}
	_, ok = jsonRPCError.ErrorDetail.(*ResponseError)
	return !ok && strings.Contains(jsonRPCError.ErrorDetail.Error(), "invalid character")
}

func IsJSONRPCRespErrorTransportTypeAlreadyExists(err error) bool {
	jsonRPCError, ok := err.(JSONClientError)
	if !ok {
		return false
	}
	_, ok = jsonRPCError.ErrorDetail.(*ResponseError)
	if !ok {
		return false
	}
	matched, _ := regexp.MatchString("Transport type .* already exists", jsonRPCError.ErrorDetail.Error())
	return matched
}
