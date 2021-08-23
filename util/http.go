package util

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/sirupsen/logrus"
)

func CopyReq(req *http.Request) *http.Request {
	r := *req
	buf, _ := ioutil.ReadAll(r.Body)
	req.Body = ioutil.NopCloser(bytes.NewBuffer(buf))
	r.Body = ioutil.NopCloser(bytes.NewBuffer(buf))
	return &r
}

func ResponseBody(obj interface{}) []byte {
	respBody, err := json.Marshal(obj)
	if err != nil {
		return []byte(`{\"errors\":[\"Failed to parse response body\"]}`)
	}
	return respBody
}

func ResponseOKWithBody(rw http.ResponseWriter, obj interface{}) {
	rw.Header().Set("Content-type", "application/json")
	rw.WriteHeader(http.StatusOK)
	_, err := rw.Write(ResponseBody(obj))
	if err != nil {
		logrus.Debug("Can not write back OK response")
	}
}

func ResponseOK(rw http.ResponseWriter) {
	rw.WriteHeader(http.StatusOK)
}

func ResponseError(rw http.ResponseWriter, statusCode int, err error) {
	ResponseErrorMsg(rw, statusCode, err.Error())
}

func ResponseErrorMsg(rw http.ResponseWriter, statusCode int, errMsg string) {
	rw.WriteHeader(statusCode)
	_, err := rw.Write(ResponseBody([]string{errMsg}))
	if err != nil {
		logrus.Debug("Can not write back error message")
	}
}
