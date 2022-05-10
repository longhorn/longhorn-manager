package error

import (
	"net/http"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type AdmitError struct {
	message string
	code    int32
	reason  metav1.StatusReason
	causes  []metav1.StatusCause
}

func (e AdmitError) Error() string {
	return e.message
}

func (e AdmitError) AsResult() *metav1.Status {
	status := metav1.Status{
		Status:  "Failure",
		Message: e.message,
		Code:    e.code,
		Reason:  e.reason,
	}

	if len(e.causes) > 0 {
		status.Details = &metav1.StatusDetails{
			Causes: e.causes,
		}
	}

	return &status
}

// NewBadRequest returns HTTP status code 400
func NewBadRequest(message string) AdmitError {
	return AdmitError{
		code:    http.StatusBadRequest,
		message: message,
		reason:  metav1.StatusReasonBadRequest,
	}
}

// NewMethodNotAllowed returns HTTP status code 405
func NewMethodNotAllowed(message string) AdmitError {
	return AdmitError{
		code:    http.StatusMethodNotAllowed,
		message: message,
		reason:  metav1.StatusReasonMethodNotAllowed,
	}
}

// NewInvalidError returns HTTP status code 422
func NewInvalidError(message string, field string) AdmitError {
	return AdmitError{
		code:    http.StatusUnprocessableEntity,
		message: message,
		reason:  metav1.StatusReasonInvalid,
		causes: []metav1.StatusCause{
			{
				Type:    metav1.CauseTypeFieldValueInvalid,
				Message: message,
				Field:   field,
			},
		},
	}
}

// NewConflict returns HTTP status code 409
func NewConflict(message string) AdmitError {
	return AdmitError{
		code:    http.StatusConflict,
		message: message,
		reason:  metav1.StatusReasonConflict,
	}
}

// NewInternalError returns HTTP status code 500
func NewInternalError(message string) AdmitError {
	return AdmitError{
		code:    http.StatusInternalServerError,
		message: message,
		reason:  metav1.StatusReasonInternalError,
	}
}

// NewForbiddenError returns HTTP status code 403
func NewForbiddenError(message string) AdmitError {
	return AdmitError{
		code:    http.StatusForbidden,
		message: message,
		reason:  metav1.StatusReasonForbidden,
	}
}
