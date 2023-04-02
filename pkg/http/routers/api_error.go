package routers

import (
	"fmt"
	"net/http"

	"github.com/go-chi/render"
)

type APIError struct {
	httpStatusCode int
	ErrorCode      int    `json:"error_code"`
	Message        string `json:"message"`
	err            error
}

func NewAPIError(httpStatusCode int, schemaRegistryErrorCode int, err error) *APIError {
	return &APIError{
		httpStatusCode: httpStatusCode,
		ErrorCode:      schemaRegistryErrorCode,
		Message:        err.Error(),
		err:            err,
	}
}

func (a *APIError) Error() string {
	return fmt.Sprintf("apiError: %s", a.Message)
}

func (a *APIError) Unwrap() error {
	return a.err
}

func (a *APIError) Render(w http.ResponseWriter, r *http.Request) error {
	render.Status(r, a.httpStatusCode)
	return nil
}
