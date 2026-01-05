package dsql

import (
	"errors"
	"fmt"
)

// ErrorType represents the classification of DSQL errors.
type ErrorType int

const (
	ErrorTypeUnknown ErrorType = iota
	ErrorTypeRetryable
	ErrorTypeConditionFailed
	ErrorTypeUnsupportedFeature
	ErrorTypePermanent
)

func (e ErrorType) String() string {
	switch e {
	case ErrorTypeRetryable:
		return "retryable"
	case ErrorTypeConditionFailed:
		return "condition_failed"
	case ErrorTypeUnsupportedFeature:
		return "unsupported_feature"
	case ErrorTypePermanent:
		return "permanent"
	default:
		return "unknown"
	}
}

// ConditionFailedError represents a CAS/fencing condition failure.
// This is a *normal* outcome under contention and MUST NOT be retried by this layer.
type ConditionFailedError struct {
	Msg string
}

func (e *ConditionFailedError) Error() string {
	return fmt.Sprintf("condition failed: %s", e.Msg)
}

// IsConditionFailedError returns true if err is or wraps a ConditionFailedError.
func IsConditionFailedError(err error) bool {
	var cfe *ConditionFailedError
	return errors.As(err, &cfe)
}

// NewConditionFailedError creates a new condition failed error
func NewConditionFailedError(msg string, args ...interface{}) *ConditionFailedError {
	return &ConditionFailedError{
		Msg: fmt.Sprintf(msg, args...),
	}
}