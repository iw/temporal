package dsql

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
