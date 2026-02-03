package dsql

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

const (
	// TokenBucketEnabledEnvVar enables the token bucket rate limiter.
	TokenBucketEnabledEnvVar = "DSQL_TOKEN_BUCKET_ENABLED"

	// TokenBucketRateEnvVar overrides the default token refill rate (tokens/sec).
	TokenBucketRateEnvVar = "DSQL_TOKEN_BUCKET_RATE"

	// TokenBucketCapacityEnvVar overrides the default bucket capacity.
	TokenBucketCapacityEnvVar = "DSQL_TOKEN_BUCKET_CAPACITY"

	// TokenBucketMaxWaitEnvVar overrides the default max wait time.
	TokenBucketMaxWaitEnvVar = "DSQL_TOKEN_BUCKET_MAX_WAIT"

	// DefaultTokenBucketRate is DSQL's sustained connection rate limit.
	DefaultTokenBucketRate = 100

	// DefaultTokenBucketCapacity is DSQL's burst capacity.
	DefaultTokenBucketCapacity = 1000

	// DefaultTokenBucketMaxWait is the default max wait time for acquiring a token.
	DefaultTokenBucketMaxWait = 30 * time.Second

	// DefaultTokenBucketBackoff is the base backoff duration between retries.
	DefaultTokenBucketBackoff = 50 * time.Millisecond

	// tokenBucketPKPrefix is the partition key prefix for token bucket items.
	tokenBucketPKPrefix = "dsql_connect_bucket#"

	// milliMultiplier converts tokens to milli-tokens (avoid floats).
	milliMultiplier = 1000
)

// TokenBucketLimiter implements distributed rate limiting using a token bucket model.
//
// This takes advantage of DSQL's burst capacity:
//   - Sustained rate: 100 connections/second
//   - Burst capacity: 1,000 connections
//
// The bucket refills at 100 tokens/sec and can hold up to 1,000 tokens.
// This allows fast initial fill using burst, then settles to 100/sec sustained.
//
// Schema (single item per endpoint):
//
//	pk = dsql_connect_bucket#<endpoint>
//	tokens_milli: int64      // Current tokens × 1000
//	last_refill_ms: int64    // Last refill timestamp (Unix millis)
//	rate_milli: int64        // 100_000 (100 tokens/sec × 1000)
//	capacity_milli: int64    // 1_000_000 (1000 tokens × 1000)
//	ttl_epoch: int64         // For cleanup if endpoint unused
//
// Implements driver.RateLimiter interface.
type TokenBucketLimiter struct {
	ddb       *dynamodb.Client
	tableName string
	endpoint  string
	logger    log.Logger

	// Rate is tokens per second (default: 100).
	Rate int64

	// Capacity is the maximum tokens the bucket can hold (default: 1000).
	Capacity int64

	// MaxWait caps how long Wait will wait before returning an error.
	MaxWait time.Duration

	// BackoffBase controls retry pacing when bucket is empty.
	BackoffBase time.Duration

	// TTLWindow controls how long the bucket item lives without updates.
	TTLWindow time.Duration
}

// NewTokenBucketLimiter creates a token bucket rate limiter backed by DynamoDB.
func NewTokenBucketLimiter(ddb *dynamodb.Client, tableName, endpoint string, logger log.Logger) *TokenBucketLimiter {
	rate := int64(getEnvInt(TokenBucketRateEnvVar, DefaultTokenBucketRate))
	capacity := int64(getEnvInt(TokenBucketCapacityEnvVar, DefaultTokenBucketCapacity))
	maxWait := getEnvDuration(TokenBucketMaxWaitEnvVar, DefaultTokenBucketMaxWait)

	if logger == nil {
		logger = log.NewNoopLogger()
	}

	return &TokenBucketLimiter{
		ddb:         ddb,
		tableName:   tableName,
		endpoint:    normalizeEndpoint(endpoint),
		logger:      logger,
		Rate:        rate,
		Capacity:    capacity,
		MaxWait:     maxWait,
		BackoffBase: DefaultTokenBucketBackoff,
		TTLWindow:   1 * time.Hour, // Bucket item expires after 1 hour of inactivity
	}
}

// Wait blocks until a connection token can be acquired.
// Implements driver.RateLimiter interface.
func (l *TokenBucketLimiter) Wait(ctx context.Context) error {
	if l.Rate <= 0 || l.Capacity <= 0 {
		return nil // Disabled
	}

	start := time.Now()
	deadline := start.Add(l.MaxWait)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}

	attempts := 0
	for {
		attempts++
		acquired, retryAfter, err := l.tryAcquire(ctx)
		if err != nil {
			l.logger.Warn("Token bucket acquire failed",
				tag.Error(err),
				tag.NewStringTag("endpoint", l.endpoint),
				tag.NewInt("attempts", attempts))
			return fmt.Errorf("token bucket acquire failed: %w", err)
		}
		if acquired {
			waitTime := time.Since(start)
			if attempts > 1 || waitTime > 10*time.Millisecond {
				// Only log if we had to wait or retry
				l.logger.Debug("Token bucket token acquired",
					tag.NewStringTag("endpoint", l.endpoint),
					tag.NewInt("attempts", attempts),
					tag.NewDurationTag("wait_time", waitTime))
			}
			return nil
		}

		if time.Now().After(deadline) {
			waitTime := time.Since(start)
			l.logger.Warn("Token bucket timeout acquiring token",
				tag.NewStringTag("endpoint", l.endpoint),
				tag.NewInt("attempts", attempts),
				tag.NewDurationTag("wait_time", waitTime))
			return fmt.Errorf("token bucket: timeout acquiring token (endpoint=%s, attempts=%d, waited=%s)", l.endpoint, attempts, waitTime)
		}

		// Wait before retry
		sleep := l.calculateBackoff(retryAfter, deadline)
		select {
		case <-time.After(sleep):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// tryAcquire attempts to acquire one token from the bucket.
// Returns (true, 0, nil) on success.
// Returns (false, retryAfterMs, nil) if bucket is empty.
// Returns (false, 0, err) on DynamoDB error.
//
// Uses a read-modify-write pattern with optimistic locking:
// 1. Read current bucket state (or initialize if new)
// 2. Compute refilled tokens locally
// 3. Conditional write with version check
func (l *TokenBucketLimiter) tryAcquire(ctx context.Context) (bool, int64, error) {
	pk := tokenBucketPKPrefix + l.endpoint
	nowMs := time.Now().UTC().UnixMilli()

	// Convert to milli-tokens for integer math
	rateMilli := l.Rate * milliMultiplier         // 100_000
	capacityMilli := l.Capacity * milliMultiplier // 1_000_000
	oneTokenMilli := int64(milliMultiplier)       // 1_000

	// Step 1: Read current bucket state
	getResult, err := l.ddb.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(l.tableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: pk},
		},
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return false, 0, fmt.Errorf("failed to read bucket: %w", err)
	}

	// Step 2: Compute new state
	var currentTokensMilli, lastRefillMs int64
	isNewBucket := getResult.Item == nil || len(getResult.Item) == 0

	if isNewBucket {
		// New bucket: start at capacity
		currentTokensMilli = capacityMilli
		lastRefillMs = nowMs
	} else {
		// Parse existing values
		if v, ok := getResult.Item["tokens_milli"].(*types.AttributeValueMemberN); ok {
			currentTokensMilli, _ = strconv.ParseInt(v.Value, 10, 64)
		}
		if v, ok := getResult.Item["last_refill_ms"].(*types.AttributeValueMemberN); ok {
			lastRefillMs, _ = strconv.ParseInt(v.Value, 10, 64)
		}
	}

	// Compute refill: tokens += (elapsed_ms * rate_milli) / 1000
	elapsedMs := nowMs - lastRefillMs
	if elapsedMs > 0 {
		refillMilli := (elapsedMs * rateMilli) / 1000
		currentTokensMilli += refillMilli
	}

	// Cap at capacity
	if currentTokensMilli > capacityMilli {
		currentTokensMilli = capacityMilli
	}

	// Check if we have at least 1 token
	if currentTokensMilli < oneTokenMilli {
		// Bucket empty - calculate retry hint
		retryAfterMs := l.calculateRetryHint(nowMs)
		return false, retryAfterMs, nil
	}

	// Decrement by 1 token
	newTokensMilli := currentTokensMilli - oneTokenMilli
	ttlEpoch := time.Now().UTC().Add(l.TTLWindow).Unix()

	// Step 3: Conditional write
	var updateInput *dynamodb.UpdateItemInput

	if isNewBucket {
		// For new buckets: condition on attribute_not_exists
		updateInput = &dynamodb.UpdateItemInput{
			TableName: aws.String(l.tableName),
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: pk},
			},
			UpdateExpression:    aws.String("SET tokens_milli = :tokens, last_refill_ms = :now, rate_milli = :rate, capacity_milli = :capacity, ttl_epoch = :ttl"),
			ConditionExpression: aws.String("attribute_not_exists(pk)"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":tokens":   &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", newTokensMilli)},
				":now":      &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", nowMs)},
				":rate":     &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", rateMilli)},
				":capacity": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", capacityMilli)},
				":ttl":      &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", ttlEpoch)},
			},
		}
	} else {
		// For existing buckets: condition on last_refill_ms matching what we read
		updateInput = &dynamodb.UpdateItemInput{
			TableName: aws.String(l.tableName),
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: pk},
			},
			UpdateExpression:    aws.String("SET tokens_milli = :tokens, last_refill_ms = :now, ttl_epoch = :ttl"),
			ConditionExpression: aws.String("last_refill_ms = :expected_refill"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":tokens":          &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", newTokensMilli)},
				":now":             &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", nowMs)},
				":ttl":             &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", ttlEpoch)},
				":expected_refill": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", lastRefillMs)},
			},
		}
	}

	_, err = l.ddb.UpdateItem(ctx, updateInput)
	if err != nil {
		var cfe *types.ConditionalCheckFailedException
		if errors.As(err, &cfe) {
			// Concurrent modification - retry immediately
			// Return 0 for retryAfterMs to signal immediate retry
			return false, 0, nil
		}
		return false, 0, fmt.Errorf("failed to update bucket: %w", err)
	}

	return true, 0, nil
}

// calculateRetryHint estimates when the next token will be available.
func (l *TokenBucketLimiter) calculateRetryHint(_ int64) int64 {
	// At 100 tokens/sec, one token refills every 10ms
	msPerToken := int64(1000) / l.Rate
	return msPerToken
}

// calculateBackoff determines how long to wait before retrying.
func (l *TokenBucketLimiter) calculateBackoff(retryAfterMs int64, deadline time.Time) time.Duration {
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return 0
	}

	// Use retry hint if provided, otherwise use base backoff
	backoff := l.BackoffBase
	if retryAfterMs > 0 {
		backoff = time.Duration(retryAfterMs) * time.Millisecond
	}

	// Add jitter (0-50% of backoff)
	jitter := randDuration(0, backoff/2)
	backoff += jitter

	// Cap at remaining time
	if backoff > remaining {
		backoff = remaining
	}

	return backoff
}

// IsTokenBucketEnabled checks if the token bucket rate limiter should be used.
func IsTokenBucketEnabled() bool {
	val := os.Getenv(TokenBucketEnabledEnvVar)
	if val == "" {
		return false
	}
	enabled, err := strconv.ParseBool(val)
	if err != nil {
		return false
	}
	return enabled
}
