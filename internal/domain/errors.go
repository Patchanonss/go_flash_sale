package domain

import "errors"

var (
	ErrOutOfStock         = errors.New("item is out of stock")
	ErrItemNotFound       = errors.New("item not found")
	ErrInvalidQuantity    = errors.New("quantity must be greater than zero")
	ErrOrderNotFound      = errors.New("order not found")
	ErrDuplicateOrder     = errors.New("duplicate order request (idempotency key conflict)")
	ErrRateLimitExceeded  = errors.New("rate limit exceeded, please slow down")
	ErrInvalidRequest     = errors.New("invalid request payload")
)
