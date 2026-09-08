package domain

import "errors"

var (
	ErrAlreadyExists      = errors.New("already exists")
	ErrUUIDInvalid        = errors.New("invalid UUID format")
	ErrTimeInvalid        = errors.New("created_at is required and created_at cannot be in the future")
	ErrNameInvalid        = errors.New("name is required, min=3, max=64")
	ErrAgeInvalid         = errors.New("age is required, min=18, max=120")
	ErrEmailInvalid       = errors.New("email is required in  valid format")
	ErrKafkaConnection    = errors.New("kafka connection error")
	ErrRedisConnection    = errors.New("redis connection error")
	ErrPostgresConnection = errors.New("postgres connection error")
	ErrNilKeyRedis        = errors.New("key not found in cache")
)
