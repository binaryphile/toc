package natstransport

import (
	"errors"
	"fmt"
	"strings"
)

// ValidatePipelineID returns an error if id is not a valid single NATS
// subject token. It rejects empty strings, NATS-reserved characters
// (`.`, `*`, `>`), and whitespace.
func ValidatePipelineID(id string) error {
	if id == "" {
		return errors.New("pipeline ID must not be empty")
	}
	if strings.ContainsAny(id, ".*> \t\n\r") {
		return fmt.Errorf("pipeline ID %q contains invalid characters", id)
	}
	return nil
}

// validatePrefix returns an error if prefix is not a valid NATS subject
// prefix. Multi-token prefixes (e.g. "toc.dev") are allowed. Each token
// must be non-empty and must not contain NATS-reserved characters.
func validatePrefix(prefix string) error {
	if prefix == "" {
		return errors.New("prefix must not be empty")
	}
	if strings.HasPrefix(prefix, ".") || strings.HasSuffix(prefix, ".") {
		return fmt.Errorf("prefix %q must not start or end with '.'", prefix)
	}
	if strings.Contains(prefix, "..") {
		return fmt.Errorf("prefix %q contains empty token", prefix)
	}
	if strings.ContainsAny(prefix, "*> \t\n\r") {
		return fmt.Errorf("prefix %q contains invalid characters", prefix)
	}
	return nil
}
