package redimo

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestSecondsFromMillis verifies that millisecond epochs are truncated (not
// rounded) to whole seconds, matching Pika v3.2.2's second-only precision.
func TestSecondsFromMillis(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		ms   int64
		want int64
	}{
		{"exact second", 1_700_000_000_000, 1_700_000_000},
		{"sub-second remainder truncated", 1_700_000_000_999, 1_700_000_000},
		{"just under next second", 1_700_000_000_001, 1_700_000_000},
		{"zero", 0, 0},
		{"under one second", 999, 0},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, SecondsFromMillis(tc.ms))
		})
	}
}

// TestSecondsFromTime verifies wall-clock instants are reduced to whole epoch
// seconds, discarding any sub-second component.
func TestSecondsFromTime(t *testing.T) {
	t.Parallel()

	base := time.Unix(1_700_000_000, 0).UTC()
	assert.Equal(t, int64(1_700_000_000), SecondsFromTime(base))
	assert.Equal(t, int64(1_700_000_000), SecondsFromTime(base.Add(999*time.Millisecond)))
}

// TestTTLAttributeNameMatchesMeta ensures the attribute registered for native TTL
// is exactly the meta item's exp attribute, so read-path expiry and DynamoDB
// native cleanup act on the same attribute.
func TestTTLAttributeNameMatchesMeta(t *testing.T) {
	t.Parallel()

	assert.Equal(t, metaAttrExp, TTLAttributeName)
	assert.Equal(t, "exp", TTLAttributeName)
}
