// Copyright 2026 OpenCloud GmbH <mail@opencloud.eu>
// SPDX-License-Identifier: Apache-2.0

package revisions

import (
	"context"
	"time"

	"github.com/rs/zerolog"
)

// A RevisionCleanupStrategy that retains files that are older than a given duration.
//
// TODO document midnight truncation
type durationRetentionRevisionCleanupStrategy struct {
	retention          time.Duration
	truncateToMidnight bool
}

var _ RevisionCleanupStrategy = &durationRetentionRevisionCleanupStrategy{}

var _ CleanupStrategyFunc = (*durationRetentionRevisionCleanupStrategy)(nil).Cleanup

func newDurationRetentionRevisionCleanupStrategy(retention time.Duration, truncateToMidnight bool) (RevisionCleanupStrategy, error) {
	return &durationRetentionRevisionCleanupStrategy{
		retention:          retention,
		truncateToMidnight: truncateToMidnight,
	}, nil
}

func newDurationRetentionRevisionCleanupStrategyFromString(config string) (RevisionCleanupStrategy, error) {
	if matches, duration, truncateToMidnight, err := durationFromConfig(config); !matches {
		return nil, nil
	} else if err != nil {
		return nil, err
	} else {
		return newDurationRetentionRevisionCleanupStrategy(duration, truncateToMidnight)
	}
}

func (s *durationRetentionRevisionCleanupStrategy) Cleanup(now time.Time, revs []Revision, ctx context.Context, logger *zerolog.Logger) (Plan, error) {
	cutoff := computeCutoffFromRetention(now, -s.retention, s.truncateToMidnight)
	plan := NewSimplePlan()
	for _, rev := range revs {
		if rev.LastModificationTime().After(cutoff) {
			// revision is younger than the cutoff time:
			// do nothing in order to drop the revision from the revisions that
			// need to be processed by following-up strategies
			plan.Safeguard(rev)
		} else {
			// revision is older than the cutoff time:
			// mark it for retention, in order to be processed by any
			// strategy that comes next
			plan.Retain(rev)
		}
	}
	return plan, nil
}
