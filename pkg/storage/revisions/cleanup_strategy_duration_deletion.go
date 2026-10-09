// Copyright 2026 OpenCloud GmbH <mail@opencloud.eu>
// SPDX-License-Identifier: Apache-2.0

package revisions

import (
	"context"
	"time"

	"github.com/rs/zerolog"
)

// A RevisionCleanupStrategy that deletes files that are older than a given duration.
//
// TODO document midnight truncation
type durationDeletionRevisionCleanupStrategy struct {
	retention          time.Duration
	truncateToMidnight bool
}

var _ RevisionCleanupStrategy = &durationDeletionRevisionCleanupStrategy{}

var _ CleanupStrategyFunc = (*durationDeletionRevisionCleanupStrategy)(nil).Cleanup

func newDurationDeletionRevisionCleanupStrategy(retention time.Duration, truncateToMidnight bool) (RevisionCleanupStrategy, error) {
	return &durationDeletionRevisionCleanupStrategy{
		retention:          retention,
		truncateToMidnight: truncateToMidnight,
	}, nil
}

func newDurationDeletionRevisionCleanupStrategyFromString(config string) (RevisionCleanupStrategy, error) {
	if matches, duration, truncateToMidnight, err := durationFromConfig(config); !matches {
		return nil, nil
	} else if err != nil {
		return nil, err
	} else {
		return newDurationDeletionRevisionCleanupStrategy(duration, truncateToMidnight)
	}
}

func (s *durationDeletionRevisionCleanupStrategy) Cleanup(now time.Time, revs []Revision, ctx context.Context, logger *zerolog.Logger) (Plan, error) {
	cutoff := computeCutoffFromRetention(now, -s.retention, s.truncateToMidnight)
	plan := NewSimplePlan()
	for _, rev := range revs {
		if rev.LastModificationTime().Before(cutoff) {
			plan.Delete(rev)
		} else {
			plan.Retain(rev)
		}
	}
	return plan, nil
}
