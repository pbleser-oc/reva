// Copyright 2026 OpenCloud GmbH <mail@opencloud.eu>
// SPDX-License-Identifier: Apache-2.0

package revisions

import (
	"context"
	"time"

	"github.com/rs/zerolog"
)

type countDeletionRevisionCleanupStrategy struct {
	maxVersionsPerFile int
}

var _ RevisionCleanupStrategy = &countDeletionRevisionCleanupStrategy{}

var _ CleanupStrategyFunc = (*countDeletionRevisionCleanupStrategy)(nil).Cleanup

func newCountDeletionRevisionCleanupStrategy(maxVersionsPerFile int) (RevisionCleanupStrategy, error) {
	return &countDeletionRevisionCleanupStrategy{
		maxVersionsPerFile: maxVersionsPerFile,
	}, nil
}

func newCountDeletionRevisionCleanupStrategyFromString(config string) (RevisionCleanupStrategy, error) {
	if match, maxVersionsPerFile, err := countFromConfig(config); !match {
		return nil, nil
	} else if err != nil {
		return nil, err
	} else {
		return newCountDeletionRevisionCleanupStrategy(maxVersionsPerFile)
	}
}

func (s *countDeletionRevisionCleanupStrategy) Cleanup(now time.Time, revs []Revision, ctx context.Context, logger *zerolog.Logger) (Plan, error) {
	plan := NewSimplePlan()
	if len(revs) <= s.maxVersionsPerFile {
		plan.Retain(revs...)
		return plan, nil
	}
	// no need to sort revisions, they are guaranteed to be sorted from newest to oldest
	plan.Delete(revs[s.maxVersionsPerFile:]...)
	plan.Retain(revs[:s.maxVersionsPerFile]...)
	return plan, nil
}
