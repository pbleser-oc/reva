// Copyright 2026 OpenCloud GmbH <mail@opencloud.eu>
// SPDX-License-Identifier: Apache-2.0

package revisions

import (
	"context"
	"slices"
	"time"

	"github.com/rs/zerolog"
)

const (
	defaultStrategy  = autoStrategy
	disabledStrategy = "disabled"
)

type chainRevisionCleanupStrategy struct {
	chain []RevisionCleanupStrategy
}

var _ RevisionCleanupStrategy = &chainRevisionCleanupStrategy{}

var _ CleanupStrategyFunc = (*chainRevisionCleanupStrategy)(nil).Cleanup

func newChainRevisionCleanupStrategy(chain []RevisionCleanupStrategy) (RevisionCleanupStrategy, error) {
	return &chainRevisionCleanupStrategy{
		chain: chain,
	}, nil
}

func (s *chainRevisionCleanupStrategy) Cleanup(now time.Time, revs []Revision, ctx context.Context, logger *zerolog.Logger) (Plan, error) {
	masterPlan := NewSimplePlan()
	remaining := slices.Clone(revs)
	for _, strategy := range s.chain {
		plan, err := strategy.Cleanup(now, remaining, ctx, logger)
		if err != nil {
			return nil, err
		}

		masterPlan.Delete(plan.Deleted()...)
		masterPlan.Safeguard(plan.Safeguarded()...)
		remaining = slices.Clone(plan.Retained())
	}
	masterPlan.Retain(remaining...)
	return masterPlan, nil
}
