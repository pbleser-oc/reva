// Copyright 2026 OpenCloud GmbH <mail@opencloud.eu>
// SPDX-License-Identifier: Apache-2.0

package revisions

import (
	"context"
	"time"

	"github.com/rs/zerolog"
)

// A RevisionCleanupStrategy that does nothing.
type disabledRevisionCleanupStrategy struct {
}

var _ RevisionCleanupStrategy = &disabledRevisionCleanupStrategy{}

var _ CleanupStrategyFunc = (*disabledRevisionCleanupStrategy)(nil).Cleanup

func newDisabledRevisionCleanupStrategy() (RevisionCleanupStrategy, error) {
	// Note that we don't register this one in the registry with an init function as it is handled
	// by the configuration string parsing in chainRevisionCleanupStrategy.
	return &disabledRevisionCleanupStrategy{}, nil
}

func (s *disabledRevisionCleanupStrategy) Cleanup(now time.Time, revs []Revision, ctx context.Context, logger *zerolog.Logger) (Plan, error) {
	plan := NewSimplePlan()
	plan.Safeguard(revs...)
	return plan, nil
}
