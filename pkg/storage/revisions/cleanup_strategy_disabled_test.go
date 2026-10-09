// Copyright 2026 OpenCloud GmbH <mail@opencloud.eu>
// SPDX-License-Identifier: Apache-2.0

package revisions

import (
	"context"
	"os"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestNewDisabledRevisionCleanupStrategyFromString(t *testing.T) {
	now := rfc3339(t, "2026-09-28T15:00:00.000Z")
	revs := []Revision{
		newPlainRevision("a", rfc3339(t, "2026-09-28T14:00:00.000Z"), 1), // -1h
		newPlainRevision("b", rfc3339(t, "2026-09-28T00:00:01.000Z"), 2), // same day, midnight
		newPlainRevision("c", rfc3339(t, "2026-09-27T14:00:00.000Z"), 1), // -1d1h
		newPlainRevision("d", rfc3339(t, "2026-09-26T14:00:00.000Z"), 1), // -2d1h
		newPlainRevision("e", rfc3339(t, "2026-09-25T14:00:00.000Z"), 1), // -3d1h
	}

	require := require.New(t)

	strategy, err := newDisabledRevisionCleanupStrategy()
	require.NoError(err)

	logger := zerolog.New(os.Stdout)

	plan, err := strategy.Cleanup(now, revs, context.TODO(), &logger)
	require.NoError(err)
	require.Empty(plan.Deleted())           // disabled should never delete anything
	require.Empty(plan.Retained())          // it shouldn't retain anything either, in case it is used in combination with another strategy
	require.Equal(revs, plan.Safeguarded()) // it should safeguard all the revisions
}
