// Copyright 2026 OpenCloud GmbH <mail@opencloud.eu>
// SPDX-License-Identifier: Apache-2.0

package revisions

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestRevisionComparator(t *testing.T) {
	require := require.New(t)

	a := newPlainRevision("a", rfc3339(t, "2026-09-22T16:49:00.000Z"), 300)
	b := newPlainRevision("b", rfc3339(t, "2026-09-22T16:50:00.000Z"), 200)
	c := newPlainRevision("c", rfc3339(t, "2026-09-22T16:51:00.000Z"), 100)

	require.Positive(CompareNewestFirst(a, b))
	require.Positive(CompareNewestFirst(b, c))
	require.Positive(CompareNewestFirst(a, c))

	require.Zero(CompareNewestFirst(a, a))
	require.Zero(CompareNewestFirst(b, b))
	require.Zero(CompareNewestFirst(c, c))

	require.Negative(CompareNewestFirst(b, a))
	require.Negative(CompareNewestFirst(c, b))
	require.Negative(CompareNewestFirst(c, a))
}

func TestSort(t *testing.T) {
	require := require.New(t)

	a := newPlainRevision("a", rfc3339(t, "2026-09-22T16:51:00.000Z"), 100)
	b := newPlainRevision("b", rfc3339(t, "2026-09-22T16:50:00.000Z"), 200)
	c := newPlainRevision("c", rfc3339(t, "2026-09-22T16:49:00.000Z"), 300)

	perm3 := func(a, b, c Revision) [][]Revision {
		return [][]Revision{
			{a, b, c},
			{a, c, b},
			{b, a, c},
			{b, c, a},
			{c, a, b},
			{c, b, a},
		}
	}

	for i, perm := range perm3(a, b, c) {
		names := make([]string, len(perm))
		for i, value := range perm {
			names[i] = value.Key()
		}
		name := "[" + strings.Join(names, "|") + "]"
		t.Run(fmt.Sprintf("Permutation_%d:%s", i, name), func(t *testing.T) {
			s := slices.Clone(perm)
			SortRevisionsCanonically(s)
			require.Equal([]Revision{a, b, c}, s)
		})
	}

	{
		s := []Revision{a, a, a}
		SortRevisionsCanonically(s)
		require.Equal([]Revision{a, a, a}, s)
	}
	{
		s := []Revision{a, a}
		SortRevisionsCanonically(s)
		require.Equal([]Revision{a, a}, s)
	}
	{
		s := []Revision{a}
		SortRevisionsCanonically(s)
		require.Equal([]Revision{a}, s)
	}
	{
		s := []Revision{}
		SortRevisionsCanonically(s)
		require.Empty(s)
	}
}

func TestExecuteCleanupStrategy(t *testing.T) {
	require := require.New(t)

	logger := zerolog.New(os.Stdout)
	ctx := context.TODO()
	now := rfc3339(t, "2026-09-22T17:00:00.000Z")
	a := newPlainRevision("a", rfc3339(t, "2026-09-22T16:49:00.000Z"), 200)
	b := newPlainRevision("b", rfc3339(t, "2026-09-22T16:50:00.000Z"), 300)
	c := newPlainRevision("c", rfc3339(t, "2026-09-22T16:51:00.000Z"), 100)
	revs := []Revision{a, b, c}

	var strategy CleanupStrategyFunc = func(now time.Time, revs []Revision, ctx context.Context, logger *zerolog.Logger) (Plan, error) {
		plan := NewSimplePlan()
		for _, rev := range revs {
			if rev.Size() > 150 {
				plan.Delete(rev)
			} else {
				plan.Retain(rev)
			}
		}
		return plan, nil
	}

	deleted := []Revision{}
	err := ExecuteCleanupStrategy(now, strategy, revs, ctx, &logger, func(rev Revision) error {
		deleted = append(deleted, rev)
		return nil
	})
	require.NoError(err)
	require.Equal([]Revision{b, a}, deleted)
}

func TestParseDuration(t *testing.T) {
	for _, tt := range []struct {
		input    string
		expected time.Duration
	}{
		{"6w", time.Duration(6) * 24 * 7 * time.Hour},
		{"5d", time.Duration(5) * 24 * time.Hour},
		{"4h", time.Duration(4) * time.Hour},
		{"3m", time.Duration(3) * time.Minute},
		{"2s", time.Duration(2) * time.Second},
	} {
		t.Run(fmt.Sprintf("%s: %s", t.Name(), tt.input), func(t *testing.T) {
			require := require.New(t)
			result, err := parseDuration(tt.input)
			require.NoError(err)
			require.Equal(tt.expected, result)
		})
	}

	{
		_, err := parseDuration("")
		require.Error(t, err)
	}

	{
		_, err := parseDuration("123")
		require.Error(t, err)
	}
}
