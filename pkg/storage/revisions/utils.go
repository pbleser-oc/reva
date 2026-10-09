// Copyright 2026 OpenCloud GmbH <mail@opencloud.eu>
// SPDX-License-Identifier: Apache-2.0

package revisions

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/rs/zerolog"
)

// Compares two Revisions, newest first.
func CompareNewestFirst[T Revision](a, b T) int {
	return b.LastModificationTime().Compare(a.LastModificationTime())
}

// Sorts a slice of Revisions canonically: newest first.
func SortRevisionsCanonically[T Revision](revisions []T) {
	slices.SortFunc(revisions, CompareNewestFirst)
}

// A function that implements a strategy for removing revisions.
type CleanupStrategyFunc func(now time.Time, revs []Revision, ctx context.Context, logger *zerolog.Logger) (Plan, error)

// Takes a CleanupStrategyFunc and acts as a glue to piece everything together,
// except the deletion of Revisions itself, that it delegates to the cleanup function
// that is passed as a parameter.
//
// Note that this implementation is assuming that it is used in a single-threaded
// fashion as it does not perform any locking of anything.
func ExecuteCleanupStrategy(now time.Time, strategy CleanupStrategyFunc, revs []Revision, ctx context.Context, logger *zerolog.Logger,
	cleanup func(rev Revision) error,
) error {
	revs = slices.Clone(revs)
	SortRevisionsCanonically(revs)

	plan, err := strategy(now, revs, ctx, logger)
	if err != nil {
		return err
	}

	deleted := plan.Deleted()

	errs := make([]error, len(deleted))
	for i, rev := range deleted {
		err := cleanup(rev)
		errs[i] = err
	}
	return errors.Join(errs...)
}

var (
	durationParserRegex = regexp.MustCompile(`^(\d+)([wdhms])$`)
)

func parseDuration(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	m := durationParserRegex.FindStringSubmatch(s)
	if len(m) < 1 {
		return 0, fmt.Errorf("invalid duration number: %q", s)
	}
	value := m[1]
	unit := m[2]

	n, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("invalid duration number: %w", err)
	}

	switch unit {
	case "w":
		return time.Duration(n) * 7 * 24 * time.Hour, nil
	case "d":
		return time.Duration(n) * 24 * time.Hour, nil
	case "h":
		return time.Duration(n) * time.Hour, nil
	case "m":
		return time.Duration(n) * time.Minute, nil
	case "s":
		return time.Duration(n) * time.Second, nil
	default:
		return 0, fmt.Errorf("unknown duration unit: %q", unit)
	}
}

var (
	durationSpec = regexp.MustCompile(`^\d+[wdhms]=?$`)
)

func durationFromConfig(config string) (bool, time.Duration, bool, error) {
	config = strings.TrimSpace(config)
	if !durationSpec.MatchString(config) {
		return false, 0, false, nil
	}

	truncateToMidnight := false
	if strings.HasSuffix(config, "=") {
		_, size := utf8.DecodeLastRuneInString(config)
		config = config[:len(config)-size]
		truncateToMidnight = true
	}

	duration, err := parseDuration(config)
	if err != nil {
		return true, 0, false, err
	}

	return true, duration, truncateToMidnight, nil
}

func computeCutoffFromRetention(now time.Time, d time.Duration, truncateToMidnight bool) time.Time {
	var cutoff time.Time
	if truncateToMidnight {
		year, month, day := now.Date()
		midnight := time.Date(year, month, day, 0, 0, 0, 0, now.Location())
		cutoff = midnight.Add(d)
	} else {
		cutoff = now.Add(d)
	}
	return cutoff
}

var (
	countConfigFormat = regexp.MustCompile(`^(\d+)$`)
)

func countFromConfig(config string) (bool, int, error) {
	if m := countConfigFormat.FindStringSubmatch(config); len(m) > 1 {
		if maxVersionsPerFile, err := strconv.Atoi(m[1]); err != nil {
			return true, 0, err
		} else if maxVersionsPerFile < 0 {
			return true, 0, fmt.Errorf("invalid value for maxVersionsPerFile: %q", m[1])
		} else {
			return true, maxVersionsPerFile, nil
		}
	} else {
		return false, 0, nil
	}
}
