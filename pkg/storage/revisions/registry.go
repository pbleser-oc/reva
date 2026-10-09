// Copyright 2026 OpenCloud GmbH <mail@opencloud.eu>
// SPDX-License-Identifier: Apache-2.0

package revisions

import (
	"fmt"
	"regexp"
	"strings"
)

var (
	configPatternAutoOnly            = regexp.MustCompile(`^(auto)$`)
	configPatternSafeguardThenAuto   = regexp.MustCompile(`^(\d+[wdhms]?)\s*,\s*(auto)$`)
	configPatternAutoThenDelete      = regexp.MustCompile(`^(auto)\s*,\s*(\d+[wdhms]?)$`)
	configPatternSafeguardThenDelete = regexp.MustCompile(`^(\d+[wdhms]?)\s*,\s*(\d+[wdhms]?)$`)
	configPatternDisabled            = regexp.MustCompile(`^disabled$`)
)

// This is the entry point for instantiating a strategy that may be composed of multiple ones.
//
// The config parameter is a string that denotes which strategy/ies to use, as a comma separated
// list of specifications.
//
// Examples:
//   - "auto": Default, default clean-up pattern applies
//   - "D, auto": Keeps versions at least for D days, apply default clean-up pattern to all versions that are older than D days
//   - "auto, D": Delete all versions that are older than D days automatically, delete other versions according to expiration rules
//   - "D1, D2": Keep versions for at least D1 days and delete when they exceed D2 days.
//   - "disabled": Disable version retention; no files will be deleted.
func NewRevisionCleanupStrategy(config string) (RevisionCleanupStrategy, error) {
	config = strings.TrimSpace(config)

	// "auto": Default, default clean-up pattern applies
	if m := configPatternAutoOnly.FindStringSubmatch(config); len(m) > 0 {
		auto := m[1]
		return newAutoRevisionCleanupStrategyFromString(auto)
	}

	// "D, auto": Keeps versions at least for D days, apply default clean-up pattern to all versions that are older than D days
	if m := configPatternSafeguardThenAuto.FindStringSubmatch(config); len(m) > 0 {
		chain := make([]RevisionCleanupStrategy, 2)
		// keep versions, then cleanup
		retainYoungerThan := m[1]
		auto := m[2]

		if strategy, err := parseKeep(retainYoungerThan); err != nil {
			return nil, err
		} else {
			chain[0] = strategy
		}
		if strategy, err := parseAuto(auto); err != nil {
			return nil, err
		} else {
			chain[1] = strategy
		}
		return newChainRevisionCleanupStrategy(chain)
	}

	// "auto, D": Delete all versions that are older than D days automatically, delete other versions according to expiration rules
	if m := configPatternAutoThenDelete.FindStringSubmatch(config); len(m) > 0 {
		chain := make([]RevisionCleanupStrategy, 2)
		auto := m[1]
		delete := m[2]

		if strategy, err := parseDelete(delete); err != nil {
			return nil, err
		} else {
			chain[0] = strategy
		}
		if strategy, err := parseAuto(auto); err != nil {
			return nil, err
		} else {
			chain[1] = strategy
		}
		return newChainRevisionCleanupStrategy(chain)
	}

	// "D1, D2": Keep versions for at least D1 days and delete when they exceed D2 days.
	if m := configPatternSafeguardThenDelete.FindStringSubmatch(config); len(m) > 0 {
		chain := make([]RevisionCleanupStrategy, 2)
		keep := m[1]
		delete := m[2]

		if strategy, err := parseKeep(keep); err != nil {
			return nil, err
		} else {
			chain[0] = strategy
		}
		if strategy, err := parseDelete(delete); err != nil {
			return nil, err
		} else {
			chain[1] = strategy
		}
		return newChainRevisionCleanupStrategy(chain)
	}

	// "disabled": Disable version retention; no files will be deleted.
	if configPatternDisabled.MatchString(config) {
		return newDisabledRevisionCleanupStrategy()
	}

	return nil, fmt.Errorf("unsupported revision cleanup strategy specification %q", config)
}

func parseKeep(retain string) (RevisionCleanupStrategy, error) {
	strategy, err := newDurationRetentionRevisionCleanupStrategyFromString(retain)
	if err != nil {
		return nil, err
	}
	if strategy != nil {
		return strategy, nil
	}
	strategy, err = newCountRetentionRevisionCleanupStrategyFromString(retain)
	if err != nil {
		return nil, err
	}
	if strategy != nil {
		return strategy, nil
	}
	// doesn't match either
	return nil, fmt.Errorf("unsupported revision safeguarding specification %q", retain)
}

func parseDelete(delete string) (RevisionCleanupStrategy, error) {
	strategy, err := newDurationDeletionRevisionCleanupStrategyFromString(delete)
	if err != nil {
		return nil, err
	}
	if strategy != nil {
		return strategy, nil
	}
	strategy, err = newCountDeletionRevisionCleanupStrategyFromString(delete)
	if err != nil {
		return nil, err
	}
	if strategy != nil {
		return strategy, nil
	}
	// doesn't match either
	return nil, fmt.Errorf("unsupported revision deletion specification %q", delete)
}

func parseAuto(auto string) (RevisionCleanupStrategy, error) {
	if strategy, err := newAutoRevisionCleanupStrategyFromString(auto); err != nil {
		return nil, err
	} else if strategy != nil {
		return strategy, nil
	} else {
		return nil, fmt.Errorf("unsupported revision 'auto' specification %q", auto)
	}
}
