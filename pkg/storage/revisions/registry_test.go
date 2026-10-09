// Copyright 2026 OpenCloud GmbH <mail@opencloud.eu>
// SPDX-License-Identifier: Apache-2.0

package revisions

import (
	"context"
	"os"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rs/zerolog"
)

var _ = Describe("RevisionCleanupStrategy", func() {
	var (
		now      time.Time
		revs     []Revision
		logger   *zerolog.Logger
		strategy RevisionCleanupStrategy
	)

	BeforeEach(func() {
		rfc3339 := func(s string) time.Time {
			result, err := time.Parse(time.RFC3339, s)
			Expect(err).NotTo(HaveOccurred())
			return result
		}

		zl := zerolog.New(os.Stdout)
		logger = &zl

		now = rfc3339("2026-09-28T15:00:00.000Z")

		revs = []Revision{
			// three revisions that are older than an hour, but not older than a day
			newPlainRevision("a1", rfc3339("2026-09-28T13:59:00.000Z"), 1), // -1h
			newPlainRevision("a2", rfc3339("2026-09-28T13:58:00.000Z"), 1), // -1h
			newPlainRevision("a3", rfc3339("2026-09-28T13:57:00.000Z"), 1), // -1h
			// three revisions that are older than a day, but not older than two days
			newPlainRevision("b1", rfc3339("2026-09-27T14:00:00.000Z"), 1), // -1d1h
			newPlainRevision("b2", rfc3339("2026-09-27T13:59:00.000Z"), 1), // -1d1h
			newPlainRevision("b3", rfc3339("2026-09-27T13:58:00.000Z"), 1), // -1d1h
			// three revisions that are older than two days, but not older than three days
			newPlainRevision("c1", rfc3339("2026-09-26T14:00:00.000Z"), 1), // -2d1h
			newPlainRevision("c2", rfc3339("2026-09-26T13:59:00.000Z"), 1), // -2d1h
			newPlainRevision("c3", rfc3339("2026-09-26T13:58:00.000Z"), 1), // -2d1h
			// three revisions that are older than three days
			newPlainRevision("d1", rfc3339("2026-09-25T14:00:00.000Z"), 1), // -3d1h
			newPlainRevision("d2", rfc3339("2026-09-25T13:59:00.000Z"), 1), // -3d1h
			newPlainRevision("d3", rfc3339("2026-09-25T13:58:00.000Z"), 1), // -3d1h
		}
	})

	Context("using 'auto'", func() {
		JustBeforeEach(func() {
			var err error
			strategy, err = NewRevisionCleanupStrategy("auto")
			Expect(err).NotTo(HaveOccurred())
			Expect(strategy).NotTo(BeNil())
		})
		Describe("the strategy", func() {
			var plan Plan
			It("does not produce an error", func() {
				var err error
				plan, err = strategy.Cleanup(now, revs, context.TODO(), logger)
				Expect(err).NotTo(HaveOccurred())
			})

			It("deletes all but the newest revision in each bucket", func() {
				Expect(keysOf(plan.Deleted())).To(ConsistOf(
					"a2", "a3", // keeping the youngest in the 1h bucket (a1)
					"b2", "b3", // keeping the youngest in the 1d bucket (b1)
					"c2", "c3", // keeping the youngest in the 2d bucket (c1)
					"d2", "d3", // keeping the youngest in the 3d bucket (d1)
				))
			})
			It("retains the newest revision in each bucket", func() {
				Expect(keysOf(plan.Retained())).To(ConsistOf(
					"a1", "b1", "c1", "d1",
				))
			})
			It("does not safeguard any revisions", func() {
				Expect(plan.Safeguarded()).To(BeEmpty())
			})
		})
	})

	Context("using '2d, auto'", func() {
		JustBeforeEach(func() {
			var err error
			strategy, err = NewRevisionCleanupStrategy("2d, auto")
			Expect(err).NotTo(HaveOccurred())
		})
		Describe("the strategy", func() {
			var plan Plan
			It("does not produce an error", func() {
				var err error
				plan, err = strategy.Cleanup(now, revs, context.TODO(), logger)
				Expect(err).NotTo(HaveOccurred())
			})
			It("safeguards the revisions that are up to 2 days old", func() {
				Expect(keysOf(plan.Safeguarded())).To(ConsistOf(
					"a1", "a2", "a3", "b1", "b2", "b3",
				))
			})
			It("retains the newest revision in each bucket", func() {
				Expect(keysOf(plan.Retained())).To(ConsistOf(
					"c1", "d1",
				))
			})
			It("deletes the revisions that are older than the newest in each bucket", func() {
				Expect(keysOf(plan.Deleted())).To(ConsistOf(
					"c2", "c3", // keeping the youngest in the 2d bucket (c1)
					"d2", "d3", // keeping the youngest in the 3d bucket (d1)
				))
			})
		})
	})

	Context("using 'auto, 1d'", func() {
		JustBeforeEach(func() {
			var err error
			strategy, err = NewRevisionCleanupStrategy("auto, 1d")
			Expect(err).NotTo(HaveOccurred())
		})
		Describe("the strategy", func() {
			// should delete:
			// b1, b2, b3: older than 1d
			// c1, c2, c3: older than 1d
			// d1, d2, d3: older than 1d
			// a2, a3: keep only a1 as the newest in the 1h bucket

			var plan Plan
			It("does not produce an error", func() {
				var err error
				plan, err = strategy.Cleanup(now, revs, context.TODO(), logger)
				Expect(err).NotTo(HaveOccurred())
			})
			It("does not safeguard any revision", func() {
				Expect(plan.Safeguarded()).To(BeEmpty())
			})
			It("retains the newest revision in the 1h bucket", func() {
				Expect(keysOf(plan.Retained())).To(ConsistOf(
					"a1",
				))
			})
			It("deletes the revisions that are older than 1d and older than the newest in each bucket", func() {
				Expect(keysOf(plan.Deleted())).To(ConsistOf(
					"b1", "b2", "b3", "c1", "c2", "c3", "d1", "d2", "d3", // older then 1d
					"a2", "a3", // keeping the youngest in the 1h bucket (a1)
				))
			})
		})
	})

	Context("using 'auto, 2d'", func() {
		JustBeforeEach(func() {
			var err error
			strategy, err = NewRevisionCleanupStrategy("auto, 2d")
			Expect(err).NotTo(HaveOccurred())
		})
		Describe("the strategy", func() {
			// should delete:
			// c1, c2, c3: older than 2d
			// d1, d2, d3: older than 2d
			// a2, a3: keep only a1 as the newest in the 1h bucket
			// b2, b3: keep only b1 as the newest in the 1d bucket

			var plan Plan
			It("does not produce an error", func() {
				var err error
				plan, err = strategy.Cleanup(now, revs, context.TODO(), logger)
				Expect(err).NotTo(HaveOccurred())
			})
			It("does not safeguard any revision", func() {
				Expect(plan.Safeguarded()).To(BeEmpty())
			})
			It("retains the newest revision in the 1h and in the 1d buckets", func() {
				Expect(keysOf(plan.Retained())).To(ConsistOf(
					"a1",
					"b1",
				))
			})
			It("deletes the revisions that are older than 2d and older than the newest in each bucket", func() {
				Expect(keysOf(plan.Deleted())).To(ConsistOf(
					"c1", "c2", "c3", "d1", "d2", "d3", // older then 1d
					"a2", "a3", // keeping the youngest in the 1h bucket (a1)
					"b2", "b3", // keeping the youngest in the 1d bucket (b1)
				))
			})
		})
	})

	Context("using '1d, 2d'", func() {
		JustBeforeEach(func() {
			var err error
			strategy, err = NewRevisionCleanupStrategy("1d, 2d")
			Expect(err).NotTo(HaveOccurred())
		})
		Describe("the strategy", func() {
			// should delete:
			// c1, c2, c3: older than 2d
			// d1, d2, d3: older than 2d

			var plan Plan
			It("does not produce an error", func() {
				var err error
				plan, err = strategy.Cleanup(now, revs, context.TODO(), logger)
				Expect(err).NotTo(HaveOccurred())
			})

			It("safeguards revisions that are up to 1 day old", func() {
				Expect(keysOf(plan.Safeguarded())).To(ConsistOf(
					"a1", "a2", "a3",
				))
			})
			It("retains the revisions that are less than 2 days old", func() {
				Expect(keysOf(plan.Retained())).To(ConsistOf(
					"b1", "b2", "b3",
				))
			})
			It("deletes the revisions that are older than 2 days", func() {
				Expect(keysOf(plan.Deleted())).To(ConsistOf(
					"c1", "c2", "c3", // older than 2d
					"d1", "d2", "d3", // older than 2d
				))
			})
		})
	})

	Context("using 'disabled'", func() {
		JustBeforeEach(func() {
			var err error
			strategy, err = NewRevisionCleanupStrategy("disabled")
			Expect(err).NotTo(HaveOccurred())
		})
		Describe("the strategy", func() {
			// should delete: none.

			var plan Plan
			It("does not produce an error", func() {
				var err error
				plan, err = strategy.Cleanup(now, revs, context.TODO(), logger)
				Expect(err).NotTo(HaveOccurred())
			})

			It("does not retain anything", func() {
				Expect(plan.Retained()).To(BeEmpty())
			})
			It("does not delete anything", func() {
				Expect(plan.Deleted()).To(BeEmpty())
			})
			It("safeguards all the revisions", func() {
				Expect(keysOf(plan.Safeguarded())).To(ConsistOf(keysOf(revs)))
			})
		})
	})
})
