// Copyright 2026 OpenCloud GmbH <mail@opencloud.eu>
// SPDX-License-Identifier: Apache-2.0

package decomposedfs_test

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	"github.com/stretchr/testify/mock"

	"github.com/opencloud-eu/reva/v2/pkg/storage/pkg/decomposedfs/lookup"
	"github.com/opencloud-eu/reva/v2/pkg/storage/pkg/decomposedfs/metadata/prefixes"
	"github.com/opencloud-eu/reva/v2/pkg/storage/pkg/decomposedfs/node"
	"github.com/opencloud-eu/reva/v2/pkg/storage/pkg/decomposedfs/revisioncleanup"
	"github.com/opencloud-eu/reva/v2/pkg/storage/pkg/decomposedfs/spaceidindex"
	helpers "github.com/opencloud-eu/reva/v2/pkg/storage/pkg/decomposedfs/testhelpers"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// Tests for the on-demand revision cleanup of whole spaces, folders or files, as used by
// command line tools (Decomposedfs.CleanupRevisions).
var _ = Describe("On-demand revision cleanup", func() {
	// the context of a command line tool, without a user, unlike env.Ctx
	toolCtx := context.Background()
	var (
		env       *helpers.DecomposedTestEnv
		spaceID   string
		file1     *node.Node // dir1/file1, with 2 revisions
		file2     *node.Node // dir1/subdir1/file2, with 1 revision
		deletedMu sync.Mutex
		deleted   []string // the blob ids deleted from the blobstore
		results   []revisioncleanup.Result
	)

	// addRevisions gives n revisions of the given ages, each with a blob of its own
	addRevisions := func(n *node.Node, ages ...time.Duration) {
		for i, age := range ages {
			version := time.Now().Add(-age).UTC().Format(time.RFC3339Nano)
			_, err := env.Tree.CreateRevision(env.Ctx, n, version)
			Expect(err).ToNot(HaveOccurred())
			rev := node.NewBaseNode(n.SpaceID, n.ID+node.RevisionIDDelimiter+version, env.Lookup)
			Expect(env.Lookup.MetadataBackend().Set(env.Ctx, rev, prefixes.BlobIDAttr, fmt.Appendf(nil, "%s-rev-%d", n.Name, i))).To(Succeed())
		}
	}
	revisionCount := func(n *node.Node) int {
		revs, err := env.Tree.ListRevisionsOfNode(env.Ctx, n)
		Expect(err).ToNot(HaveOccurred())
		return len(revs)
	}
	deletedBlobs := func() []string {
		deletedMu.Lock()
		defer deletedMu.Unlock()
		return append([]string{}, deleted...)
	}
	collect := func(result revisioncleanup.Result) {
		results = append(results, result)
	}
	resultOf := func(n *node.Node) *revisioncleanup.Result {
		for i := range results {
			if results[i].NodeID == n.ID {
				return &results[i]
			}
		}
		return nil
	}

	BeforeEach(func() {
		var err error
		// no strategy configured, the requests give one
		env, err = helpers.NewTestEnv(map[string]any{"revisions_cleanup_async": false})
		Expect(err).ToNot(HaveOccurred())
		spaceID = env.SpaceRootRes.SpaceId
		deleted, results = nil, nil
		env.Blobstore.On("Delete", mock.Anything).Run(func(args mock.Arguments) {
			deletedMu.Lock()
			defer deletedMu.Unlock()
			deleted = append(deleted, args.Get(0).(*node.Node).BlobID)
		}).Return(nil)

		file1, err = env.Lookup.NodeFromResource(env.Ctx, &provider.Reference{ResourceId: env.SpaceRootRes, Path: "/dir1/file1"})
		Expect(err).ToNot(HaveOccurred())
		subdir1, err := env.Lookup.NodeFromResource(env.Ctx, &provider.Reference{ResourceId: env.SpaceRootRes, Path: "/dir1/subdir1"})
		Expect(err).ToNot(HaveOccurred())
		file2, err = env.CreateTestFile("file2", "file2-blobid", subdir1.ID, subdir1.SpaceID, 10)
		Expect(err).ToNot(HaveOccurred())

		addRevisions(file1, 48*time.Hour, 72*time.Hour)
		addRevisions(file2, 48*time.Hour)
	})

	AfterEach(func() {
		if env != nil {
			env.Cleanup()
		}
	})

	It("cleans up all files of a space, recursively", func() {
		Expect(env.Fs.CleanupRevisions(toolCtx, revisioncleanup.Request{SpaceIDs: []string{spaceID}, Strategy: "0"}, collect)).To(Succeed())

		Expect(revisionCount(file1)).To(BeZero())
		Expect(revisionCount(file2)).To(BeZero())
		Expect(deletedBlobs()).To(ConsistOf("file1-rev-0", "file1-rev-1", "file2-rev-0"))
		Expect(resultOf(file1)).ToNot(BeNil())
		Expect(resultOf(file1).Revisions).To(HaveLen(2))
		Expect(resultOf(file2)).ToNot(BeNil())
		Expect(resultOf(file1).Path).To(Equal("/dir1/file1"))
		Expect(resultOf(file2).Path).To(Equal("/dir1/subdir1/file2"))
		// folders are not cleaned up themselves
		for _, result := range results {
			Expect(result.NodeID).ToNot(Equal(spaceID))
		}
	})

	It("cleans up all spaces", func() {
		Expect(env.Fs.CleanupRevisions(toolCtx, revisioncleanup.Request{All: true, Strategy: "0"}, collect)).To(Succeed())

		Expect(revisionCount(file1)).To(BeZero())
		Expect(revisionCount(file2)).To(BeZero())
	})

	Describe("all spaces", func() {
		// newSpace creates a space of the given type with a file with one revision in it
		newSpace := func(spaceType string) (string, *node.Node) {
			id, err := env.CreateTestStorageSpace(spaceType, nil)
			Expect(err).ToNot(HaveOccurred())
			f, err := env.CreateTestFile("file-in-"+spaceType, spaceType+"-blobid", id.OpaqueId, id.SpaceId, 10)
			Expect(err).ToNot(HaveOccurred())
			addRevisions(f, 48*time.Hour)
			Expect(revisionCount(f)).To(Equal(1))
			return id.SpaceId, f
		}

		It("include spaces of any type", func() {
			customID, f := newSpace("custom")
			// the "share" index lists all spaces with grants, which the test helper adds: take it
			// out, so that only its own type lists it, like an unshared space of that type
			index := spaceidindex.New(filepath.Join(env.Root, lookup.IndexesDir), "by-type")
			Expect(index.Remove("share", customID)).To(Succeed())

			Expect(env.Fs.CleanupRevisions(toolCtx, revisioncleanup.Request{All: true, Strategy: "0"}, collect)).To(Succeed())

			Expect(revisionCount(f)).To(BeZero())
			Expect(revisionCount(file1)).To(BeZero())
		})

		It("skip disabled spaces, and fail for a disabled space requested by its id", func() {
			disabledID, f := newSpace("project")
			root, err := env.Lookup.NodeFromSpaceID(env.Ctx, disabledID)
			Expect(err).ToNot(HaveOccurred())
			dtime := time.Now()
			Expect(root.SetDTime(env.Ctx, &dtime)).To(Succeed())

			Expect(env.Fs.CleanupRevisions(toolCtx, revisioncleanup.Request{All: true, Strategy: "0"}, collect)).To(Succeed())
			Expect(resultOf(root)).ToNot(BeNil())
			Expect(resultOf(root).Skipped).To(ContainSubstring("disabled"))
			Expect(revisionCount(f)).To(Equal(1))
			Expect(revisionCount(file1)).To(BeZero(), "the other spaces were cleaned up")

			err = env.Fs.CleanupRevisions(toolCtx, revisioncleanup.Request{SpaceIDs: []string{disabledID}, Strategy: "0"}, collect)
			Expect(err).To(MatchError(ContainSubstring("disabled")))
		})

		It("report a space that can't be found, and clean up the others", func() {
			index := spaceidindex.New(filepath.Join(env.Root, lookup.IndexesDir), "by-type")
			Expect(index.Add("project", "missing", "../../spaces/mi/ssing")).To(Succeed())

			Expect(env.Fs.CleanupRevisions(toolCtx, revisioncleanup.Request{All: true, Strategy: "0"}, collect)).To(Succeed())

			missing := resultOf(&node.Node{BaseNode: node.BaseNode{ID: "missing"}})
			Expect(missing).ToNot(BeNil())
			Expect(missing.Err).To(HaveOccurred())
			Expect(revisionCount(file1)).To(BeZero())
		})
	})

	It("cleans up only the given file or folder", func() {
		Expect(env.Fs.CleanupRevisions(toolCtx, revisioncleanup.Request{SpaceIDs: []string{spaceID}, Path: "/dir1/subdir1", Strategy: "0"}, collect)).To(Succeed())

		Expect(revisionCount(file1)).To(Equal(2))
		Expect(revisionCount(file2)).To(BeZero())
		Expect(resultOf(file1)).To(BeNil())
		Expect(resultOf(file2).Path).To(Equal("/dir1/subdir1/file2"), "the path within the space, not within the folder")

		results = nil
		Expect(env.Fs.CleanupRevisions(toolCtx, revisioncleanup.Request{SpaceIDs: []string{spaceID}, Path: "dir1/file1", Strategy: "0"}, collect)).To(Succeed())
		Expect(revisionCount(file1)).To(BeZero())
		Expect(results).To(HaveLen(1))
		Expect(results[0].Path).To(Equal("/dir1/file1"))
	})

	It("only reports in a dry run", func() {
		Expect(env.Fs.CleanupRevisions(toolCtx, revisioncleanup.Request{SpaceIDs: []string{spaceID}, Strategy: "0", DryRun: true}, collect)).To(Succeed())

		Expect(revisionCount(file1)).To(Equal(2))
		Expect(revisionCount(file2)).To(Equal(1))
		Expect(deletedBlobs()).To(BeEmpty())
		Expect(resultOf(file1).DryRun).To(BeTrue())
		count, _ := resultOf(file1).DeletedCount()
		Expect(count).To(Equal(2))
	})

	It("uses the strategy of the request", func() {
		// keep the newest revision of each file
		Expect(env.Fs.CleanupRevisions(toolCtx, revisioncleanup.Request{SpaceIDs: []string{spaceID}, Strategy: "1"}, collect)).To(Succeed())

		Expect(revisionCount(file1)).To(Equal(1))
		Expect(revisionCount(file2)).To(Equal(1))
		Expect(deletedBlobs()).To(ConsistOf("file1-rev-1"))
	})

	It("runs in the background with the AsyncOptions of the request, and waits for it", func() {
		async := &revisioncleanup.AsyncOptions{Enabled: true, Workers: 2, QueueSize: 1}
		Expect(env.Fs.CleanupRevisions(toolCtx, revisioncleanup.Request{SpaceIDs: []string{spaceID}, Strategy: "0", Async: async}, collect)).To(Succeed())

		// all cleanups are done when it returns
		Expect(revisionCount(file1)).To(BeZero())
		Expect(revisionCount(file2)).To(BeZero())
		Expect(resultOf(file1)).ToNot(BeNil())
		Expect(resultOf(file2)).ToNot(BeNil())
		Expect(resultOf(file1).Path).To(Equal("/dir1/file1"))
		Expect(resultOf(file2).Path).To(Equal("/dir1/subdir1/file2"))
	})

	It("reports files it skipped", func() {
		Expect(file1.SetXattrString(env.Ctx, prefixes.StatusPrefix, node.ProcessingStatus+"some-upload")).To(Succeed())

		Expect(env.Fs.CleanupRevisions(toolCtx, revisioncleanup.Request{SpaceIDs: []string{spaceID}, Strategy: "0"}, collect)).To(Succeed())

		Expect(resultOf(file1).Skipped).To(ContainSubstring("still being processed"))
		Expect(resultOf(file1).Path).To(Equal("/dir1/file1"))
		Expect(revisionCount(file1)).To(Equal(2))
		Expect(revisionCount(file2)).To(BeZero())
	})

	DescribeTable("rejects invalid requests",
		func(req revisioncleanup.Request, errorContains string) {
			if req.SpaceIDs != nil && req.SpaceIDs[0] == "the space" {
				req.SpaceIDs[0] = spaceID
			}
			err := env.Fs.CleanupRevisions(toolCtx, req, collect)
			Expect(err).To(MatchError(ContainSubstring(errorContains)))
			Expect(revisionCount(file1)).To(Equal(2))
		},
		Entry("without a scope", revisioncleanup.Request{Strategy: "0"}, "no space"),
		Entry("with all and specific spaces", revisioncleanup.Request{All: true, SpaceIDs: []string{"the space"}, Strategy: "0"}, "not both"),
		Entry("with a path but without a space", revisioncleanup.Request{All: true, Path: "dir1", Strategy: "0"}, "exactly one space"),
		Entry("with a path in two spaces", revisioncleanup.Request{SpaceIDs: []string{"the space", "other"}, Path: "dir1", Strategy: "0"}, "exactly one space"),
		Entry("with an unknown path", revisioncleanup.Request{SpaceIDs: []string{"the space"}, Path: "nope", Strategy: "0"}, "nope"),
		Entry("without any strategy", revisioncleanup.Request{SpaceIDs: []string{"the space"}}, "no revisions cleanup strategy"),
		Entry("with an invalid strategy", revisioncleanup.Request{SpaceIDs: []string{"the space"}, Strategy: "whenever"}, "invalid revisions cleanup strategy"),
		Entry("with invalid async options", revisioncleanup.Request{SpaceIDs: []string{"the space"}, Strategy: "0",
			Async: &revisioncleanup.AsyncOptions{Enabled: true, QueueFull: "wait"}}, "queue full policy"),
	)
})
