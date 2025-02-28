package manifest

import (
	"context"
	"encoding/json"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/content/index"
	"github.com/kopia/kopia/repo/encryption"
	"github.com/kopia/kopia/repo/format"
	"github.com/kopia/kopia/repo/hashing"
)

func TestMain(m *testing.M) { testutil.MyTestMain(m) }

func TestManifest(t *testing.T) {
	ctx := testlogging.Context(t)
	data := blobtesting.DataMap{}
	mgr := newManagerForTesting(ctx, t, data)

	item1 := map[string]int{"foo": 1, "bar": 2}
	item2 := map[string]int{"foo": 2, "bar": 3}
	item3 := map[string]int{"foo": 3, "bar": 4}

	labels1 := map[string]string{"type": "item", "color": "red"}
	labels2 := map[string]string{"type": "item", "color": "blue", "shape": "square"}
	labels3 := map[string]string{"type": "item", "shape": "square", "color": "red"}

	id1 := addAndVerify(ctx, t, mgr, labels1, item1)
	id2 := addAndVerify(ctx, t, mgr, labels2, item2)
	id3 := addAndVerify(ctx, t, mgr, labels3, item3)

	cases := []struct {
		criteria map[string]string
		expected []ID
	}{
		{map[string]string{"color": "red"}, []ID{id1, id3}},
		{map[string]string{"color": "blue"}, []ID{id2}},
		{map[string]string{"color": "green"}, nil},
		{map[string]string{"color": "red", "shape": "square"}, []ID{id3}},
		{map[string]string{"color": "blue", "shape": "square"}, []ID{id2}},
		{map[string]string{"color": "red", "shape": "circle"}, nil},
	}

	// verify before flush
	for _, tc := range cases {
		verifyMatches(ctx, t, mgr, tc.criteria, tc.expected)
	}

	verifyItem(ctx, t, mgr, id1, labels1, item1)
	verifyItem(ctx, t, mgr, id2, labels2, item2)
	verifyItem(ctx, t, mgr, id3, labels3, item3)

	if err := mgr.Flush(ctx); err != nil {
		t.Errorf("flush error: %v", err)
	}

	if err := mgr.Flush(ctx); err != nil {
		t.Errorf("flush error: %v", err)
	}

	// verify after flush
	for _, tc := range cases {
		verifyMatches(ctx, t, mgr, tc.criteria, tc.expected)
	}

	verifyItem(ctx, t, mgr, id1, labels1, item1)
	verifyItem(ctx, t, mgr, id2, labels2, item2)
	verifyItem(ctx, t, mgr, id3, labels3, item3)

	// flush underlying content manager and verify in new manifest manager.
	mgr.b.Flush(ctx)
	mgr2 := newManagerForTesting(ctx, t, data)

	for _, tc := range cases {
		verifyMatches(ctx, t, mgr2, tc.criteria, tc.expected)
	}

	verifyItem(ctx, t, mgr2, id1, labels1, item1)
	verifyItem(ctx, t, mgr2, id2, labels2, item2)
	verifyItem(ctx, t, mgr2, id3, labels3, item3)

	if err := mgr2.Flush(ctx); err != nil {
		t.Errorf("flush error: %v", err)
	}

	// delete from one
	time.Sleep(1 * time.Second)

	if err := mgr.Delete(ctx, id3); err != nil {
		t.Errorf("delete error: %v", err)
	}

	verifyItemNotFound(ctx, t, mgr, id3)
	mgr.Flush(ctx)
	verifyItemNotFound(ctx, t, mgr, id3)

	// still found in another
	verifyItem(ctx, t, mgr2, id3, labels3, item3)

	if err := mgr.Compact(ctx); err != nil {
		t.Errorf("can't compact: %v", err)
	}

	foundContents := 0

	if err := mgr.b.IterateContents(
		ctx,
		content.IterateOptions{Range: index.PrefixRange(ContentPrefix)},
		func(ci content.Info) error {
			foundContents++
			return nil
		}); err != nil {
		t.Errorf("unable to list manifest content: %v", err)
	}

	if got, want := foundContents, 1; got != want {
		t.Errorf("unexpected number of blocks: %v, want %v", got, want)
	}

	mgr.b.Flush(ctx)

	mgr3 := newManagerForTesting(ctx, t, data)

	verifyItem(ctx, t, mgr3, id1, labels1, item1)
	verifyItem(ctx, t, mgr3, id2, labels2, item2)
	verifyItemNotFound(ctx, t, mgr3, id3)
}

func TestManifestInitCorruptedBlock(t *testing.T) {
	ctx := testlogging.Context(t)
	data := blobtesting.DataMap{}
	st := blobtesting.NewMapStorage(data, nil, nil)

	fop, err := format.NewFormattingOptionsProvider(&format.ContentFormat{
		Hash:       hashing.DefaultAlgorithm,
		Encryption: encryption.DefaultAlgorithm,
		MutableParameters: format.MutableParameters{
			Version:     1,
			MaxPackSize: 100000,
		},
	}, nil)
	require.NoError(t, err)

	// write some data to storage
	bm, err := content.NewManagerForTesting(ctx, st, fop, nil, nil)
	require.NoError(t, err)

	bm0 := bm

	t.Cleanup(func() { bm0.Close(ctx) })

	mgr, err := NewManager(ctx, bm, ManagerOptions{})
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	mgr.Put(ctx, map[string]string{"type": "foo"}, map[string]string{"some": "value"})
	mgr.Flush(ctx)
	bm.Flush(ctx)

	// corrupt data at the storage level.
	for blobID, v := range data {
		for _, prefix := range content.PackBlobIDPrefixes {
			if strings.HasPrefix(string(blobID), string(prefix)) {
				for i := 0; i < len(v); i++ {
					v[i] ^= 1
				}
			}
		}
	}

	// make a new content manager based on corrupted data.
	bm, err = content.NewManagerForTesting(ctx, st, fop, nil, nil)
	require.NoError(t, err)

	t.Cleanup(func() { bm.Close(ctx) })

	mgr, err = NewManager(ctx, bm, ManagerOptions{})
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	cases := []struct {
		desc string
		f    func() error
	}{
		{"GetRaw", func() error {
			var raw json.RawMessage
			_, err := mgr.Get(ctx, "anything", &raw)
			return err
		}},
		{"GetMetadata", func() error { _, err := mgr.GetMetadata(ctx, "anything"); return err }},
		{"Get", func() error {
			_, err := mgr.Get(ctx, "anything", nil)
			return err
		}},
		{"Delete", func() error { return mgr.Delete(ctx, "anything") }},
		{"Find", func() error { _, err := mgr.Find(ctx, nil); return err }},
		// Put does not need to initialize
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.desc, func(t *testing.T) {
			err := tc.f()
			if err == nil || !strings.Contains(err.Error(), "invalid checksum") {
				t.Errorf("invalid error when initializing malformed manifest manager: %v", err)
			}
		})
	}
}

func addAndVerify(ctx context.Context, t *testing.T, mgr *Manager, labels map[string]string, data map[string]int) ID {
	t.Helper()

	id, err := mgr.Put(ctx, labels, data)
	if err != nil {
		t.Errorf("unable to add %v (%v): %v", labels, data, err)
		return ""
	}

	verifyItem(ctx, t, mgr, id, labels, data)

	return id
}

func verifyItem(ctx context.Context, t *testing.T, mgr *Manager, id ID, labels map[string]string, data map[string]int) {
	t.Helper()

	l, err := mgr.GetMetadata(ctx, id)
	if err != nil {
		t.Errorf("unable to retrieve %q: %v", id, err)
		return
	}

	if !reflect.DeepEqual(l.Labels, labels) {
		t.Errorf("invalid labels retrieved %v, wanted %v", l.Labels, labels)
	}

	var d2 map[string]int
	if _, err := mgr.Get(ctx, id, &d2); err != nil {
		t.Errorf("Get failed: %v", err)
	}

	if !reflect.DeepEqual(d2, data) {
		t.Errorf("invalid data retrieved %v, wanted %v", d2, data)
	}
}

func verifyItemNotFound(ctx context.Context, t *testing.T, mgr *Manager, id ID) {
	t.Helper()

	_, err := mgr.GetMetadata(ctx, id)
	if got, want := err, ErrNotFound; !errors.Is(got, want) {
		t.Errorf("invalid error when getting %q %v, expected %v", id, err, ErrNotFound)
		return
	}
}

func verifyMatches(ctx context.Context, t *testing.T, mgr *Manager, labels map[string]string, expected []ID) {
	t.Helper()

	var matches []ID

	items, err := mgr.Find(ctx, labels)
	if err != nil {
		t.Errorf("error in Find(): %v", err)
		return
	}

	for _, m := range items {
		matches = append(matches, m.ID)
	}

	sortIDs(matches)
	sortIDs(expected)

	if !reflect.DeepEqual(matches, expected) {
		t.Errorf("invalid matches for %v: %v, expected %v", labels, matches, expected)
	}
}

func sortIDs(s []ID) {
	sort.Slice(s, func(i, j int) bool {
		return s[i] < s[j]
	})
}

func newManagerForTesting(ctx context.Context, t *testing.T, data blobtesting.DataMap) *Manager {
	t.Helper()

	st := blobtesting.NewMapStorage(data, nil, nil)

	fop, err := format.NewFormattingOptionsProvider(&format.ContentFormat{
		Hash:       hashing.DefaultAlgorithm,
		Encryption: encryption.DefaultAlgorithm,
		MutableParameters: format.MutableParameters{
			Version:     1,
			MaxPackSize: 100000,
		},
	}, nil)

	require.NoError(t, err)

	bm, err := content.NewManagerForTesting(ctx, st, fop, nil, nil)
	require.NoError(t, err)

	t.Cleanup(func() { bm.Close(ctx) })

	mm, err := NewManager(ctx, bm, ManagerOptions{})
	require.NoError(t, err)

	return mm
}

func TestManifestInvalidPut(t *testing.T) {
	ctx := testlogging.Context(t)
	data := blobtesting.DataMap{}
	mgr := newManagerForTesting(ctx, t, data)

	cases := []struct {
		labels        map[string]string
		payload       interface{}
		expectedError string
	}{
		{map[string]string{"": ""}, "xxx", "'type' label is required"},
		{map[string]string{"type": "blah"}, complex128(1), "marshal error"},
	}

	for i, tc := range cases {
		_, err := mgr.Put(ctx, tc.labels, tc.payload)
		if err == nil || !strings.Contains(err.Error(), tc.expectedError) {
			t.Errorf("invalid error when putting case %v: %v, expected %v", i, err, tc.expectedError)
		}
	}
}

func TestManifestAutoCompaction(t *testing.T) {
	ctx := testlogging.Context(t)
	data := blobtesting.DataMap{}

	mgr := newManagerForTesting(ctx, t, data)

	for i := 0; i < 100; i++ {
		item1 := map[string]int{"foo": 1, "bar": 2}
		labels1 := map[string]string{"type": "item", "color": "red"}
		found, err := mgr.Find(ctx, labels1)
		require.NoError(t, err)

		if i%30 == 0 {
			require.NoError(t, mgr.Compact(ctx))
		}

		if got, want := len(found), i; got != want {
			t.Fatalf("unexpected number of manifests found: %v, want %v", got, want)
		}

		addAndVerify(ctx, t, mgr, labels1, item1)

		require.NoError(t, mgr.Flush(ctx))
		require.NoError(t, mgr.b.Flush(ctx))
	}
}
