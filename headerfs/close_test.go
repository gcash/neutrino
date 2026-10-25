package headerfs

import (
	"testing"

	"github.com/gcash/bchd/wire"
)

// TestHeaderStoreClose checks that closing the header stores closes their flat
// files.
func TestHeaderStoreClose(t *testing.T) {
	cleanUp, _, _, bhs, err := createTestBlockHeaderStore()
	if cleanUp != nil {
		defer cleanUp()
	}
	if err != nil {
		t.Fatalf("unable to create block header store: %v", err)
	}
	if err := bhs.Close(); err != nil {
		t.Fatalf("unable to close block header store: %v", err)
	}
	err = bhs.WriteHeaders(BlockHeader{
		BlockHeader: &wire.BlockHeader{},
		Height:      1,
	})
	if err == nil {
		t.Fatal("wrote to a closed block header store")
	}

	cleanUp2, _, _, fhs, err := createTestFilterHeaderStore()
	if cleanUp2 != nil {
		defer cleanUp2()
	}
	if err != nil {
		t.Fatalf("unable to create filter header store: %v", err)
	}
	if err := fhs.Close(); err != nil {
		t.Fatalf("unable to close filter header store: %v", err)
	}
	if err := fhs.Close(); err == nil {
		t.Fatal("closed a filter header store twice without an error")
	}
}
