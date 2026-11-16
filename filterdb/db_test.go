package filterdb

import (
	"math/rand"
	"os"
	"reflect"
	"testing"

	"github.com/gcash/bchd/chaincfg"
	"github.com/gcash/bchd/chaincfg/chainhash"
	"github.com/gcash/bchutil/gcs"
	"github.com/gcash/bchutil/gcs/builder"
	"github.com/gcash/bchwallet/walletdb"
	_ "github.com/gcash/bchwallet/walletdb/bdb"
)

func createTestDatabase() (func(), FilterDatabase, error) {
	tempDir, err := os.MkdirTemp("", "neutrino")
	if err != nil {
		return nil, nil, err
	}

	db, err := walletdb.Create("bdb", tempDir+"/test.db", true)
	if err != nil {
		return nil, nil, err
	}

	cleanUp := func() {
		_ = os.RemoveAll(tempDir)
		_ = db.Close()
	}

	filterDB, err := New(db, chaincfg.SimNetParams)
	if err != nil {
		return nil, nil, err
	}

	return cleanUp, filterDB, nil
}

func TestGenesisFilterCreation(t *testing.T) {
	cleanUp, database, err := createTestDatabase()
	defer cleanUp()
	if err != nil {
		t.Fatalf("unable to create test db: %v", err)
	}

	genesisHash := chaincfg.SimNetParams.GenesisHash

	// With the database initialized, we should be able to fetch the
	// regular filter for the genesis block.
	regGenesisFilter, err := database.FetchFilter(genesisHash, RegularFilter)
	if err != nil {
		t.Fatalf("unable to fetch regular genesis filter: %v", err)
	}

	// The regular filter should be non-nil as the gensis block's output
	// and the coinbase txid should be indexed.
	if regGenesisFilter == nil {
		t.Fatalf("regular genesis filter is nil")
	}
}

func genRandFilter(numElements uint32) (*gcs.Filter, error) {
	elements := make([][]byte, numElements)
	for i := uint32(0); i < numElements; i++ {
		var elem [20]byte
		//nolint:staticcheck // test code: deterministic math/rand is fine.
		if _, err := rand.Read(elem[:]); err != nil {
			return nil, err
		}

		elements[i] = elem[:]
	}

	var key [16]byte
	//nolint:staticcheck // test code: deterministic math/rand is fine.
	if _, err := rand.Read(key[:]); err != nil {
		return nil, err
	}

	filter, err := gcs.BuildGCSFilter(
		builder.DefaultP, builder.DefaultM, key, elements,
	)
	if err != nil {
		return nil, err
	}

	return filter, nil
}

func TestFilterStorage(t *testing.T) {
	// TODO(roasbeef): use testing.Quick
	cleanUp, database, err := createTestDatabase()
	defer cleanUp()
	if err != nil {
		t.Fatalf("unable to create test db: %v", err)
	}

	// We'll generate a random block hash to create our test filters
	// against.
	var randHash chainhash.Hash
	//nolint:staticcheck // test code: deterministic math/rand is fine.
	if _, err := rand.Read(randHash[:]); err != nil {
		t.Fatalf("unable to generate random hash: %v", err)
	}

	// First, we'll create and store a random fitler for the regular filter
	// type for the block hash generate above.
	regFilter, err := genRandFilter(100)
	if err != nil {
		t.Fatalf("unable to create random filter: %v", err)
	}
	err = database.PutFilter(&randHash, regFilter, RegularFilter)
	if err != nil {
		t.Fatalf("unable to store regular filter: %v", err)
	}

	// With the filter stored, we should be able to retrieve the filter
	// without any issue, and it should match the stored filter exactly.
	regFilterDB, err := database.FetchFilter(&randHash, RegularFilter)
	if err != nil {
		t.Fatalf("unable to retrieve reg filter: %v", err)
	}
	if !reflect.DeepEqual(regFilter, regFilterDB) {
		t.Fatalf("regular filter doesn't match!")
	}
}

// TestFilterDeletion checks that DeleteFilters removes the stored filters it's
// given, so that fetching them again reports ErrFilterNotFound, that it skips
// filters the database doesn't hold, and that it leaves the others.
func TestFilterDeletion(t *testing.T) {
	cleanUp, database, err := createTestDatabase()
	defer cleanUp()
	if err != nil {
		t.Fatalf("unable to create test db: %v", err)
	}

	hashes := []chainhash.Hash{{1}, {2}, {3}}
	for i := range hashes {
		filter, err := genRandFilter(10)
		if err != nil {
			t.Fatalf("unable to create random filter: %v", err)
		}
		err = database.PutFilter(&hashes[i], filter, RegularFilter)
		if err != nil {
			t.Fatalf("unable to store filter: %v", err)
		}
	}

	missing := chainhash.Hash{4}
	err = database.DeleteFilters(
		[]*chainhash.Hash{&hashes[0], &missing, &hashes[2]},
		RegularFilter,
	)
	if err != nil {
		t.Fatalf("unable to delete filters: %v", err)
	}
	for _, hash := range []chainhash.Hash{hashes[0], hashes[2]} {
		_, err = database.FetchFilter(&hash, RegularFilter)
		if err != ErrFilterNotFound {
			t.Fatalf("fetching deleted filter %v returned %v, "+
				"want ErrFilterNotFound", hash, err)
		}
	}
	if _, err := database.FetchFilter(&hashes[1], RegularFilter); err != nil {
		t.Fatalf("the other filter is gone: %v", err)
	}
}

// TestScriptFilterStorage checks that script filters are stored apart from
// the regular ones, under the same block hash, that a database without any
// reports none rather than failing, and that they can be deleted.
func TestScriptFilterStorage(t *testing.T) {
	cleanUp, database, err := createTestDatabase()
	defer cleanUp()
	if err != nil {
		t.Fatalf("unable to create test db: %v", err)
	}
	hash := chainhash.Hash{0x51}

	// Before the first one is stored there is no bucket for them, as in a
	// database made before script filters.
	if _, err := database.FetchFilter(&hash, ScriptFilter); err != ErrFilterNotFound {
		t.Fatalf("script filter of an empty database: %v", err)
	}
	if err := database.DeleteFilters([]*chainhash.Hash{&hash}, ScriptFilter); err != nil {
		t.Fatalf("deleting from an empty database: %v", err)
	}

	regFilter, err := genRandFilter(50)
	if err != nil {
		t.Fatal(err)
	}
	scriptFilter, err := genRandFilter(70)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.PutFilter(&hash, regFilter, RegularFilter); err != nil {
		t.Fatal(err)
	}
	if err := database.PutFilter(&hash, scriptFilter, ScriptFilter); err != nil {
		t.Fatal(err)
	}
	for fType, want := range map[FilterType]*gcs.Filter{
		RegularFilter: regFilter, ScriptFilter: scriptFilter,
	} {
		got, err := database.FetchFilter(&hash, fType)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Errorf("filter of type %d doesn't match: %v", fType, err)
		}
	}

	// Deleting the script filter leaves the regular one.
	if err := database.DeleteFilters([]*chainhash.Hash{&hash}, ScriptFilter); err != nil {
		t.Fatal(err)
	}
	if _, err := database.FetchFilter(&hash, ScriptFilter); err != ErrFilterNotFound {
		t.Errorf("deleted script filter: %v", err)
	}
	if _, err := database.FetchFilter(&hash, RegularFilter); err != nil {
		t.Errorf("regular filter after deleting the script filter: %v", err)
	}
}
