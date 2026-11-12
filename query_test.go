package neutrino

import (
	"bytes"
	"compress/bzip2"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/big"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/gcash/bchd/blockchain"
	"github.com/gcash/bchd/chaincfg"
	"github.com/gcash/bchd/chaincfg/chainhash"
	"github.com/gcash/bchd/peer"
	"github.com/gcash/bchd/wire"
	"github.com/gcash/bchutil"
	"github.com/gcash/bchutil/gcs"
	"github.com/gcash/bchutil/gcs/builder"
	"github.com/gcash/bchwallet/waddrmgr"
	"github.com/gcash/bchwallet/walletdb"
	"github.com/gcash/neutrino/cache"
	"github.com/gcash/neutrino/cache/lru"
	"github.com/gcash/neutrino/filterdb"
	"github.com/gcash/neutrino/headerfs"
)

var (
	// maxPowLimit is used as the max block target to ensure all PoWs are
	// valid.
	bigOne      = big.NewInt(1)
	maxPowLimit = new(big.Int).Sub(new(big.Int).Lsh(bigOne, 255), bigOne)

	// blockDataNet is the expected network in the test block data.
	blockDataNet = wire.BitcoinNet(0xd9b4bef9)

	// blockDataFile is the path to a file containing the first 256 blocks
	// of the block chain.
	blockDataFile = filepath.Join("testdata", "blocks1-256.bz2")
)

// loadBlocks loads the blocks contained in the testdata directory and returns
// a slice of them.
//
// NOTE: copied from gcash/bchd/database/ffldb/interface_test.go
func loadBlocks(t *testing.T, dataFile string, network wire.BitcoinNet) (
	[]*bchutil.Block, error) {
	// Open the file that contains the blocks for reading.
	fi, err := os.Open(dataFile)
	if err != nil {
		t.Errorf("failed to open file %v, err %v", dataFile, err)
		return nil, err
	}
	defer func() {
		if err := fi.Close(); err != nil {
			t.Errorf("failed to close file %v %v", dataFile,
				err)
		}
	}()
	dr := bzip2.NewReader(fi)

	// Set the first block as the genesis block.
	blocks := make([]*bchutil.Block, 0, 256)
	genesis := bchutil.NewBlock(chaincfg.MainNetParams.GenesisBlock)
	blocks = append(blocks, genesis)

	// Load the remaining blocks.
	for height := 1; ; height++ {
		var net uint32
		err := binary.Read(dr, binary.LittleEndian, &net)
		if err == io.EOF {
			// Hit end of file at the expected offset.  No error.
			break
		}
		if err != nil {
			t.Errorf("Failed to load network type for block %d: %v",
				height, err)
			return nil, err
		}
		if net != uint32(network) {
			t.Errorf("Block doesn't match network: %v expects %v",
				net, network)
			return nil, err
		}

		var blockLen uint32
		err = binary.Read(dr, binary.LittleEndian, &blockLen)
		if err != nil {
			t.Errorf("Failed to load block size for block %d: %v",
				height, err)
			return nil, err
		}

		// Read the block.
		blockBytes := make([]byte, blockLen)
		_, err = io.ReadFull(dr, blockBytes)
		if err != nil {
			t.Errorf("Failed to load block %d: %v", height, err)
			return nil, err
		}

		// Deserialize and store the block.
		block, err := bchutil.NewBlockFromBytes(blockBytes)
		if err != nil {
			t.Errorf("Failed to parse block %v: %v", height, err)
			return nil, err
		}
		blocks = append(blocks, block)
	}

	return blocks, nil
}

// genRandomBlockHash generates a random block hash using math/rand.
func genRandomBlockHash() *chainhash.Hash {
	var seed [32]byte
	//nolint:staticcheck // test code: deterministic math/rand is fine.
	rand.Read(seed[:])
	hash := chainhash.Hash(seed)
	return &hash
}

// getRandFilter generates a random GCS filter that contains numElements. It
// will then convert that filter into CacheableFilter to compute it's size for
// convenience. It will return the filter along with it's size and randomly
// generated block hash. testing.T is passed in as a convenience to deal with
// errors in this method and making the test code more straigthforward. Method
// originally taken from filterdb/db_test.go.
func genRandFilter(numElements uint32, t *testing.T) (
	*chainhash.Hash, *gcs.Filter, uint64) {
	elements := make([][]byte, numElements)
	for i := uint32(0); i < numElements; i++ {
		var elem [20]byte
		//nolint:staticcheck // test code: deterministic math/rand is fine.
		if _, err := rand.Read(elem[:]); err != nil {
			t.Fatalf("unable to create random filter: %v", err)
			return nil, nil, 0
		}

		elements[i] = elem[:]
	}

	var key [16]byte
	//nolint:staticcheck // test code: deterministic math/rand is fine.
	if _, err := rand.Read(key[:]); err != nil {
		t.Fatalf("unable to create random filter: %v", err)
		return nil, nil, 0
	}

	filter, err := gcs.BuildGCSFilter(
		builder.DefaultP, builder.DefaultM, key, elements)
	if err != nil {
		t.Fatalf("unable to create random filter: %v", err)
		return nil, nil, 0
	}

	// Convert into CacheableFilter and compute Size.
	c := &cache.CacheableFilter{Filter: filter}
	s, err := c.Size()
	if err != nil {
		t.Fatalf("unable to create random filter: %v", err)
		return nil, nil, 0
	}

	return genRandomBlockHash(), filter, s
}

// getFilter is a convenience method which will extract a value from the cache
// and handle errors, it makes the test code easier to follow.
func getFilter(cs *ChainService, b *chainhash.Hash, t *testing.T) *gcs.Filter {
	val, err := cs.getFilterFromCache(b, filterdb.RegularFilter)
	if err != nil {
		t.Fatal(err)
	}
	return val
}

func assertEqual(t *testing.T, a interface{}, b interface{}, message string) {
	if a == b {
		return
	}
	if len(message) == 0 {
		message = fmt.Sprintf("%v != %v", a, b)
	}
	t.Fatal(message)
}

// TestCacheBigEnoughHoldsAllFilter creates a cache big enough to hold all
// filters, then gets them in random order and makes sure they are always there.
func TestCacheBigEnoughHoldsAllFilter(t *testing.T) {
	// Create different sized filters.
	b1, f1, s1 := genRandFilter(1, t)
	b2, f2, s2 := genRandFilter(10, t)
	b3, f3, s3 := genRandFilter(100, t)

	cs := &ChainService{
		FilterCache: lru.NewCache(s1 + s2 + s3),
	}

	// Insert those filters into the cache making sure nothing gets evicted.
	assertEqual(t, cs.FilterCache.Len(), 0, "")
	_, _ = cs.putFilterToCache(b1, filterdb.RegularFilter, f1)
	assertEqual(t, cs.FilterCache.Len(), 1, "")
	_, _ = cs.putFilterToCache(b2, filterdb.RegularFilter, f2)
	assertEqual(t, cs.FilterCache.Len(), 2, "")
	_, _ = cs.putFilterToCache(b3, filterdb.RegularFilter, f3)
	assertEqual(t, cs.FilterCache.Len(), 3, "")

	// Check that we can get those filters back independent of Get order.
	assertEqual(t, getFilter(cs, b1, t), f1, "")
	assertEqual(t, getFilter(cs, b2, t), f2, "")
	assertEqual(t, getFilter(cs, b3, t), f3, "")
	assertEqual(t, getFilter(cs, b2, t), f2, "")
	assertEqual(t, getFilter(cs, b3, t), f3, "")
	assertEqual(t, getFilter(cs, b1, t), f1, "")
	assertEqual(t, getFilter(cs, b3, t), f3, "")

	assertEqual(t, cs.FilterCache.Len(), 3, "")
}

// TestBigFilterEvictsEverything creates a cache big enough to hold a large
// filter and inserts many smaller filters into. Then it inserts the big filter
// and verifies that it's the only one remaining.
func TestBigFilterEvictsEverything(t *testing.T) {
	// Create different sized filters.
	b1, f1, _ := genRandFilter(1, t)
	b2, f2, _ := genRandFilter(3, t)
	b3, f3, s3 := genRandFilter(10, t)

	cs := &ChainService{
		FilterCache: lru.NewCache(s3),
	}

	// Insert the smaller filters.
	assertEqual(t, cs.FilterCache.Len(), 0, "")
	_, _ = cs.putFilterToCache(b1, filterdb.RegularFilter, f1)
	assertEqual(t, cs.FilterCache.Len(), 1, "")
	_, _ = cs.putFilterToCache(b2, filterdb.RegularFilter, f2)
	assertEqual(t, cs.FilterCache.Len(), 2, "")

	// Insert the big filter and check all previous filters are evicted.
	_, _ = cs.putFilterToCache(b3, filterdb.RegularFilter, f3)
	assertEqual(t, cs.FilterCache.Len(), 1, "")
	assertEqual(t, getFilter(cs, b3, t), f3, "")
}

// TestBlockCache checks that blocks are inserted and fetched from the cache
// before peers are queried.
func TestBlockCache(t *testing.T) {
	t.Parallel()

	// Load the first 255 blocks from disk.
	blocks, err := loadBlocks(t, blockDataFile, blockDataNet)
	if err != nil {
		t.Fatalf("loadBlocks: Unexpected error: %v", err)
	}

	// We'll use a simple mock header store since the GetBlocks method
	// assumes we only query for blocks with an already known header.
	headers := newMockBlockHeaderStore()

	// Iterate through the blocks, calculating the size of half of them,
	// and writing them to the header store.
	var size uint64
	for i, b := range blocks {
		header := headerfs.BlockHeader{
			BlockHeader: &b.MsgBlock().Header,
			Height:      uint32(i),
		}
		_ = headers.WriteHeaders(header)

		sz, _ := (&cache.CacheableBlock{Block: b}).Size()
		if i < len(blocks)/2 {
			size += sz
		}
	}

	// No filter headers are synced, so blocks aren't checked against them.
	_, _, cfStore, cleanUp, err := setupBlockManager()
	if err != nil {
		t.Fatal(err)
	}
	defer cleanUp()

	// Set up a ChainService with a BlockCache that can fit the first half
	// of the blocks.
	cs := &ChainService{
		BlockCache:       lru.NewCache(size),
		BlockHeaders:     headers,
		RegFilterHeaders: cfStore,
		filterHeaders:    cfStore,
		chainParams: chaincfg.Params{
			PowLimit: maxPowLimit,
		},
		timeSource: blockchain.NewMedianTime(),
	}

	// We'll set up the queryPeers method to make sure we are only querying
	// for blocks, and send the block hashes queried over the queries
	// channel.
	queries := make(chan chainhash.Hash, 1)
	cs.queryPeers = func(msg wire.Message, f func(*ServerPeer,
		wire.Message, chan<- struct{}), _ ...QueryOption) {

		getData, ok := msg.(*wire.MsgGetData)
		if !ok {
			t.Fatalf("unexpected type: %T", msg)
		}

		if len(getData.InvList) != 1 {
			t.Fatalf("expected 1 element in inv list, found %v",
				len(getData.InvList))
		}

		inv := getData.InvList[0]
		if inv.Type != wire.InvTypeBlock {
			t.Fatalf("unexpected inv type: %v", inv.Type)
		}

		// Serve the block that matches the requested block header.
		for _, b := range blocks {
			if *b.Hash() == inv.Hash {

				// Execute the callback with the found block,
				// and wait for the quit channel to be closed.
				quit := make(chan struct{})
				f(nil, b.MsgBlock(), quit)

				select {
				case <-quit:
				case <-time.After(1 * time.Second):
					t.Fatalf("channel not closed")
				}

				// Notify the test about the query.
				select {
				case queries <- inv.Hash:
				case <-time.After(1 * time.Second):
					t.Fatalf("query was not handled")
				}

				return
			}
		}

		t.Fatalf("queried for unknown block: %v", inv.Hash)
	}

	// fetchAndAssertPeersQueried calls GetBlock and makes sure the block
	// is fetched from the peers.
	fetchAndAssertPeersQueried := func(hash chainhash.Hash) {
		found, err := cs.GetBlock(hash)
		if err != nil {
			t.Fatalf("error getting block: %v", err)
		}

		if *found.Hash() != hash {
			t.Fatalf("requested block with hash %v, got %v",
				hash, found.Hash())
		}

		select {
		case q := <-queries:
			if q != hash {
				t.Fatalf("expected hash %v to be queried, "+
					"got %v", hash, q)
			}
		case <-time.After(1 * time.Second):
			t.Fatalf("did not query peers for block")
		}
	}

	// fetchAndAssertInCache calls GetBlock and makes sure the block is not
	// fetched from the peers.
	fetchAndAssertInCache := func(hash chainhash.Hash) {
		found, err := cs.GetBlock(hash)
		if err != nil {
			t.Fatalf("error getting block: %v", err)
		}

		if *found.Hash() != hash {
			t.Fatalf("requested block with hash %v, got %v",
				hash, found.Hash())
		}

		// Make sure we didn't query the peers for this block.
		select {
		case q := <-queries:
			t.Fatalf("did not expect query for block %v", q)
		default:
		}
	}

	// Get the first half of the blocks. Since this is the first time we
	// request them, we expect them all to be fetched from peers.
	for _, b := range blocks[:len(blocks)/2] {
		fetchAndAssertPeersQueried(*b.Hash())
	}

	// Get the first half of the blocks again. This time we expect them all
	// to be fetched from the cache.
	for _, b := range blocks[:len(blocks)/2] {
		fetchAndAssertInCache(*b.Hash())
	}

	// Get the second half of the blocks. These have not been fetched
	// before, and we expect them to be fetched from peers.
	for _, b := range blocks[len(blocks)/2:] {
		fetchAndAssertPeersQueried(*b.Hash())
	}

	// Since the cache only had capacity for the first half of the blocks,
	// some of these should now have been evicted. We only check the first
	// one, since we cannot know for sure how many because of the variable
	// size.
	b := blocks[0]
	fetchAndAssertPeersQueried(*b.Hash())
}

// TestGetCFilterNotFetched checks that GetCFilter returns an error when no peer
// sends the filter it asks for, or when the ChainService is shutting down. It
// returned no filter and no error, which callers took for a block that matches
// nothing.
func TestGetCFilterNotFetched(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	db, err := walletdb.Create("bdb", filepath.Join(tempDir, "weks.db"), true)
	if err != nil {
		t.Fatalf("unable to create db: %v", err)
	}
	defer func() { _ = db.Close() }()

	hdrStore, err := headerfs.NewBlockHeaderStore(
		tempDir, db, &chaincfg.SimNetParams,
	)
	if err != nil {
		t.Fatalf("unable to create block header store: %v", err)
	}
	cfStore, err := headerfs.NewFilterHeaderStore(
		tempDir, db, headerfs.RegularFilter, &chaincfg.SimNetParams,
		nil,
	)
	if err != nil {
		t.Fatalf("unable to create filter header store: %v", err)
	}
	filterDB, err := filterdb.New(db, chaincfg.SimNetParams)
	if err != nil {
		t.Fatalf("unable to create filter db: %v", err)
	}

	// Add a block above genesis with its filter header, so that GetCFilter
	// asks peers for its filter.
	genesis, _, err := hdrStore.ChainTip()
	if err != nil {
		t.Fatal(err)
	}
	header := wire.BlockHeader{PrevBlock: genesis.BlockHash(), Nonce: 1}
	err = hdrStore.WriteHeaders(headerfs.BlockHeader{
		BlockHeader: &header,
		Height:      1,
	})
	if err != nil {
		t.Fatalf("unable to write block header: %v", err)
	}
	err = cfStore.WriteHeaders(headerfs.FilterHeader{
		HeaderHash: header.BlockHash(),
		FilterHash: chainhash.Hash{1},
		Height:     1,
	})
	if err != nil {
		t.Fatalf("unable to write filter header: %v", err)
	}

	for _, shutdown := range []bool{false, true} {
		cs := &ChainService{
			FilterDB:         filterDB,
			FilterCache:      lru.NewCache(1000),
			BlockHeaders:     hdrStore,
			RegFilterHeaders: cfStore,
			filterHeaders:    cfStore,
			quit:             make(chan struct{}),
		}

		// No peer answers the query.
		queried := make(chan struct{}, 1)
		cs.queryPeers = func(wire.Message, func(*ServerPeer,
			wire.Message, chan<- struct{}), ...QueryOption) {

			queried <- struct{}{}
		}
		if shutdown {
			close(cs.quit)
		}

		want := ErrFilterFetchFailed
		if shutdown {
			want = ErrShuttingDown
		}
		filter, err := cs.GetCFilter(
			header.BlockHash(), wire.GCSFilterRegular,
		)
		if filter != nil || !errors.Is(err, want) {
			t.Fatalf("shutdown=%v: expected error %v, got filter "+
				"%v and error %v", shutdown, want, filter, err)
		}

		// Unless the ChainService was shutting down, which can end
		// the wait before the query starts, peers were asked.
		if !shutdown {
			select {
			case <-queried:
			default:
				t.Fatal("expected peers to be queried")
			}
		}
	}
}

// TestGetCFilterRequestsRepair checks that GetCFilter asks for a repair of the
// filter headers when the only filters peers serve for a block don't match its
// filter header, and not when a peer serves one that does, or none at all.
func TestGetCFilterRequestsRepair(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		served     []string
		wantFilter bool
		wantRepair bool
	}{
		{
			name:       "only filters that don't match",
			served:     []string{"bad", "bad"},
			wantRepair: true,
		},
		{
			name:       "a filter that matches",
			served:     []string{"bad", "good"},
			wantFilter: true,
		},
		{
			name: "no filters",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			bm, hdrStore, cfStore, cleanUp, err := setupBlockManager()
			if err != nil {
				t.Fatalf("unable to set up ChainService: %v", err)
			}
			defer cleanUp()

			// Add a block above genesis with the filter header of
			// the good filter.
			genesis, _, err := hdrStore.ChainTip()
			if err != nil {
				t.Fatal(err)
			}
			header := wire.BlockHeader{
				PrevBlock: genesis.BlockHash(), Nonce: 1,
			}
			blockHash := header.BlockHash()
			err = hdrStore.WriteHeaders(headerfs.BlockHeader{
				BlockHeader: &header,
				Height:      1,
			})
			if err != nil {
				t.Fatalf("unable to write block header: %v", err)
			}

			filters := make(map[string]*gcs.Filter)
			for i, name := range []string{"good", "bad"} {
				filters[name], err = builder.WithKeyHash(&blockHash).
					AddEntry([]byte{byte(i)}).Build()
				if err != nil {
					t.Fatalf("unable to build filter: %v", err)
				}
			}
			genesisFilterHeader, _, err := cfStore.ChainTip()
			if err != nil {
				t.Fatal(err)
			}
			filterHeader, err := builder.MakeHeaderForFilter(
				filters["good"], *genesisFilterHeader,
			)
			if err != nil {
				t.Fatal(err)
			}
			err = cfStore.WriteHeaders(headerfs.FilterHeader{
				HeaderHash: blockHash,
				FilterHash: filterHeader,
				Height:     1,
			})
			if err != nil {
				t.Fatalf("unable to write filter header: %v", err)
			}

			db, err := walletdb.Create(
				"bdb", filepath.Join(t.TempDir(), "filters.db"),
				true,
			)
			if err != nil {
				t.Fatalf("unable to create db: %v", err)
			}
			defer func() { _ = db.Close() }()
			filterDB, err := filterdb.New(db, chaincfg.SimNetParams)
			if err != nil {
				t.Fatalf("unable to create filter db: %v", err)
			}

			cs := bm.server
			cs.blockManager = bm
			cs.FilterDB = filterDB
			cs.FilterCache = lru.NewCache(1000)
			cs.quit = make(chan struct{})
			cs.queryPeers = func(_ wire.Message, f func(*ServerPeer,
				wire.Message, chan<- struct{}), _ ...QueryOption) {

				quit := make(chan struct{})
				for _, name := range test.served {
					data, _ := filters[name].NBytes()
					f(nil, wire.NewMsgCFilter(
						wire.GCSFilterRegular, &blockHash,
						data,
					), quit)

					select {
					case <-quit:
						return
					default:
					}
				}
			}

			filter, err := cs.GetCFilter(
				blockHash, wire.GCSFilterRegular,
			)
			if test.wantFilter {
				if err != nil || filter == nil {
					t.Fatalf("expected the good filter, got "+
						"%v, %v", filter, err)
				}
			} else if !errors.Is(err, ErrFilterFetchFailed) {
				t.Fatalf("expected ErrFilterFetchFailed, got "+
					"%v, %v", filter, err)
			}

			// GetCFilter can return before its query is done,
			// which we'll wait for, as the query holds the lock on
			// filter queries until then.
			cs.mtxCFilter.Lock()
			bm.newHeadersMtx.Lock()
			requested := bm.filterRepairRequested
			bm.newHeadersMtx.Unlock()
			cs.mtxCFilter.Unlock()
			if requested != test.wantRepair {
				t.Fatalf("expected a repair of the filter "+
					"headers requested %v, got %v",
					test.wantRepair, requested)
			}
		})
	}
}

// TestGetCFilterAfterRollback checks that a filter query that read the filter
// headers before they're rolled back ends at the next filter served, and
// neither returns nor keeps a filter that matches them, nor asks for a repair
// of them for a filter that didn't match them, so that the filter is fetched
// again with the filter headers that replace them.
func TestGetCFilterAfterRollback(t *testing.T) {
	t.Parallel()

	rt := setupRepairTest(t, 1500)
	rt.storeFilterChain(t, rt.wrong, repairTestTip)
	cs := rt.bm.server
	cs.quit = make(chan struct{})

	// A peer serves the right filter, which doesn't match the filter
	// headers, and they're rolled back before it serves it again and then
	// the filter that matches them.
	blockHash := rt.blockHashes[rt.divergence]
	served := make([][]byte, 0, 2)
	for _, chain := range []*filterChain{rt.right, rt.wrong} {
		data, err := chain.filters[rt.divergence].NBytes()
		if err != nil {
			t.Fatal(err)
		}
		served = append(served, data)
	}
	cs.queryPeers = func(_ wire.Message, f func(*ServerPeer,
		wire.Message, chan<- struct{}), _ ...QueryOption) {

		quit := make(chan struct{})
		f(nil, wire.NewMsgCFilter(
			wire.GCSFilterRegular, &blockHash, served[0],
		), quit)

		err := rt.bm.rollBackFilterHeaders(rt.divergence - 1)
		if err != nil {
			t.Errorf("unable to roll back filter headers: %v", err)
		}
		f(nil, wire.NewMsgCFilter(
			wire.GCSFilterRegular, &blockHash, served[0],
		), quit)
		select {
		case <-quit:
		default:
			t.Error("expected the query to end once the filter " +
				"headers were rolled back")
		}

		f(nil, wire.NewMsgCFilter(
			wire.GCSFilterRegular, &blockHash, served[1],
		), quit)
	}

	filter, err := cs.GetCFilter(blockHash, wire.GCSFilterRegular)
	if filter != nil || !errors.Is(err, ErrFilterFetchFailed) {
		t.Fatalf("expected ErrFilterFetchFailed, got %v, %v", filter,
			err)
	}

	// We'll wait for the query to be done, as it holds the lock on filter
	// queries until then.
	cs.mtxCFilter.Lock()
	cached, err := cs.getFilterFromCache(&blockHash, filterdb.RegularFilter)
	rt.bm.newHeadersMtx.Lock()
	requested := rt.bm.filterRepairRequested
	rt.bm.newHeadersMtx.Unlock()
	cs.mtxCFilter.Unlock()

	if cached != nil || err != cache.ErrElementNotFound {
		t.Fatalf("expected the filter not to be cached, got %v, %v",
			cached, err)
	}
	if requested {
		t.Fatal("expected no repair of the filter headers requested")
	}
}

// TestPeerTurnEndsDespiteMessages ensures a peer that keeps sending messages loses
// its turn, and that the next peer gets a turn of its own.  The timers are too long
// to fire, so that it doesn't depend on how the test is scheduled.
func TestPeerTurnEndsDespiteMessages(t *testing.T) {
	idle := time.NewTimer(time.Hour)
	turn := newPeerTurn(idle, time.Hour)
	defer turn.stop()

	turn.responded()
	turn.end()
	turn.responded()
	select {
	case <-idle.C:
	case <-time.After(time.Minute):
		t.Fatal("a message kept the peer's turn going")
	}

	// The timer has fired, so it's only running if the turn started it, and
	// the next peer has a timeout without sending anything.  The deadline
	// is stopped first for the same reason, as the next peer has its own.
	turn.deadline.Stop()
	turn.next()
	if !idle.Stop() {
		t.Fatal("the next peer has no timeout")
	}
	if !turn.deadline.Stop() {
		t.Fatal("the next peer has no deadline")
	}

	// It's stopped now, so it's only running if the message reset it.
	turn.responded()
	if !idle.Stop() {
		t.Fatal("the next peer's turn is over")
	}
}

// TestLockCFilterGivesUp ensures a query that can give up doesn't wait for a lock
// another query holds, and leaves when we're shutting down too.
func TestLockCFilterGivesUp(t *testing.T) {
	s := &ChainService{quit: make(chan struct{})}
	s.mtxCFilter.Lock()
	defer s.mtxCFilter.Unlock()
	quit := make(chan struct{})
	close(quit)
	if s.lockCFilter(QueryQuit(quit)) {
		t.Fatal("took a lock that's held")
	}

	close(s.quit)
	if s.lockCFilter(QueryQuit(make(chan struct{}))) {
		t.Fatal("took a lock that's held while shutting down")
	}
}

// TestLockCFilterWaits ensures a query that can give up gets the lock once it's
// free, rather than giving up because it was held.
func TestLockCFilterWaits(t *testing.T) {
	s := &ChainService{quit: make(chan struct{})}
	s.mtxCFilter.Lock()
	got := make(chan bool, 1)
	go func() { got <- s.lockCFilter(QueryQuit(make(chan struct{}))) }()

	// Long enough for a wait that gives up to show itself.
	time.Sleep(100 * time.Millisecond)
	s.mtxCFilter.Unlock()
	select {
	case ok := <-got:
		if !ok {
			t.Fatal("gave up with nothing telling it to")
		}
	case <-time.After(time.Minute):
		t.Fatal("didn't take a lock that's free")
	}
}

// TestCFilterLock ensures the filter lock's zero value is unlocked, that it holds
// its slot's value while taken and none once freed, that it can be taken again,
// and that freeing it while free panics, as a mutex's does.
func TestCFilterLock(t *testing.T) {
	var l cfilterLock
	for range 2 {
		l.Lock()
		if len(l.ch()) != 1 {
			t.Fatal("a taken lock holds nothing")
		}
		l.Unlock()
		if len(l.ch()) != 0 {
			t.Fatal("a freed lock still holds its value")
		}
	}
	defer func() {
		if recover() == nil {
			t.Fatal("freed a free lock")
		}
	}()
	l.Unlock()
}

// TestGetCFilterShuttingDown ensures a query waiting for the lock says why it left.
func TestGetCFilterShuttingDown(t *testing.T) {
	t.Parallel()

	rt := setupRepairTest(t, 1500)
	rt.storeFilterChain(t, rt.wrong, repairTestTip)
	cs := rt.bm.server
	cs.quit = make(chan struct{})
	close(cs.quit)
	cs.mtxCFilter.Lock()
	defer cs.mtxCFilter.Unlock()
	_, err := cs.GetCFilter(
		rt.blockHashes[rt.divergence], wire.GCSFilterRegular,
		QueryQuit(make(chan struct{})),
	)
	if !errors.Is(err, ErrShuttingDown) {
		t.Fatalf("expected ErrShuttingDown, got %v", err)
	}
}

// TestGetCFilterGaveUpRequestsNoRepair ensures a query that was ended asks for no
// repair of the filter headers, as peers it didn't ask could have had the filter.
func TestGetCFilterGaveUpRequestsNoRepair(t *testing.T) {
	t.Parallel()

	rt := setupRepairTest(t, 1500)
	rt.storeFilterChain(t, rt.wrong, repairTestTip)
	cs := rt.bm.server
	cs.quit = make(chan struct{})
	blockHash := rt.blockHashes[rt.divergence]
	data, err := rt.right.filters[rt.divergence].NBytes()
	if err != nil {
		t.Fatal(err)
	}
	gaveUp := make(chan struct{})
	cs.queryPeers = func(_ wire.Message, f func(*ServerPeer,
		wire.Message, chan<- struct{}), _ ...QueryOption) {

		f(nil, wire.NewMsgCFilter(
			wire.GCSFilterRegular, &blockHash, data,
		), make(chan struct{}))
		close(gaveUp)
	}

	filter, err := cs.GetCFilter(
		blockHash, wire.GCSFilterRegular, QueryQuit(gaveUp),
	)
	if filter != nil {
		t.Fatalf("expected no filter, got %v, %v", filter, err)
	}

	// The query holds the lock on filter queries until it's done.
	cs.mtxCFilter.Lock()
	rt.bm.newHeadersMtx.Lock()
	requested := rt.bm.filterRepairRequested
	rt.bm.newHeadersMtx.Unlock()
	cs.mtxCFilter.Unlock()
	if requested {
		t.Fatal("expected no repair of the filter headers requested")
	}
}

// TestRescanQueriesEnd ensures a rescan's queries for blocks and for the filters it
// catches up with end when it stops, and keep the options it gave them.
func TestRescanQueriesEnd(t *testing.T) {
	quit := make(chan struct{})
	close(quit)
	var got *queryOptions
	chain := &blockOptionsChain{options: func(options []QueryOption) {
		got = defaultQueryOptions()
		got.applyQueryOptions(options...)
	}}
	ro := &rescanOptions{
		quit:         quit,
		queryOptions: []QueryOption{Timeout(time.Second)},
	}
	_, _ = extractBlockMatches(chain, ro, &waddrmgr.BlockStamp{})
	if got == nil || got.quit == nil || got.timeout != time.Second {
		t.Fatalf("%+v", got)
	}

	got = nil
	_, _ = blockFilterMatches(chain, ro, &chainhash.Hash{})
	if got == nil || got.quit == nil || got.optimisticBatch == noBatch {
		t.Fatalf("%+v", got)
	}

	got = nil
	if len((&rescanOptions{}).withQuit(Timeout(time.Second))) != 1 {
		t.Fatal("added a quit option for a rescan with none")
	}
}

// blockOptionsChain tells what a block was asked for with.
type blockOptionsChain struct {
	ChainSource
	options func([]QueryOption)
}

func (c *blockOptionsChain) GetBlock(_ chainhash.Hash,
	options ...QueryOption) (*bchutil.Block, error) {

	c.options(options)
	return nil, errors.New("no block")
}

func (c *blockOptionsChain) GetCFilter(_ chainhash.Hash, _ wire.FilterType,
	options ...QueryOption) (*gcs.Filter, error) {

	c.options(options)
	return nil, errors.New("no filter")
}

// TestFetchBlockTimeout ensures a block gets the longer timeout, unless the caller
// asks for its own.
func TestFetchBlockTimeout(t *testing.T) {
	var timeout time.Duration
	s := &ChainService{queryPeers: func(_ wire.Message, _ func(*ServerPeer,
		wire.Message, chan<- struct{}), options ...QueryOption) {

		qo := defaultQueryOptions()
		qo.applyQueryOptions(options...)
		timeout = qo.timeout
	}}
	inv := wire.NewInvVect(wire.InvTypeBlock, &chainhash.Hash{})
	if _, err := s.fetchBlock(inv, 1); err == nil {
		t.Fatal("expected no block")
	}
	if timeout != QueryBatchTimeout {
		t.Fatalf("timeout %v", timeout)
	}
	if _, err := s.fetchBlock(inv, 1, Timeout(time.Second)); err == nil {
		t.Fatal("expected no block")
	}
	if timeout != time.Second {
		t.Fatalf("timeout %v", timeout)
	}
}

// silentPeers returns a service with connected peers that never answer, the first
// of them the sync peer.
func silentPeers(t *testing.T, count int) (*ChainService, []*ServerPeer) {
	t.Helper()
	return silentPeersLater(t, count, 0)
}

// silentPeersLater is silentPeers, with no peer yet for the first askings.
func silentPeersLater(t *testing.T, count, askings int) (*ChainService, []*ServerPeer) {
	t.Helper()
	s := &ChainService{
		quit:         make(chan struct{}),
		query:        make(chan interface{}),
		blockManager: &blockManager{},
	}
	peers := make([]*ServerPeer, 0, count)
	for i := range count {
		sp := newServerPeer(s, false)
		var err error
		sp.Peer, err = peer.NewOutboundPeer(
			&peer.Config{ChainParams: &chaincfg.SimNetParams},
			fmt.Sprintf("127.0.0.1:%d", 18000+i),
		)
		if err != nil {
			t.Fatal(err)
		}
		local, remote := net.Pipe()
		sp.associate(local)
		go func() { _, _ = io.Copy(io.Discard, remote) }()
		t.Cleanup(func() {
			sp.Disconnect()
			_ = remote.Close()
		})
		peers = append(peers, sp)
	}
	s.blockManager.syncPeer = peers[0]
	go func() {
		for {
			select {
			case query := <-s.query:
				if askings > 0 {
					askings--
					query.(getPeersMsg).reply <- nil
					continue
				}
				query.(getPeersMsg).reply <- peers
			case <-s.quit:
				return
			}
		}
	}()
	t.Cleanup(func() { close(s.quit) })
	return s, peers
}

// TestQueryTriesEachSilentPeer ensures each peer gets a timeout of its own, and that
// the timeout for having no peer doesn't end a query that has one.  The query can't
// end before its timeouts, and the limit is far below a turn left to its deadline.
func TestQueryTriesEachSilentPeer(t *testing.T) {
	t.Parallel()

	s, _ := silentPeers(t, 2)
	timeout := time.Second
	start := time.Now()
	queryChainServicePeers(
		s, wire.NewMsgPing(1),
		func(*ServerPeer, wire.Message, chan<- struct{}) {},
		Timeout(timeout), NumRetries(1),
		PeerConnectTimeout(time.Millisecond),
	)
	elapsed := time.Since(start)
	if elapsed < 2*timeout {
		t.Fatalf("a query ended after %v, without the last peer's turn",
			elapsed)
	}
	if elapsed > maxPeerQueryTimeouts*timeout*8/10 {
		t.Fatalf("a query took %v, as a silent peer had no timeout",
			elapsed)
	}
}

// TestQueryAsksWithoutSyncPeer ensures a query with no sync peer asks a connected
// peer at once, rather than after a timeout.
func TestQueryAsksWithoutSyncPeer(t *testing.T) {
	t.Parallel()

	s, peers := silentPeers(t, 1)
	s.blockManager.syncPeer = nil
	quit := make(chan struct{})
	done := make(chan struct{})
	go func() {
		queryChainServicePeers(
			s, wire.NewMsgPing(1),
			func(*ServerPeer, wire.Message, chan<- struct{}) {},
			Timeout(time.Hour), QueryQuit(quit),
		)
		close(done)
	}()
	defer func() {
		close(quit)
		<-done
	}()
	deadline := time.Now().Add(time.Minute)
	for {
		peers[0].mtxSubscribers.RLock()
		asked := len(peers[0].recvSubscribers) != 0
		peers[0].mtxSubscribers.RUnlock()
		if asked {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("no peer was asked")
		}
		time.Sleep(time.Millisecond)
	}
}

// TestQueryFindsPeerWithoutSyncPeer ensures a query that started with no peer to
// ask isn't ended by that timeout once it has found one.
func TestQueryFindsPeerWithoutSyncPeer(t *testing.T) {
	t.Parallel()

	s, _ := silentPeersLater(t, 2, 1)
	s.blockManager.syncPeer = nil
	timeout := time.Second
	start := time.Now()
	queryChainServicePeers(
		s, wire.NewMsgPing(1),
		func(*ServerPeer, wire.Message, chan<- struct{}) {},
		Timeout(timeout), NumRetries(1),
		PeerConnectTimeout(timeout*3/2),
	)
	if elapsed := time.Since(start); elapsed < 3*timeout {
		t.Fatalf("a query ended after %v, during a peer's turn", elapsed)
	}
}

// TestQueryLooksForPeerLastThing ensures a peer that connected while a query waited
// for one is asked, rather than the query ending as its time for that ran out.
func TestQueryLooksForPeerLastThing(t *testing.T) {
	t.Parallel()

	s, _ := silentPeersLater(t, 1, 1)
	s.blockManager.syncPeer = nil
	timeout := time.Second
	start := time.Now()
	queryChainServicePeers(
		s, wire.NewMsgPing(1),
		func(*ServerPeer, wire.Message, chan<- struct{}) {},
		Timeout(timeout), NumRetries(1),
		PeerConnectTimeout(timeout/2),
	)
	if elapsed := time.Since(start); elapsed < timeout*3/2 {
		t.Fatalf("a query ended after %v, without the peer's whole turn",
			elapsed)
	}
}

// TestQueryWaitsForFirstPeer ensures a query with no peer yet keeps looking for one
// for its time to connect, and ends then if there still is none.
func TestQueryWaitsForFirstPeer(t *testing.T) {
	t.Parallel()

	// There's no peer at the start or the first timeout, and one at the next,
	// which then has its turn.
	s, _ := silentPeersLater(t, 1, 2)
	s.blockManager.syncPeer = nil
	timeout := 2 * time.Second
	start := time.Now()
	queryChainServicePeers(
		s, wire.NewMsgPing(1),
		func(*ServerPeer, wire.Message, chan<- struct{}) {},
		Timeout(timeout), NumRetries(1),
		PeerConnectTimeout(timeout*5/2),
	)
	if elapsed := time.Since(start); elapsed < 3*timeout {
		t.Fatalf("a query ended after %v without waiting for a peer",
			elapsed)
	}

	s, _ = silentPeersLater(t, 1, 1000)
	s.blockManager.syncPeer = nil
	timeout = 100 * time.Millisecond
	start = time.Now()
	queryChainServicePeers(
		s, wire.NewMsgPing(1),
		func(*ServerPeer, wire.Message, chan<- struct{}) {},
		Timeout(timeout), PeerConnectTimeout(5*timeout),
	)
	if elapsed := time.Since(start); elapsed < 5*timeout {
		t.Fatalf("a query with no peer ended after %v", elapsed)
	}
}

// TestQueryMovesOnWhenPeerLacks ensures a peer that says it doesn't have what's asked
// for loses its turn at once, and that a word on something else doesn't count.
func TestQueryMovesOnWhenPeerLacks(t *testing.T) {
	t.Parallel()

	s, peers := silentPeers(t, 2)
	wanted := wire.NewInvVect(wire.InvTypeBlock, &chainhash.Hash{1})
	other := wire.NewInvVect(wire.InvTypeBlock, &chainhash.Hash{2})
	quit := make(chan struct{})
	done := make(chan struct{})
	go func() {
		queryChainServicePeers(
			s, wire.NewMsgPing(1),
			func(*ServerPeer, wire.Message, chan<- struct{}) {},
			Timeout(time.Hour), NumRetries(1), QueryQuit(quit),
			peerLacks(func(msg wire.Message) bool {
				return notFoundOf(msg, wanted)
			}),
		)
		close(done)
	}()
	defer func() {
		close(quit)
		<-done
	}()

	subscription := func(sp *ServerPeer) *spMsgSubscription {
		sp.mtxSubscribers.RLock()
		defer sp.mtxSubscribers.RUnlock()
		for found := range sp.recvSubscribers {
			return &found
		}
		return nil
	}
	wait := func(sp *ServerPeer) *spMsgSubscription {
		t.Helper()
		deadline := time.Now().Add(time.Minute)
		for {
			if found := subscription(sp); found != nil {
				return found
			}
			if time.Now().After(deadline) {
				t.Fatal("the peer wasn't asked")
			}
			time.Sleep(time.Millisecond)
		}
	}
	notFound := func(inv *wire.InvVect) *wire.MsgNotFound {
		msg := wire.NewMsgNotFound()
		if err := msg.AddInvVect(inv); err != nil {
			t.Fatal(err)
		}
		return msg
	}

	// The channel takes messages one at a time, so a word has been dealt with
	// once the next is taken, and a turn that it ended moves on among these.
	first := wait(peers[0])
	settle := func() {
		for range 200 {
			first.msgChan <- spMsg{sp: peers[0], msg: wire.NewMsgPing(2)}
		}
	}
	first.msgChan <- spMsg{sp: peers[0], msg: notFound(other)}
	settle()
	if subscription(peers[1]) != nil {
		t.Fatal("a word on another block ended the turn")
	}

	// What the last peer said doesn't count against this one, shown here by
	// a word from a peer that isn't being asked.
	first.msgChan <- spMsg{sp: peers[1], msg: notFound(wanted)}
	settle()
	if subscription(peers[1]) != nil {
		t.Fatal("another peer's word ended the turn")
	}

	first.msgChan <- spMsg{sp: peers[0], msg: notFound(wanted)}
	wait(peers[1])
}

// TestFetchBlockPeerLacks ensures a block's query counts a notfound for that block.
func TestFetchBlockPeerLacks(t *testing.T) {
	var lacks func(wire.Message) bool
	s := &ChainService{queryPeers: func(_ wire.Message, _ func(*ServerPeer,
		wire.Message, chan<- struct{}), options ...QueryOption) {

		qo := defaultQueryOptions()
		qo.applyQueryOptions(options...)
		lacks = qo.peerLacks
	}}
	inv := wire.NewInvVect(wire.InvTypeBlock, &chainhash.Hash{1})
	_, _ = s.fetchBlock(inv, 1)
	if lacks == nil {
		t.Fatal("no check")
	}
	msg := wire.NewMsgNotFound()
	if err := msg.AddInvVect(inv); err != nil {
		t.Fatal(err)
	}
	if !lacks(msg) || lacks(wire.NewMsgNotFound()) || lacks(wire.NewMsgPing(1)) {
		t.Fatal("wrong answer")
	}
}

// TestQueryEndsChattyPeersTurn ensures a peer that keeps sending messages that
// don't answer the query doesn't keep it.
func TestQueryEndsChattyPeersTurn(t *testing.T) {
	t.Parallel()

	s, peers := silentPeers(t, 1)
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for {
			peers[0].mtxSubscribers.RLock()
			subscriptions := make([]spMsgSubscription, 0, 1)
			for subscription := range peers[0].recvSubscribers {
				subscriptions = append(subscriptions, subscription)
			}
			peers[0].mtxSubscribers.RUnlock()
			for _, subscription := range subscriptions {
				select {
				case subscription.msgChan <- spMsg{
					sp:  peers[0],
					msg: wire.NewMsgPing(1),
				}:
				case <-subscription.quitChan:
				case <-stop:
					return
				}
			}
			select {
			case <-time.After(time.Millisecond):
			case <-stop:
				return
			}
		}
	}()

	done := make(chan struct{})
	go func() {
		queryChainServicePeers(
			s, wire.NewMsgPing(1),
			func(*ServerPeer, wire.Message, chan<- struct{}) {},
			Timeout(100*time.Millisecond), NumRetries(1),
		)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Minute):
		t.Fatal("a peer's messages kept the query going")
	}
}

// TestQueryTurnIgnoresLastPeer ensures what a peer whose turn is over still sends
// doesn't keep the next peer's turn going, which ends after its timeout rather than
// at its deadline, ten timeouts on.
func TestQueryTurnIgnoresLastPeer(t *testing.T) {
	t.Parallel()

	s, peers := silentPeers(t, 2)
	timeout := 300 * time.Millisecond
	done := make(chan struct{})
	go func() {
		queryChainServicePeers(
			s, wire.NewMsgPing(1),
			func(*ServerPeer, wire.Message, chan<- struct{}) {},
			Timeout(timeout), NumRetries(1),
		)
		close(done)
	}()

	// Wait for the second peer's turn.
	var second *spMsgSubscription
	for second == nil {
		peers[1].mtxSubscribers.RLock()
		for found := range peers[1].recvSubscribers {
			second = &found
		}
		peers[1].mtxSubscribers.RUnlock()
		select {
		case <-done:
			t.Fatal("the query ended before the second peer's turn")
		case <-time.After(time.Millisecond):
		}
	}
	start := time.Now()
	for {
		select {
		case second.msgChan <- spMsg{sp: peers[0], msg: wire.NewMsgPing(2)}:
		case <-done:
			if took := time.Since(start); took > 5*timeout {
				t.Fatalf("the first peer's messages kept the second's turn "+
					"for %v", took)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestQueryLateWordOnEarlierTry ensures a peer's word that it lacks what was asked,
// coming in its second try after its first timed out, doesn't end the second, which
// TestQueryLateWordOnEarlierTry ensures a peer's word that it lacks what was asked
// ends its turn only if it can be the word on that try: with a peer's tries one
// after another, its word on the first can come in the second, which goes on,
// while after another peer's turn, its first word ends its next try, as a word
// on its earlier try came while nothing listened.
func TestQueryLateWordOnEarlierTry(t *testing.T) {
	t.Parallel()

	wanted := wire.NewInvVect(wire.InvTypeBlock, &chainhash.Hash{1})
	timeout := time.Second
	notFound := wire.NewMsgNotFound()
	if err := notFound.AddInvVect(wanted); err != nil {
		t.Fatal(err)
	}
	query := func(s *ChainService) chan struct{} {
		done := make(chan struct{})
		go func() {
			queryChainServicePeers(
				s, wire.NewMsgPing(1),
				func(*ServerPeer, wire.Message, chan<- struct{}) {},
				Timeout(timeout), NumRetries(2),
				peerLacks(func(msg wire.Message) bool {
					return notFoundOf(msg, wanted)
				}),
			)
			close(done)
		}()
		return done
	}
	waitFor := func(sp *ServerPeer) *spMsgSubscription {
		t.Helper()
		deadline := time.Now().Add(time.Minute)
		for {
			sp.mtxSubscribers.RLock()
			for found := range sp.recvSubscribers {
				sp.mtxSubscribers.RUnlock()
				return &found
			}
			sp.mtxSubscribers.RUnlock()
			if time.Now().After(deadline) {
				t.Fatal("the peer wasn't asked")
			}
			time.Sleep(time.Millisecond)
		}
	}

	t.Run("one after another", func(t *testing.T) {
		s, peers := silentPeers(t, 1)
		done := query(s)

		// The first try times out, and halfway through the second, the
		// word on the first comes in, which leaves the second to go on.
		sub := waitFor(peers[0])
		time.Sleep(timeout + timeout/2)
		sub.msgChan <- spMsg{sp: peers[0], msg: notFound}
		select {
		case <-done:
			t.Fatal("a word on the first try ended the second")
		case <-time.After(timeout / 4):
		}

		// Its word on the second try ends it.
		sub.msgChan <- spMsg{sp: peers[0], msg: notFound}
		<-done
	})

	t.Run("after another peer", func(t *testing.T) {
		s, peers := silentPeers(t, 2)
		done := query(s)

		// The first peer's and the second's first tries time out. The first
		// peer's word on its second try ends it at once.
		waitFor(peers[0])
		waitFor(peers[1])
		second := waitFor(peers[0])
		asked := time.Now()
		second.msgChan <- spMsg{sp: peers[0], msg: notFound}
		waitFor(peers[1])
		if took := time.Since(asked); took >= timeout {
			t.Fatalf("the word took %v to end the turn", took)
		}
		<-done
	})
}

// TestQueryDropsSilentPeer ensures a peer that lets maxSilentQueryTries query
// tries in a row time out without sending a byte is disconnected, as its
// connection is most likely gone, and that one sending a message slowly, which
// isn't whole when the tries time out, isn't. A phone's peer whose connection
// went when the phone changed networks held its queries until the peer's idle
// timeout of 5 minutes.
func TestQueryDropsSilentPeer(t *testing.T) {
	t.Parallel()

	// Wide, as a slow machine's timers fire late: a query one try short is
	// silent a whole timeout less than the limit.
	timeout := 300 * time.Millisecond
	query := func(s *ChainService) {
		queryChainServicePeers(
			s, wire.NewMsgPing(1),
			func(*ServerPeer, wire.Message, chan<- struct{}) {},
			Timeout(timeout), NumRetries(maxSilentQueryTries),
		)
	}

	t.Run("silent", func(t *testing.T) {
		// The query disconnects it before it returns, which is long
		// before the peer's handshake timeout would.
		s, peers := silentPeers(t, 1)
		query(s)
		if peers[0].Connected() {
			t.Fatal("the silent peer is still connected")
		}
	})

	t.Run("one try short", func(t *testing.T) {
		s, peers := silentPeers(t, 1)
		peers[0].lastRead.Store(time.Now().UnixNano())
		queryChainServicePeers(
			s, wire.NewMsgPing(1),
			func(*ServerPeer, wire.Message, chan<- struct{}) {},
			Timeout(timeout), NumRetries(maxSilentQueryTries-1),
		)
		if !peers[0].Connected() {
			t.Fatal("a peer silent for fewer tries was disconnected")
		}
	})

	t.Run("concurrent", func(t *testing.T) {
		// Queries that time out on the peer at once, as for what it
		// doesn't have yet, count as one timeout of silence.
		s, peers := silentPeers(t, 1)
		peers[0].lastRead.Store(time.Now().UnixNano())
		var wg sync.WaitGroup
		for range maxSilentQueryTries {
			wg.Add(1)
			go func() {
				defer wg.Done()
				queryChainServicePeers(
					s, wire.NewMsgPing(1),
					func(*ServerPeer, wire.Message, chan<- struct{}) {},
					Timeout(timeout), NumRetries(1),
				)
			}()
		}
		wg.Wait()
		if !peers[0].Connected() {
			t.Fatal("queries that timed out at once disconnected the peer")
		}
	})

	t.Run("slow", func(t *testing.T) {
		s := &ChainService{
			quit:         make(chan struct{}),
			query:        make(chan interface{}),
			blockManager: &blockManager{},
		}
		t.Cleanup(func() { close(s.quit) })
		sp := newServerPeer(s, false)
		var err error
		sp.Peer, err = peer.NewOutboundPeer(
			&peer.Config{ChainParams: &chaincfg.SimNetParams},
			"127.0.0.1:18200",
		)
		if err != nil {
			t.Fatal(err)
		}
		local, remote := net.Pipe()
		sp.associate(local)
		t.Cleanup(func() {
			sp.Disconnect()
			_ = remote.Close()
		})
		go func() { _, _ = io.Copy(io.Discard, remote) }()
		s.blockManager.syncPeer = sp
		go func() {
			for {
				select {
				case query := <-s.query:
					query.(getPeersMsg).reply <- []*ServerPeer{sp}
				case <-s.quit:
					return
				}
			}
		}()

		// The header of a large block, then a byte of it now and then,
		// as over a slow link, so the peer never has all of it.
		var header bytes.Buffer
		_ = binary.Write(&header, binary.LittleEndian, uint32(chaincfg.SimNetParams.Net))
		command := make([]byte, 12)
		copy(command, wire.CmdBlock)
		header.Write(command)
		_ = binary.Write(&header, binary.LittleEndian, uint32(1<<20))
		header.Write(make([]byte, 4))
		stop := make(chan struct{})
		sent := make(chan struct{})
		go func() {
			defer close(sent)
			if _, err := remote.Write(header.Bytes()); err != nil {
				return
			}
			for {
				select {
				case <-stop:
					return
				case <-time.After(timeout / 4):
				}
				if _, err := remote.Write([]byte{0}); err != nil {
					return
				}
			}
		}()
		query(s)
		close(stop)
		<-sent
		if !sp.Connected() {
			t.Fatal("a peer slowly sending a message was disconnected")
		}
	})
}

// TestDisconnectPeers ensures DisconnectPeers drops every peer.
func TestDisconnectPeers(t *testing.T) {
	t.Parallel()

	s, peers := silentPeers(t, 3)
	s.DisconnectPeers()
	for i, sp := range peers {
		if sp.Connected() {
			t.Errorf("peer %d is still connected", i)
		}
	}
}

// TestQueriesEndAtQueryQuit ensures the query of every peer and the batch query end
// when the caller gives up, as the query of one peer at a time does, rather than at
// their timeouts.
func TestQueriesEndAtQueryQuit(t *testing.T) {
	t.Parallel()

	queries := map[string]func(s *ChainService, quit <-chan struct{}){
		"every peer": func(s *ChainService, quit <-chan struct{}) {
			s.queryAllPeers(wire.NewMsgPing(1),
				func(*ServerPeer, wire.Message, chan<- struct{}, chan<- struct{}) {},
				Timeout(time.Hour), QueryQuit(quit))
		},
		"batch": func(s *ChainService, quit <-chan struct{}) {
			queryChainServiceBatch(s, []wire.Message{wire.NewMsgPing(1)},
				func(*ServerPeer, wire.Message, wire.Message) bool { return false },
				nil, Timeout(time.Hour), QueryQuit(quit))
		},
	}
	for name, query := range queries {
		s, _ := silentPeers(t, 2)
		quit := make(chan struct{})
		done := make(chan struct{})
		go func() {
			query(s, quit)
			close(done)
		}()
		time.Sleep(100 * time.Millisecond)
		close(quit)
		select {
		case <-done:
		case <-time.After(time.Minute):
			t.Fatalf("the %s query didn't end when the caller gave up", name)
		}
	}
}

// TestQueryTakesTurnsAmongPeers ensures each peer has a try before any has a second,
// the sync peer first, wherever the list has it, so that a peer that keeps a query
// for its whole turn doesn't keep it for all its tries before another is asked.
func TestQueryTakesTurnsAmongPeers(t *testing.T) {
	t.Parallel()

	for syncPeer, want := range [][]int{{0, 1, 0, 1}, {1, 0, 1, 0}} {
		if order := peerTurnOrder(t, syncPeer); !slices.Equal(order, want) {
			t.Fatalf("with peer %d the sync peer, peers asked in the order %v, "+
				"want %v", syncPeer, order, want)
		}
	}
}

// peerTurnOrder returns the order in which two peers that never answer are asked,
// with two tries each, the one at syncPeer the sync peer.
func peerTurnOrder(t *testing.T, syncPeer int) []int {
	t.Helper()
	s := &ChainService{
		quit:         make(chan struct{}),
		query:        make(chan interface{}),
		blockManager: &blockManager{},
	}
	asked := make(chan int, 16)
	var peers []*ServerPeer
	for i := range 2 {
		sp := newServerPeer(s, false)
		var err error
		sp.Peer, err = peer.NewOutboundPeer(
			&peer.Config{ChainParams: &chaincfg.SimNetParams},
			fmt.Sprintf("127.0.0.1:%d", 18100+10*syncPeer+i),
		)
		if err != nil {
			t.Fatal(err)
		}
		local, remote := net.Pipe()
		sp.associate(local)
		go func() {
			net := chaincfg.SimNetParams.Net
			for {
				msg, _, err := wire.ReadMessage(remote, wire.ProtocolVersion, net)
				if err != nil {
					return
				}
				switch msg := msg.(type) {
				case *wire.MsgVersion:
					// Answer the handshake, so that the peer sends what's
					// queued.
					reply := wire.NewMsgVersion(&msg.AddrMe, &msg.AddrYou,
						msg.Nonce+1, 0)
					reply.ProtocolVersion = int32(wire.ProtocolVersion)
					if wire.WriteMessage(remote, reply, wire.ProtocolVersion, net) != nil ||
						wire.WriteMessage(remote, wire.NewMsgVerAck(),
							wire.ProtocolVersion, net) != nil {

						return
					}
				case *wire.MsgPing:
					asked <- i
				}
			}
		}()
		t.Cleanup(func() {
			sp.Disconnect()
			_ = remote.Close()
		})
		peers = append(peers, sp)
	}
	s.blockManager.syncPeer = peers[syncPeer]
	go func() {
		for {
			select {
			case query := <-s.query:
				query.(getPeersMsg).reply <- peers
			case <-s.quit:
				return
			}
		}
	}()
	t.Cleanup(func() { close(s.quit) })

	queryChainServicePeers(
		s, wire.NewMsgPing(1),
		func(*ServerPeer, wire.Message, chan<- struct{}) {},
		Timeout(200*time.Millisecond), NumRetries(2),
		PeerConnectTimeout(time.Millisecond),
	)
	var order []int
	for len(order) < 4 {
		select {
		case i := <-asked:
			order = append(order, i)
		case <-time.After(time.Minute):
			t.Fatalf("peers asked %v", order)
		}
	}
	return order
}
