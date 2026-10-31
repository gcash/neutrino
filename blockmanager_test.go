package neutrino

import (
	"container/list"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/gcash/bchd/blockchain"
	"github.com/gcash/bchd/peer"
	"github.com/gcash/bchd/txscript"
	"github.com/gcash/bchutil"
	"github.com/gcash/bchutil/gcs"
	"github.com/gcash/neutrino/banman"

	"github.com/gcash/bchd/chaincfg"
	"github.com/gcash/bchd/chaincfg/chainhash"
	"github.com/gcash/bchd/wire"
	"github.com/gcash/bchutil/gcs/builder"
	"github.com/gcash/bchwallet/walletdb"
	"github.com/gcash/neutrino/blockntfns"
	"github.com/gcash/neutrino/cache/lru"
	"github.com/gcash/neutrino/filterdb"
	"github.com/gcash/neutrino/headerfs"
)

// maxHeight is the height we will generate filter headers up to. We use an odd
// number of checkpoints to ensure we can test cases where the block manager is
// only able to fetch filter headers for one checkpoint interval rather than
// two.
const maxHeight = 21 * uint32(wire.CFCheckptInterval)

// setupBlockManager initialises a blockManager to be used in tests.
func setupBlockManager() (*blockManager, headerfs.BlockHeaderStore,
	*headerfs.FilterHeaderStore, func(), error) {

	// Set up the block and filter header stores.
	tempDir, err := os.MkdirTemp("", "neutrino")
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("Failed to create "+
			"temporary directory: %s", err)
	}

	db, err := walletdb.Create("bdb", tempDir+"/weks.db", true)
	if err != nil {
		_ = os.RemoveAll(tempDir)
		return nil, nil, nil, nil, fmt.Errorf("Error opening DB: %s",
			err)
	}

	cleanUp := func() {
		_ = db.Close()
		_ = os.RemoveAll(tempDir)
	}

	hdrStore, err := headerfs.NewBlockHeaderStore(
		tempDir, db, &chaincfg.SimNetParams,
	)
	if err != nil {
		cleanUp()
		return nil, nil, nil, nil, fmt.Errorf("Error creating block "+
			"header store: %s", err)
	}

	cfStore, err := headerfs.NewFilterHeaderStore(
		tempDir, db, headerfs.RegularFilter, &chaincfg.SimNetParams,
		nil,
	)
	if err != nil {
		cleanUp()
		return nil, nil, nil, nil, fmt.Errorf("Error creating filter "+
			"header store: %s", err)
	}

	banStore, err := banman.NewStore(db)
	if err != nil {
		cleanUp()
		return nil, nil, nil, nil, fmt.Errorf("unable to initialize "+
			"ban store: %v", err)
	}

	// Set up a chain service for the block manager. Each test should set
	// custom query methods on this chain service.
	cs := &ChainService{
		chainParams:      chaincfg.SimNetParams,
		BlockHeaders:     hdrStore,
		RegFilterHeaders: cfStore,
		banStore:         banStore,
	}

	// Set up a blockManager with the chain service we defined.
	bm, err := newBlockManager(cs, nil)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("unable to create "+
			"blockmanager: %v", err)
	}

	return bm, hdrStore, cfStore, cleanUp, nil
}

// headers wraps the different headers and filters used throughout the tests.
type headers struct {
	blockHeaders []headerfs.BlockHeader
	cfHeaders    []headerfs.FilterHeader
	checkpoints  []*chainhash.Hash
	filterHashes []chainhash.Hash
}

// generateHeaders generates block headers, filter header and hashes, and
// checkpoints from the given genesis. The onCheckpoint method will be called
// with the current cf header on each checkpoint to modify the derivation of
// the next interval
func generateHeaders(genesisBlockHeader *wire.BlockHeader,
	genesisFilterHeader *chainhash.Hash,
	onCheckpoint func(*chainhash.Hash)) (*headers, error) {

	var blockHeaders []headerfs.BlockHeader
	blockHeaders = append(blockHeaders, headerfs.BlockHeader{
		BlockHeader: genesisBlockHeader,
		Height:      0,
	})

	var cfHeaders []headerfs.FilterHeader
	cfHeaders = append(cfHeaders, headerfs.FilterHeader{
		HeaderHash: genesisBlockHeader.BlockHash(),
		FilterHash: *genesisFilterHeader,
		Height:     0,
	})

	// The filter hashes (not the filter headers!) will be sent as
	// part of the CFHeaders response, so we also keep track of
	// them.
	genesisFilter, err := builder.BuildBasicFilter(chaincfg.SimNetParams.GenesisBlock)
	if err != nil {
		return nil, fmt.Errorf("unable to build genesis filter: %v",
			err)
	}

	genesisFilterHash, err := builder.GetFilterHash(genesisFilter)
	if err != nil {
		return nil, fmt.Errorf("unable to get genesis filter hash: %v",
			err)
	}

	var filterHashes []chainhash.Hash
	filterHashes = append(filterHashes, genesisFilterHash)

	// Also keep track of the current filter header. We use this to
	// calculate the next filter header, as it commits to the
	// previous.
	currentCFHeader := *genesisFilterHeader

	// checkpoints will be the checkpoints passed to
	// getCheckpointedCFHeaders.
	var checkpoints []*chainhash.Hash

	for height := uint32(1); height <= maxHeight; height++ {
		header := heightToHeader(height)
		blockHeader := headerfs.BlockHeader{
			BlockHeader: header,
			Height:      height,
		}

		blockHeaders = append(blockHeaders, blockHeader)

		// It doesn't really matter what filter the filter
		// header commit to, so just use the height as a nonce
		// for the filters.
		filterHash := chainhash.Hash{}
		binary.BigEndian.PutUint32(filterHash[:], height)
		filterHashes = append(filterHashes, filterHash)

		// Calculate the current filter header, and add to our
		// slice.
		currentCFHeader = chainhash.DoubleHashH(
			append(filterHash[:], currentCFHeader[:]...),
		)
		cfHeaders = append(cfHeaders, headerfs.FilterHeader{
			HeaderHash: header.BlockHash(),
			FilterHash: currentCFHeader,
			Height:     height,
		})

		// Each interval we must record a checkpoint.
		if height%wire.CFCheckptInterval == 0 {
			// We must make a copy of the current header to
			// avoid mutation.
			cfh := currentCFHeader
			checkpoints = append(checkpoints, &cfh)

			if onCheckpoint != nil {
				onCheckpoint(&currentCFHeader)
			}
		}
	}

	return &headers{
		blockHeaders: blockHeaders,
		cfHeaders:    cfHeaders,
		checkpoints:  checkpoints,
		filterHashes: filterHashes,
	}, nil
}

// generateResponses generates the MsgCFHeaders messages from the given queries
// and headers.
func generateResponses(msgs []wire.Message,
	headers *headers) ([]*wire.MsgCFHeaders, error) {

	// Craft a response for each message.
	var responses []*wire.MsgCFHeaders
	for _, msg := range msgs {
		// Only GetCFHeaders expected.
		q, ok := msg.(*wire.MsgGetCFHeaders)
		if !ok {
			return nil, fmt.Errorf("got unexpected message %T",
				msg)
		}

		// The start height must be set to a checkpoint height+1.
		if q.StartHeight%wire.CFCheckptInterval != 1 {
			return nil, fmt.Errorf("unexpexted start height %v",
				q.StartHeight)
		}

		var prevFilterHeader chainhash.Hash
		switch q.StartHeight {

		// If the start height is 1 the prevFilterHeader is set to the
		// genesis header.
		case 1:
			genesisFilterHeader := headers.cfHeaders[0].FilterHash
			prevFilterHeader = genesisFilterHeader

		// Otherwise we use one of the created checkpoints.
		default:
			j := q.StartHeight/wire.CFCheckptInterval - 1
			prevFilterHeader = *headers.checkpoints[j]
		}

		resp := &wire.MsgCFHeaders{
			FilterType:       q.FilterType,
			StopHash:         q.StopHash,
			PrevFilterHeader: prevFilterHeader,
		}

		// Keep adding filter hashes until we reach the stop hash.
		for h := q.StartHeight; ; h++ {
			resp.FilterHashes = append(
				resp.FilterHashes, &headers.filterHashes[h],
			)

			blockHash := headers.blockHeaders[h].BlockHash()
			if blockHash == q.StopHash {
				break
			}
		}

		responses = append(responses, resp)
	}

	return responses, nil
}

// TestBlockManagerInitialInterval tests that the block manager is able to
// handle checkpointed filter header query responses in out of order, and when
// a partial interval is already written to the store.
func TestBlockManagerInitialInterval(t *testing.T) {
	t.Parallel()

	type testCase struct {
		// permute indicates whether responses should be permutated.
		permute bool

		// partialInterval indicates whether we should write parts of
		// the first checkpoint interval to the filter header store
		// before starting the test.
		partialInterval bool

		// repeat indicates whether responses should be repeated.
		repeat bool
	}

	// Generate all combinations of testcases.
	var testCases []testCase
	b := []bool{false, true}
	for _, perm := range b {
		for _, part := range b {
			for _, rep := range b {
				testCases = append(testCases, testCase{
					permute:         perm,
					partialInterval: part,
					repeat:          rep,
				})
			}
		}
	}

	for _, test := range testCases {
		testDesc := fmt.Sprintf("permute=%v, partial=%v, repeat=%v",
			test.permute, test.partialInterval, test.repeat)

		bm, hdrStore, cfStore, cleanUp, err := setupBlockManager()
		if err != nil {
			t.Fatalf("unable to set up ChainService: %v", err)
		}
		defer cleanUp()

		// Keep track of the filter headers and block headers. Since
		// the genesis headers are written automatically when the store
		// is created, we query it to add to the slices.
		genesisBlockHeader, _, err := hdrStore.ChainTip()
		if err != nil {
			t.Fatal(err)
		}

		genesisFilterHeader, _, err := cfStore.ChainTip()
		if err != nil {
			t.Fatal(err)
		}

		headers, err := generateHeaders(genesisBlockHeader,
			genesisFilterHeader, nil)
		if err != nil {
			t.Fatalf("unable to generate headers: %v", err)
		}

		// Write all block headers but the genesis, since it is already
		// in the store.
		if err = hdrStore.WriteHeaders(headers.blockHeaders[1:]...); err != nil {
			t.Fatalf("Error writing batch of headers: %s", err)
		}

		// We emulate the case where a few filter headers are already
		// written to the store by writing 1/3 of the first interval.
		if test.partialInterval {
			err = cfStore.WriteHeaders(
				headers.cfHeaders[1 : wire.CFCheckptInterval/3]...,
			)
			if err != nil {
				t.Fatalf("Error writing batch of headers: %s",
					err)
			}
		}

		// We set up a custom query batch method for this test, as we
		// will use this to feed the blockmanager with our crafted
		// responses.
		bm.server.queryBatch = func(msgs []wire.Message,
			f func(*ServerPeer, wire.Message, wire.Message) bool,
			_ <-chan struct{}, _ ...QueryOption) {

			responses, err := generateResponses(msgs, headers)
			if err != nil {
				t.Fatalf("unable to generate responses: %v",
					err)
			}

			// We permute the response order if the test signals
			// that.
			perm := rand.Perm(len(responses))
			for i, v := range perm {
				index := i
				if test.permute {
					index = v
				}

				// Before sending we take a copy of the
				// message, as we cannot guarantee that it
				// won't be modified.
				r := *responses[index]

				// Let the blockmanager handle the message.
				if !f(nil, msgs[index], responses[index]) {
					t.Fatalf("got response false on "+
						"send of index %d: %v",
						index, testDesc)
				}

				// If we are not testing repeated responses, go
				// on to the next response.
				if !test.repeat {
					continue
				}

				// Otherwise resend the response we just sent.
				if !f(nil, msgs[index], &r) {
					t.Fatalf("got response false on "+
						"resend of index %d: %v",
						index, testDesc)
				}

			}
		}

		// We should expect to see notifications for each new filter
		// header being connected.
		startHeight := uint32(1)
		if test.partialInterval {
			startHeight = wire.CFCheckptInterval / 3
		}
		go func() {
			for i := startHeight; i <= maxHeight; i++ {
				ntfn := <-bm.blockNtfnChan
				if _, ok := ntfn.(*blockntfns.Connected); !ok {
					t.Error("expected block connected " +
						"notification")
					return
				}
			}
		}()

		// Call the get checkpointed cf headers method with the
		// checkpoints we created to start the test.
		err = bm.getCheckpointedCFHeaders(
			headers.checkpoints, cfStore, wire.GCSFilterRegular,
		)
		if err != nil {
			t.Fatalf("unable to get checkpointed cfheaders: %v",
				err)
		}

		// Finally make sure the filter header tip is what we expect.
		tip, tipHeight, err := cfStore.ChainTip()
		if err != nil {
			t.Fatal(err)
		}

		if tipHeight != maxHeight {
			t.Fatalf("expected tip height to be %v, was %v",
				maxHeight, tipHeight)
		}

		lastCheckpoint := headers.checkpoints[len(headers.checkpoints)-1]
		if *tip != *lastCheckpoint {
			t.Fatalf("expected tip to be %v, was %v",
				lastCheckpoint, tip)
		}
	}
}

// TestBlockManagerInvalidInterval tests that the block manager is able to
// determine it is receiving corrupt checkpoints and filter headers.
func TestBlockManagerInvalidInterval(t *testing.T) {
	//t.Parallel()
	type testCase struct {
		// wrongGenesis indicates whether we should start deriving the
		// filters from a wrong genesis.
		wrongGenesis bool

		// intervalMisaligned indicates whether each interval prev hash
		// should not line up with the previous checkpoint.
		intervalMisaligned bool

		// invalidPrevHash indicates whether the interval responses
		// should have a prev hash that doesn't mathc that interval.
		invalidPrevHash bool

		// partialInterval indicates whether we should write parts of
		// the first checkpoint interval to the filter header store
		// before starting the test.
		partialInterval bool

		// firstInvalid is the first interval response we expect the
		// blockmanager to determine is invalid.
		firstInvalid int
	}

	testCases := []testCase{
		// With a set of checkpoints and filter headers calculated from
		// the wrong genesis, the block manager should be able to
		// determine that the first interval doesn't line up.
		{
			wrongGenesis: true,
			firstInvalid: 0,
		},

		// With checkpoints calculated from the wrong genesis, and a
		// partial set of filter headers already written, the first
		// interval response should be considered invalid.
		{
			wrongGenesis:    true,
			partialInterval: true,
			firstInvalid:    0,
		},

		// With intervals not lining up, the second interval response
		// should be determined invalid.
		{
			intervalMisaligned: true,
			firstInvalid:       0,
		},

		// With misaligned intervals and a partial interval written, the
		// second interval response should be considered invalid.
		{
			intervalMisaligned: true,
			partialInterval:    true,
			firstInvalid:       0,
		},

		// With responses having invalid prev hashes, the second
		// interval response should be deemed invalid.
		{
			invalidPrevHash: true,
			firstInvalid:    1,
		},
	}
	for _, test := range testCases {
		bm, hdrStore, cfStore, cleanUp, err := setupBlockManager()
		if err != nil {
			t.Fatalf("unable to set up ChainService: %v", err)
		}
		defer cleanUp()

		// Create a mock peer to prevent panics when attempting to ban
		// a peer that served an invalid filter header.
		mockPeer := newServerPeer(bm.server, false)
		mockPeer.Peer, err = peer.NewOutboundPeer(
			newPeerConfig(mockPeer), "127.0.0.1:8333",
		)
		if err != nil {
			t.Fatal(err)
		}

		// Keep track of the filter headers and block headers. Since
		// the genesis headers are written automatically when the store
		// is created, we query it to add to the slices.
		genesisBlockHeader, _, err := hdrStore.ChainTip()
		if err != nil {
			t.Fatal(err)
		}

		genesisFilterHeader, _, err := cfStore.ChainTip()
		if err != nil {
			t.Fatal(err)
		}
		// To emulate a full node serving us filter headers derived
		// from different genesis than what we have, we flip a bit in
		// the genesis filter header.
		if test.wrongGenesis {
			genesisFilterHeader[0] ^= 1
		}

		headers, err := generateHeaders(genesisBlockHeader,
			genesisFilterHeader,
			func(currentCFHeader *chainhash.Hash) {
				// If we are testing that each interval doesn't
				// line up properly with the previous, we flip
				// a bit in the current header before
				// calculating the next interval checkpoint.
				if test.intervalMisaligned {
					currentCFHeader[0] ^= 1
				}
			})
		if err != nil {
			t.Fatalf("unable to generate headers: %v", err)
		}

		// Write all block headers but the genesis, since it is already
		// in the store.
		if err = hdrStore.WriteHeaders(headers.blockHeaders[1:]...); err != nil {
			t.Fatalf("Error writing batch of headers: %s", err)
		}

		// We emulate the case where a few filter headers are already
		// written to the store by writing 1/3 of the first interval.
		if test.partialInterval {
			err = cfStore.WriteHeaders(
				headers.cfHeaders[1 : wire.CFCheckptInterval/3]...,
			)
			if err != nil {
				t.Fatalf("Error writing batch of headers: %s",
					err)
			}
		}

		bm.server.queryBatch = func(msgs []wire.Message,
			f func(*ServerPeer, wire.Message, wire.Message) bool,
			_ <-chan struct{}, _ ...QueryOption) {

			responses, err := generateResponses(msgs, headers)
			if err != nil {
				t.Fatalf("unable to generate responses: %v",
					err)
			}

			// Since we used the generated checkpoints when
			// creating the responses, we must flip the
			// PrevFilterHeader bit back before sending them if we
			// are checking for misaligned intervals. This to
			// ensure we don't hit the invalid prev hash case.
			if test.intervalMisaligned {
				for i := range responses {
					if i == 0 {
						continue
					}
					responses[i].PrevFilterHeader[0] ^= 1
				}
			}

			// If we are testing for intervals with invalid prev
			// hashes, we flip a bit to corrup them, regardless of
			// whether we are testing misaligned intervals.
			if test.invalidPrevHash {
				for i := range responses {
					if i == 0 {
						continue
					}
					responses[i].PrevFilterHeader[1] ^= 1
				}
			}

			// Check that the success of the callback match what we
			// expect.
			for i := range responses {
				success := f(mockPeer, msgs[i], responses[i])
				if i == test.firstInvalid {
					if success {
						t.Fatalf("expected interval "+
							"%d to be invalid", i)
					}
					break
				}

				if !success {
					t.Fatalf("expected interval %d to be "+
						"valid", i)
				}
			}
		}

		// We should expect to see notifications for each new filter
		// header being connected.
		startHeight := uint32(1)
		if test.partialInterval {
			startHeight = wire.CFCheckptInterval / 3
		}
		go func() {
			for i := startHeight; i <= maxHeight; i++ {
				ntfn := <-bm.blockNtfnChan
				if _, ok := ntfn.(*blockntfns.Connected); !ok {
					t.Error("expected block connected " +
						"notification")
					return
				}
			}
		}()

		// Start the test by calling the get checkpointed cf headers
		// method with the checkpoints we created.
		err = bm.getCheckpointedCFHeaders(
			headers.checkpoints, cfStore, wire.GCSFilterRegular,
		)
		if err != nil {
			t.Fatalf("unable to get checkpointed cfheaders: %v",
				err)
		}
	}
}

// buildNonPushScriptFilter creates a CFilter with all output scripts except all
// OP_RETURNS with push-only scripts.
//
// NOTE: this is not a valid filter, only for tests.
//
//nolint:unused // retained for future tests.
func buildNonPushScriptFilter(block *wire.MsgBlock) (*gcs.Filter, error) {
	blockHash := block.BlockHash()
	b := builder.WithKeyHash(&blockHash)

	for _, tx := range block.Transactions {
		for _, txOut := range tx.TxOut {
			// The old version of BIP-158 skipped OP_RETURNs that
			// had a push-only script.
			if txOut.PkScript[0] == txscript.OP_RETURN &&
				txscript.IsPushOnlyScript(txOut.PkScript[1:]) {
				continue
			}

			b.AddEntry(txOut.PkScript)
		}
	}

	return b.Build()
}

// buildAllPkScriptsFilter creates a CFilter with all output scripts, including
// OP_RETURNS.
//
// NOTE: this is not a valid filter, only for tests.
//
//nolint:unused // retained for future tests.
func buildAllPkScriptsFilter(block *wire.MsgBlock) (*gcs.Filter, error) {
	blockHash := block.BlockHash()
	b := builder.WithKeyHash(&blockHash)

	for _, tx := range block.Transactions {
		for _, txOut := range tx.TxOut {
			// An old version of BIP-158 included all output
			// scripts.
			b.AddEntry(txOut.PkScript)
		}
	}

	return b.Build()
}

func assertBadPeers(expBad map[string]struct{}, badPeers []string) error {
	remBad := make(map[string]struct{})
	for p := range expBad {
		remBad[p] = struct{}{}
	}
	for _, peer := range badPeers {
		_, ok := remBad[peer]
		if !ok {
			return fmt.Errorf("did not expect %v to be bad", peer)
		}
		delete(remBad, peer)
	}

	if len(remBad) != 0 {
		return fmt.Errorf("did expect more bad peers")
	}

	return nil
}

type mockQueryAccess struct {
	answers map[string]wire.Message
}

func (m *mockQueryAccess) queryAllPeers(
	_ wire.Message,
	checkResponse func(sp *ServerPeer, resp wire.Message,
		quit chan<- struct{}, peerQuit chan<- struct{}),
	_ ...QueryOption) {

	for p, resp := range m.answers {
		pp, err := peer.NewOutboundPeer(&peer.Config{}, p)
		if err != nil {
			panic(err)
		}

		sp := &ServerPeer{
			Peer: pp,
		}
		checkResponse(sp, resp, make(chan struct{}), make(chan struct{}))
	}
}

var _ QueryAccess = (*mockQueryAccess)(nil)

// TestBlockManagerDetectBadPeers checks that we detect bad peers, like peers
// not responding to our filter query, serving inconsistent filters etc.
func TestBlockManagerDetectBadPeers(t *testing.T) {
	t.Parallel()

	var (
		stopHash        = chainhash.Hash{}
		prev            = chainhash.Hash{}
		startHeight     = uint32(100)
		badIndex        = uint32(5)
		targetIndex     = startHeight + badIndex
		fType           = wire.GCSFilterRegular
		filterBytes, _  = correctFilter.NBytes()
		filterHash, _   = builder.GetFilterHash(correctFilter)
		blockHeader     = wire.BlockHeader{}
		targetBlockHash = block.BlockHash()

		peers  = []string{"good1:1", "good2:1", "bad:1", "good3:1"}
		expBad = map[string]struct{}{
			"bad:1": {},
		}
	)

	testCases := []struct {
		// filterAnswers is used by each testcase to set the anwers we
		// want each peer to respond with on filter queries.
		filterAnswers func(string, map[string]wire.Message)
	}{
		{
			// We let the "bad" peers not respond to the filter
			// query. They should be marked bad because they are
			// unresponsive. We do this to ensure peers cannot
			// only respond to us with headers, and stall our sync
			// by not responding to filter requests.
			filterAnswers: func(p string,
				answers map[string]wire.Message) {

				if strings.Contains(p, "bad") {
					return
				}

				answers[p] = wire.NewMsgCFilter(
					fType, &targetBlockHash, filterBytes,
				)
			},
		},
		{
			// We let the "bad" peers serve filters that don't hash
			// to the filter headers they have sent.
			filterAnswers: func(p string,
				answers map[string]wire.Message) {

				filterData := filterBytes
				if strings.Contains(p, "bad") {
					filterData, _ = fakeFilter1.NBytes()
				}

				answers[p] = wire.NewMsgCFilter(
					fType, &targetBlockHash, filterData,
				)
			},
		},
	}

	for _, test := range testCases {
		// Create a mock block header store. We only need to be able to
		// serve a header for the target index.
		blockHeaders := newMockBlockHeaderStore()
		blockHeaders.heights[targetIndex] = blockHeader
		cs := &ChainService{
			BlockHeaders: blockHeaders,
		}

		// We set up the mock QueryAccess to only respond according to
		// the active testcase.
		mock := &mockQueryAccess{
			answers: make(map[string]wire.Message),
		}
		for _, peer := range peers {
			test.filterAnswers(peer, mock.answers)
		}

		// For the CFHeaders, we pretend all peers responded with the same
		// filter headers.
		msg := &wire.MsgCFHeaders{
			FilterType:       fType,
			StopHash:         stopHash,
			PrevFilterHeader: prev,
		}

		for i := uint32(0); i < 2*badIndex; i++ {
			_ = msg.AddCFHash(&filterHash)
		}

		headers := make(map[string]*wire.MsgCFHeaders)
		for _, peer := range peers {
			headers[peer] = msg
		}

		bm := &blockManager{
			server:  cs,
			queries: mock,
		}

		// Now trying to detect which peers are bad, we should detect the
		// bad ones.
		badPeers, err := bm.detectBadPeers(
			headers, targetIndex, badIndex, fType,
		)
		if err != nil {
			t.Fatalf("failed to detect bad peers: %v", err)
		}

		if err := assertBadPeers(expBad, badPeers); err != nil {
			t.Fatal(err)
		}
	}
}

// chainFilterHashes returns the filter header that commits to the given filter
// hashes on top of prevHeader.
func chainFilterHashes(prevHeader chainhash.Hash,
	filterHashes []*chainhash.Hash) chainhash.Hash {

	lastHeader := prevHeader
	for _, hash := range filterHashes {
		lastHeader = chainhash.DoubleHashH(
			append(hash[:], lastHeader[:]...),
		)
	}

	return lastHeader
}

// drainBlockNtfns reads block notifications from the block manager until the
// returned function is called, so that writing filter headers doesn't block.
func drainBlockNtfns(bm *blockManager) func() {
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-bm.blockNtfnChan:
			case <-done:
				return
			}
		}
	}()

	return func() { close(done) }
}

// setupCheckpointedTest sets up a block manager with all block headers up to
// maxHeight written, and optionally part of the first checkpoint interval of
// filter headers. It returns a mock peer to hand responses from.
func setupCheckpointedTest(t *testing.T, partialInterval bool) (*blockManager,
	*headerfs.FilterHeaderStore, *headers, *ServerPeer) {

	t.Helper()

	bm, hdrStore, cfStore, cleanUp, err := setupBlockManager()
	if err != nil {
		t.Fatalf("unable to set up ChainService: %v", err)
	}
	t.Cleanup(cleanUp)

	// Create a mock peer so that the block manager can ban and disconnect
	// it.
	mockPeer := newServerPeer(bm.server, false)
	mockPeer.Peer, err = peer.NewOutboundPeer(
		newPeerConfig(mockPeer), "127.0.0.1:8333",
	)
	if err != nil {
		t.Fatal(err)
	}

	genesisBlockHeader, _, err := hdrStore.ChainTip()
	if err != nil {
		t.Fatal(err)
	}
	genesisFilterHeader, _, err := cfStore.ChainTip()
	if err != nil {
		t.Fatal(err)
	}

	headers, err := generateHeaders(
		genesisBlockHeader, genesisFilterHeader, nil,
	)
	if err != nil {
		t.Fatalf("unable to generate headers: %v", err)
	}

	err = hdrStore.WriteHeaders(headers.blockHeaders[1:]...)
	if err != nil {
		t.Fatalf("Error writing batch of headers: %s", err)
	}

	if partialInterval {
		err = cfStore.WriteHeaders(
			headers.cfHeaders[1 : wire.CFCheckptInterval/3]...,
		)
		if err != nil {
			t.Fatalf("Error writing batch of headers: %s", err)
		}
	}

	return bm, cfStore, headers, mockPeer
}

// TestBlockManagerCheckpointedWrongLength tests that a checkpointed cfheaders
// response that doesn't carry one filter hash per block in the requested
// range is rejected and its peer banned, even when the response matches the
// checkpoints, and that nothing is written to the filter header store.
func TestBlockManagerCheckpointedWrongLength(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name string

		// partialInterval indicates whether part of the first
		// checkpoint interval is already in the filter header store.
		partialInterval bool

		// lastQuery picks the last query, which spans a single
		// checkpoint interval, rather than the first one.
		lastQuery bool

		// filterHashes changes the filter hashes of the response.
		filterHashes func([]*chainhash.Hash) []*chainhash.Hash
	}{
		{
			name: "empty response",
			filterHashes: func([]*chainhash.Hash) []*chainhash.Hash {
				return nil
			},
		},
		{
			name: "short response",
			filterHashes: func(h []*chainhash.Hash) []*chainhash.Hash {
				return h[:len(h)/2]
			},
		},
		{
			name:      "long response",
			lastQuery: true,
			filterHashes: func(h []*chainhash.Hash) []*chainhash.Hash {
				extra := chainhash.DoubleHashH([]byte("extra"))
				return append(h, &extra)
			},
		},
		{
			// The response is shorter than the part of the
			// interval we already have, so the offset into it
			// would be out of range.
			name:            "shorter than initial offset",
			partialInterval: true,
			filterHashes: func(h []*chainhash.Hash) []*chainhash.Hash {
				return h[:wire.CFCheckptInterval/6]
			},
		},
		{
			name:            "empty response with initial offset",
			partialInterval: true,
			filterHashes: func([]*chainhash.Hash) []*chainhash.Hash {
				return nil
			},
		},
	}

	for _, test := range testCases {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			bm, cfStore, headers, mockPeer := setupCheckpointedTest(
				t, test.partialInterval,
			)
			defer drainBlockNtfns(bm)()

			tipBefore, heightBefore, err := cfStore.ChainTip()
			if err != nil {
				t.Fatal(err)
			}

			called := false
			bm.server.queryBatch = func(msgs []wire.Message,
				f func(*ServerPeer, wire.Message,
					wire.Message) bool,
				_ <-chan struct{}, _ ...QueryOption) {

				called = true

				responses, err := generateResponses(
					msgs, headers,
				)
				if err != nil {
					t.Errorf("unable to generate "+
						"responses: %v", err)
					return
				}

				idx := 0
				if test.lastQuery {
					idx = len(msgs) - 1
				}
				resp := responses[idx]
				resp.FilterHashes = test.filterHashes(
					resp.FilterHashes,
				)

				// Make the checkpoint at the end of the
				// query match the altered response, so it
				// passes the checkpoint check.
				cpIndex := uint32(idx) * maxCFCheckptsPerQuery
				nextIndex := cpIndex + maxCFCheckptsPerQuery - 1
				if nextIndex >= uint32(len(headers.checkpoints)) {
					nextIndex = uint32(
						len(headers.checkpoints),
					) - 1
				}
				cp := chainFilterHashes(
					resp.PrevFilterHeader,
					resp.FilterHashes,
				)
				headers.checkpoints[nextIndex] = &cp

				if f(mockPeer, msgs[idx], resp) {
					t.Errorf("expected response with %d "+
						"filter hashes to be rejected",
						len(resp.FilterHashes))
				}
			}

			err = bm.getCheckpointedCFHeaders(
				headers.checkpoints, cfStore,
				wire.GCSFilterRegular,
			)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !called {
				t.Fatal("expected cfheaders to be queried")
			}

			if !bm.server.IsBanned(mockPeer.Addr()) {
				t.Fatal("expected peer to be banned")
			}

			tip, height, err := cfStore.ChainTip()
			if err != nil {
				t.Fatal(err)
			}
			if height != heightBefore || *tip != *tipBefore {
				t.Fatalf("expected filter tip to stay at "+
					"height %v, got %v", heightBefore,
					height)
			}
		})
	}
}

// TestWriteCFHeadersMsgRejectsBadRange tests that writeCFHeadersMsg returns an
// error rather than writing when a message has no filter hashes, or when its
// filter hashes don't start right after the filter header tip.
func TestWriteCFHeadersMsgRejectsBadRange(t *testing.T) {
	t.Parallel()

	bm, cfStore, headers, _ := setupCheckpointedTest(t, false)
	defer drainBlockNtfns(bm)()

	tip, _, err := cfStore.ChainTip()
	if err != nil {
		t.Fatal(err)
	}

	filterHashes := make([]*chainhash.Hash, 3)
	for i := range filterHashes {
		filterHashes[i] = &headers.filterHashes[i+1]
	}

	testCases := []struct {
		name string
		msg  *wire.MsgCFHeaders
	}{
		{
			name: "no filter hashes",
			msg: &wire.MsgCFHeaders{
				FilterType:       wire.GCSFilterRegular,
				StopHash:         headers.blockHeaders[1].BlockHash(),
				PrevFilterHeader: *tip,
			},
		},
		{
			name: "gap after tip",
			msg: &wire.MsgCFHeaders{
				FilterType:       wire.GCSFilterRegular,
				StopHash:         headers.blockHeaders[10].BlockHash(),
				PrevFilterHeader: *tip,
				FilterHashes:     filterHashes,
			},
		},
	}

	for _, test := range testCases {
		_, err := bm.writeCFHeadersMsg(test.msg, cfStore)
		if err == nil {
			t.Fatalf("%s: expected an error", test.name)
		}

		_, height, err := cfStore.ChainTip()
		if err != nil {
			t.Fatal(err)
		}
		if height != 0 {
			t.Fatalf("%s: expected filter tip to stay at 0, "+
				"got %v", test.name, height)
		}
	}
}

// TestBlockManagerCheckpointedStoreErrors tests that getCheckpointedCFHeaders
// returns an error, rather than panicking, when it can't read a block header
// it needs or can't write a response to the filter header store, and that it
// stops the query in the latter case.
func TestBlockManagerCheckpointedStoreErrors(t *testing.T) {
	t.Parallel()

	t.Run("missing block header", func(t *testing.T) {
		t.Parallel()

		bm, cfStore, headers, _ := setupCheckpointedTest(t, false)

		// Pass one more checkpoint than we have block headers for.
		extra := chainhash.Hash{}
		checkpoints := append(headers.checkpoints, &extra)

		bm.server.queryBatch = func([]wire.Message,
			func(*ServerPeer, wire.Message, wire.Message) bool,
			<-chan struct{}, ...QueryOption) {

			t.Error("expected no query to be made")
		}

		err := bm.getCheckpointedCFHeaders(
			checkpoints, cfStore, wire.GCSFilterRegular,
		)
		if err == nil {
			t.Fatal("expected an error")
		}
	})

	t.Run("write failure", func(t *testing.T) {
		t.Parallel()

		bm, cfStore, headers, mockPeer := setupCheckpointedTest(
			t, false,
		)
		defer drainBlockNtfns(bm)()

		// Write part of the first interval with filter headers that
		// don't match the checkpoints. The first response is then
		// written on top of them, and the second one no longer
		// follows the tip.
		stored := make(
			[]headerfs.FilterHeader, wire.CFCheckptInterval/3-1,
		)
		copy(stored, headers.cfHeaders[1:wire.CFCheckptInterval/3])
		for i := range stored {
			stored[i].FilterHash[0] ^= 1
		}
		if err := cfStore.WriteHeaders(stored...); err != nil {
			t.Fatal(err)
		}

		bm.server.queryBatch = func(msgs []wire.Message,
			f func(*ServerPeer, wire.Message, wire.Message) bool,
			quit <-chan struct{}, _ ...QueryOption) {

			responses, err := generateResponses(msgs, headers)
			if err != nil {
				t.Errorf("unable to generate responses: %v",
					err)
				return
			}

			if !f(mockPeer, msgs[0], responses[0]) {
				t.Error("expected first response to be " +
					"accepted")
				return
			}
			if f(mockPeer, msgs[1], responses[1]) {
				t.Error("expected second response to fail " +
					"to write")
				return
			}

			select {
			case <-quit:
			default:
				t.Error("expected query to be stopped")
			}

			// Responses after a failed write are ignored.
			if f(mockPeer, msgs[2], responses[2]) {
				t.Error("expected response after failed " +
					"write to be ignored")
			}
		}

		err := bm.getCheckpointedCFHeaders(
			headers.checkpoints, cfStore, wire.GCSFilterRegular,
		)
		if err == nil {
			t.Fatal("expected an error")
		}
	})
}

// mineTestHeader returns a header that builds on prevHash with the simnet
// proof of work limit, searching nonces until its hash meets that target.
func mineTestHeader(t *testing.T, prevHash chainhash.Hash,
	timestamp time.Time, extraNonce uint32) *wire.BlockHeader {

	t.Helper()

	target := blockchain.CompactToBig(chaincfg.SimNetParams.PowLimitBits)
	header := &wire.BlockHeader{
		Version:   1,
		PrevBlock: prevHash,
		Timestamp: timestamp,
		Bits:      chaincfg.SimNetParams.PowLimitBits,
	}
	binary.BigEndian.PutUint32(header.MerkleRoot[:], extraNonce)
	for nonce := uint32(0); nonce < 1000; nonce++ {
		header.Nonce = nonce
		hash := header.BlockHash()
		if blockchain.HashToBig(&hash).Cmp(target) <= 0 {
			return header
		}
	}

	t.Fatalf("unable to find a nonce for header %d", extraNonce)
	return nil
}

// TestBlockManagerReorgHeadersMustConnect checks that headers on the reorg
// path only count toward a reorg when each one builds on the one before it.
// A batch that doesn't link is rejected and leaves the chain as it was, while
// the same amount of work in linked headers switches to the new branch.
func TestBlockManagerReorgHeadersMustConnect(t *testing.T) {
	t.Parallel()

	bm, hdrStore, _, cleanUp, err := setupBlockManager()
	if err != nil {
		t.Fatalf("unable to set up block manager: %v", err)
	}
	defer cleanUp()

	bm.server.blockManager = bm
	bm.server.timeSource = blockchain.NewMedianTime()
	bm.server.mempool = NewMempool()

	// Drain the block notifications sent when a reorg rolls the chain
	// back.
	quit := make(chan struct{})
	defer close(quit)
	go func() {
		for {
			select {
			case <-bm.blockNtfnChan:
			case <-quit:
				return
			}
		}
	}()

	sp := newServerPeer(bm.server, false)
	sp.Peer, err = peer.NewOutboundPeer(
		newPeerConfig(sp), "127.0.0.1:18555",
	)
	if err != nil {
		t.Fatal(err)
	}

	genesis, _, err := hdrStore.ChainTip()
	if err != nil {
		t.Fatal(err)
	}
	genesisHash := genesis.BlockHash()
	now := time.Unix(time.Now().Unix(), 0)

	// Build a known chain of two headers on top of genesis.
	a1 := mineTestHeader(t, genesisHash, now.Add(-time.Hour), 1)
	a2 := mineTestHeader(t, a1.BlockHash(), now.Add(-50*time.Minute), 2)
	known := wire.NewMsgHeaders()
	_ = known.AddBlockHeader(a1)
	_ = known.AddBlockHeader(a2)
	bm.handleHeadersMsg(&headersMsg{headers: known, peer: sp})

	tip, height, err := hdrStore.ChainTip()
	if err != nil {
		t.Fatal(err)
	}
	if height != 2 || tip.BlockHash() != a2.BlockHash() {
		t.Fatalf("expected tip %v at height 2, got %v at height %d",
			a2.BlockHash(), tip.BlockHash(), height)
	}

	// A branch off genesis whose first header is new, followed by two
	// headers that don't build on it, carries more work than the known
	// chain but doesn't form a chain, so it must not cause a reorg.
	b1 := mineTestHeader(t, genesisHash, now.Add(-40*time.Minute), 3)
	unlinked := wire.NewMsgHeaders()
	_ = unlinked.AddBlockHeader(b1)
	_ = unlinked.AddBlockHeader(mineTestHeader(
		t, chainhash.Hash{0x01}, now.Add(-30*time.Minute), 4,
	))
	_ = unlinked.AddBlockHeader(mineTestHeader(
		t, chainhash.Hash{0x02}, now.Add(-20*time.Minute), 5,
	))
	bm.handleHeadersMsg(&headersMsg{headers: unlinked, peer: sp})

	tip, height, err = hdrStore.ChainTip()
	if err != nil {
		t.Fatal(err)
	}
	if height != 2 || tip.BlockHash() != a2.BlockHash() {
		t.Fatalf("headers that don't link changed the tip to %v "+
			"at height %d", tip.BlockHash(), height)
	}

	// The same branch with headers that link to each other has more
	// work than the known chain, so it replaces it.
	b2 := mineTestHeader(t, b1.BlockHash(), now.Add(-30*time.Minute), 6)
	b3 := mineTestHeader(t, b2.BlockHash(), now.Add(-20*time.Minute), 7)
	linked := wire.NewMsgHeaders()
	_ = linked.AddBlockHeader(b1)
	_ = linked.AddBlockHeader(b2)
	_ = linked.AddBlockHeader(b3)
	bm.handleHeadersMsg(&headersMsg{headers: linked, peer: sp})

	tip, height, err = hdrStore.ChainTip()
	if err != nil {
		t.Fatal(err)
	}
	if height != 3 || tip.BlockHash() != b3.BlockHash() {
		t.Fatalf("expected tip %v at height 3 after the reorg, got "+
			"%v at height %d", b3.BlockHash(), tip.BlockHash(),
			height)
	}
}

// TestTxRequestsRetried checks that the block manager asks another peer that
// announced an unconfirmed transaction for it when the peer it asked
// disconnects or answers that it doesn't have it, and asks again on a later
// announcement once no peer is left to ask. It asked a transaction of a peer
// only once, and then never again, however many peers announced it.
func TestTxRequestsRetried(t *testing.T) {
	newPeer := func(addr string) *ServerPeer {
		p, err := peer.NewOutboundPeer(&peer.Config{}, addr)
		if err != nil {
			t.Fatal(err)
		}
		return &ServerPeer{Peer: p}
	}
	first := newPeer("10.0.0.1:18333")
	second := newPeer("10.0.0.2:18333")
	third := newPeer("10.0.0.3:18333")

	bm := &blockManager{
		server:        &ChainService{mempool: NewMempool()},
		requestedTxns: make(map[chainhash.Hash]*txRequest),
	}
	hash := chainhash.Hash{1}
	asked := func() *ServerPeer {
		if req := bm.requestedTxns[hash]; req != nil {
			return req.peer
		}
		return nil
	}
	notFound := func(sp *ServerPeer) {
		msg := wire.NewMsgNotFound()
		_ = msg.AddInvVect(wire.NewInvVect(wire.InvTypeTx, &hash))
		bm.handleNotFoundMsg(&notFoundMsg{notFound: msg, peer: sp})
	}

	if !bm.wantTx(first, hash) {
		t.Fatal("the first announcement of the transaction doesn't ask for it")
	}
	if bm.wantTx(second, hash) || bm.wantTx(third, hash) {
		t.Fatal("later announcements ask for the transaction again")
	}

	bm.handleDonePeerMsg(list.New(), first)
	if asked() != second {
		t.Fatalf("asked %v once the first peer disconnected, want the second",
			asked())
	}
	notFound(second)
	if asked() != third {
		t.Fatalf("asked %v once the second peer didn't have it, want the "+
			"third", asked())
	}
	notFound(third)
	if asked() != nil {
		t.Fatalf("still waiting for %v, which didn't have it", asked())
	}
	if !bm.wantTx(second, hash) {
		t.Fatal("a later announcement doesn't ask for the transaction")
	}
}

// TestRollBackToHeightDisconnectedHeights checks that rolling back several
// blocks sends a disconnected notification for each block with that block's
// own height and the header that becomes the new tip, and that the block
// manager's tip of the filter headers follows the filter headers back, so that
// a new block subscriber isn't handed blocks above it.
func TestRollBackToHeightDisconnectedHeights(t *testing.T) {
	t.Parallel()

	bm, hdrStore, cfStore, cleanUp, err := setupBlockManager()
	if err != nil {
		t.Fatalf("unable to set up ChainService: %v", err)
	}
	defer cleanUp()
	bm.server.blockManager = bm

	genesis, _, err := hdrStore.ChainTip()
	if err != nil {
		t.Fatal(err)
	}
	genesisFilterHeader, _, err := cfStore.ChainTip()
	if err != nil {
		t.Fatal(err)
	}

	// Build a chain of four blocks on top of genesis, with filter headers
	// for each, so both stores are rolled back.
	const tipHeight = 4
	chain := []wire.BlockHeader{*genesis}
	var (
		blockHeaders  []headerfs.BlockHeader
		filterHeaders []headerfs.FilterHeader
	)
	filterHeader := *genesisFilterHeader
	for height := uint32(1); height <= tipHeight; height++ {
		header := wire.BlockHeader{
			PrevBlock: chain[height-1].BlockHash(),
			Nonce:     height,
		}
		chain = append(chain, header)
		blockHeaders = append(blockHeaders, headerfs.BlockHeader{
			BlockHeader: &chain[height],
			Height:      height,
		})
		filterHeader = chainhash.DoubleHashH(filterHeader[:])
		filterHeaders = append(filterHeaders, headerfs.FilterHeader{
			HeaderHash: header.BlockHash(),
			FilterHash: filterHeader,
			Height:     height,
		})
	}
	if err := hdrStore.WriteHeaders(blockHeaders...); err != nil {
		t.Fatalf("unable to write block headers: %v", err)
	}
	if err := cfStore.WriteHeaders(filterHeaders...); err != nil {
		t.Fatalf("unable to write filter headers: %v", err)
	}
	bm.filterHeaderTip = tipHeight
	bm.filterHeaderTipHash = chain[tipHeight].BlockHash()

	// The notification channel is unbuffered, so roll back in the
	// background and read the notifications here.
	const rollBackTo = 1
	errChan := make(chan error, 1)
	go func() {
		_, err := bm.server.rollBackToHeight(rollBackTo)
		errChan <- err
	}()

	for height := uint32(tipHeight); height > rollBackTo; height-- {
		var ntfn blockntfns.BlockNtfn
		select {
		case ntfn = <-bm.blockNtfnChan:
		case err := <-errChan:
			t.Fatalf("rollback ended before the notification for "+
				"height %d: %v", height, err)
		case <-time.After(5 * time.Second):
			t.Fatalf("no notification for height %d", height)
		}

		disconnected, ok := ntfn.(*blockntfns.Disconnected)
		if !ok {
			t.Fatalf("expected a disconnected notification, got %v",
				ntfn)
		}
		if disconnected.Height() != height {
			t.Fatalf("disconnected notification has height %d, "+
				"expected %d", disconnected.Height(), height)
		}
		header := disconnected.Header()
		if header.BlockHash() != chain[height].BlockHash() {
			t.Fatalf("disconnected notification at height %d has "+
				"the wrong header", height)
		}
		tip := disconnected.ChainTip()
		if tip.BlockHash() != chain[height-1].BlockHash() {
			t.Fatalf("disconnected notification at height %d has "+
				"the wrong new tip", height)
		}
	}

	select {
	case err := <-errChan:
		if err != nil {
			t.Fatalf("unable to roll back: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("rollback did not finish")
	}

	_, height, err := hdrStore.ChainTip()
	if err != nil {
		t.Fatal(err)
	}
	if height != rollBackTo {
		t.Fatalf("block header tip at height %d, expected %d", height,
			rollBackTo)
	}
	_, height, err = cfStore.ChainTip()
	if err != nil {
		t.Fatal(err)
	}
	if height != rollBackTo {
		t.Fatalf("filter header tip at height %d, expected %d", height,
			rollBackTo)
	}
	if bm.filterHeaderTip != rollBackTo ||
		bm.filterHeaderTipHash != chain[rollBackTo].BlockHash() {

		t.Fatalf("the block manager's filter header tip is %v at "+
			"height %d, expected %v at height %d",
			bm.filterHeaderTipHash, bm.filterHeaderTip,
			chain[rollBackTo].BlockHash(), rollBackTo)
	}
	backlog, bestHeight, err := bm.NotificationsSinceHeight(rollBackTo)
	if err != nil {
		t.Fatal(err)
	}
	if len(backlog) != 0 || bestHeight != rollBackTo {
		t.Fatalf("a block subscriber at height %d is handed %d blocks "+
			"up to height %d, expected none", rollBackTo,
			len(backlog), bestHeight)
	}
}

// TestFilterHeaderRollbackSubscriptions ensures that each subscriber to
// rollbacks of the filter headers is left with the lowest height it hasn't
// received yet, that a new subscriber starts with the lowest height the filter
// headers were rolled back to before, and that a canceled one receives no more.
func TestFilterHeaderRollbackSubscriptions(t *testing.T) {
	t.Parallel()

	bm, _, _, cleanUp, err := setupBlockManager()
	if err != nil {
		t.Fatalf("unable to set up ChainService: %v", err)
	}
	defer cleanUp()

	assertRollback := func(rollbacks <-chan uint32, height uint32) {
		t.Helper()

		select {
		case got := <-rollbacks:
			if got != height {
				t.Fatalf("expected rollback to height %d, "+
					"got %d", height, got)
			}
		default:
			t.Fatalf("expected rollback to height %d", height)
		}
	}
	assertNoRollback := func(rollbacks <-chan uint32) {
		t.Helper()

		select {
		case got := <-rollbacks:
			t.Fatalf("expected no rollback, got one to height %d",
				got)
		default:
		}
	}

	// A subscriber from before any rollback starts with none.
	rollbacks1, cancel1 := bm.subscribeFilterHeaderRollbacks()
	assertNoRollback(rollbacks1)

	// It receives a rollback, and when it hasn't received several, the
	// lowest of them, as it must rewind to the lowest.
	bm.notifyFilterHeaderRollback(100)
	assertRollback(rollbacks1, 100)
	bm.notifyFilterHeaderRollback(120)
	bm.notifyFilterHeaderRollback(90)
	bm.notifyFilterHeaderRollback(110)
	assertRollback(rollbacks1, 90)
	assertNoRollback(rollbacks1)

	// A new subscriber starts with the lowest rollback so far, as it may
	// have scanned blocks above it with the wrong filters before then.
	rollbacks2, cancel2 := bm.subscribeFilterHeaderRollbacks()
	defer cancel2()
	assertRollback(rollbacks2, 90)

	// A canceled subscriber receives no more rollbacks, while the others
	// do.
	cancel1()
	bm.notifyFilterHeaderRollback(80)
	assertNoRollback(rollbacks1)
	assertRollback(rollbacks2, 80)

	// A rollback above the lowest so far is received, but new subscribers
	// still start with the lowest.
	bm.notifyFilterHeaderRollback(95)
	assertRollback(rollbacks2, 95)
	rollbacks3, cancel3 := bm.subscribeFilterHeaderRollbacks()
	defer cancel3()
	assertRollback(rollbacks3, 80)
	assertNoRollback(rollbacks3)
}

// repairTestTip is the height of the chain that the tests of repairing the
// filter headers build, past two filter checkpoints.
const repairTestTip = 2*wire.CFCheckptInterval + 500

// filterChain is a chain of filter headers, with the filter hash each commits
// to, and the filter served for a block, if any, by height.
type filterChain struct {
	hashes  []chainhash.Hash
	headers []chainhash.Hash
	filters map[uint32]*gcs.Filter
}

// newFilterChain builds a chain of filter headers on top of the given genesis
// filter header from the given filter hashes, by height.
func newFilterChain(genesis chainhash.Hash, hashes []chainhash.Hash,
	filters map[uint32]*gcs.Filter) *filterChain {

	headers := make([]chainhash.Hash, len(hashes))
	headers[0] = genesis
	for height := 1; height < len(hashes); height++ {
		// header = dsha256(filterHash || prevHeader)
		headers[height] = chainhash.DoubleHashH(
			append(hashes[height][:], headers[height-1][:]...),
		)
	}

	return &filterChain{hashes: hashes, headers: headers, filters: filters}
}

// mockFilterPeers is a QueryAccess whose peers answer queries for filter
// checkpoints, filter headers and filters from their own filter chain.
type mockFilterPeers struct {
	blockHashes []chainhash.Hash
	heights     map[chainhash.Hash]uint32
	chains      map[string]*filterChain

	// checkpoints, if it holds a chain for a peer, is the chain the peer
	// serves its filter checkpoints from instead.
	checkpoints map[string]*filterChain
}

func (m *mockFilterPeers) queryAllPeers(msg wire.Message,
	checkResponse func(sp *ServerPeer, resp wire.Message,
		quit chan<- struct{}, peerQuit chan<- struct{}),
	_ ...QueryOption) {

	for addr, chain := range m.chains {
		var resp wire.Message
		switch msg := msg.(type) {
		case *wire.MsgGetCFCheckpt:
			if cpChain, ok := m.checkpoints[addr]; ok {
				chain = cpChain
			}
			stop := m.heights[msg.StopHash]
			cp := wire.NewMsgCFCheckpt(
				msg.FilterType, &msg.StopHash,
				int(stop/wire.CFCheckptInterval),
			)
			for height := uint32(wire.CFCheckptInterval); height <= stop; height += wire.CFCheckptInterval {
				_ = cp.AddCFHeader(&chain.headers[height])
			}
			resp = cp

		case *wire.MsgGetCFHeaders:
			resp = m.cfHeaders(chain, msg)

		case *wire.MsgGetCFilters:
			filter, ok := chain.filters[msg.StartHeight]
			if !ok {
				continue
			}
			data, _ := filter.NBytes()
			resp = wire.NewMsgCFilter(
				msg.FilterType, &m.blockHashes[msg.StartHeight],
				data,
			)

		default:
			continue
		}

		pp, err := peer.NewOutboundPeer(&peer.Config{}, addr)
		if err != nil {
			panic(err)
		}
		checkResponse(
			&ServerPeer{Peer: pp}, resp, make(chan struct{}),
			make(chan struct{}),
		)
	}
}

// cfHeaders answers the given query for filter headers from the given chain.
func (m *mockFilterPeers) cfHeaders(chain *filterChain,
	msg *wire.MsgGetCFHeaders) *wire.MsgCFHeaders {

	headers := wire.NewMsgCFHeaders()
	headers.FilterType = msg.FilterType
	headers.StopHash = msg.StopHash
	headers.PrevFilterHeader = chain.headers[msg.StartHeight-1]
	stop := m.heights[msg.StopHash]
	for height := msg.StartHeight; height <= stop; height++ {
		_ = headers.AddCFHash(&chain.hashes[height])
	}

	return headers
}

var _ QueryAccess = (*mockFilterPeers)(nil)

// Peers of the tests of repairing the filter headers.
const (
	repairPeer1 = "10.0.0.1:8333"
	repairPeer2 = "10.0.0.2:8333"
	repairPeer3 = "10.0.0.3:8333"
	repairPeer4 = "10.0.0.4:8333"
)

// repairTest is a block manager whose block headers run to a tip, and the
// filter chains that the tests of repairing the filter headers serve and
// store. The chains share made up filter hashes but where they diverge, at the
// block whose header is that of block 170 of mainnet. There, the right chain
// commits to the block's filter, and the wrong and other chains to filters
// that each leave out a script the block pays.
type repairTest struct {
	bm          *blockManager
	cfStore     *headerfs.FilterHeaderStore
	peers       *mockFilterPeers
	block       *bchutil.Block
	divergence  uint32
	tip         uint32
	blockHashes []chainhash.Hash

	right, wrong, other *filterChain
}

// filterWithout returns the filter of the given block built as though its
// second transaction didn't pay the script of the given output.
func filterWithout(t *testing.T, block *bchutil.Block, output int) *gcs.Filter {
	t.Helper()

	spend := block.MsgBlock().Transactions[1].Copy()
	spend.TxOut[output].PkScript = nil
	filter, err := builder.BuildBasicFilter(&wire.MsgBlock{
		Header: block.MsgBlock().Header,
		Transactions: []*wire.MsgTx{
			block.MsgBlock().Transactions[0], spend,
		},
	})
	if err != nil {
		t.Fatalf("unable to build filter: %v", err)
	}

	return filter
}

// newTestFilterDB creates a filter database that's removed once the test ends.
func newTestFilterDB(t *testing.T) filterdb.FilterDatabase {
	t.Helper()

	db, err := walletdb.Create(
		"bdb", filepath.Join(t.TempDir(), "filters.db"), true,
	)
	if err != nil {
		t.Fatalf("unable to create filter db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	filterDB, err := filterdb.New(db, chaincfg.SimNetParams)
	if err != nil {
		t.Fatalf("unable to create filter db: %v", err)
	}

	return filterDB
}

// setupRepairTest sets up a repairTest whose block headers run to
// repairTestTip and whose filter chains diverge at the given height, with no
// peers and no filter headers stored.
func setupRepairTest(t *testing.T, divergence uint32) *repairTest {
	t.Helper()

	return setupRepairTestTo(t, divergence, repairTestTip, false)
}

// setupRepairTestTo sets up a repairTest whose block headers run to the given
// tip and whose filter chains diverge at the given height, with no peers and no
// filter headers stored. The block headers are as old as the genesis block's,
// unless synced is set, in which case they're recent, so that the block
// manager takes them as synced.
func setupRepairTestTo(t *testing.T, divergence, tip uint32,
	synced bool) *repairTest {

	t.Helper()

	bm, hdrStore, cfStore, cleanUp, err := setupBlockManager()
	if err != nil {
		t.Fatalf("unable to set up ChainService: %v", err)
	}
	t.Cleanup(cleanUp)

	blocks, err := loadBlocks(t, blockDataFile, blockDataNet)
	if err != nil {
		t.Fatalf("unable to load blocks: %v", err)
	}

	// Block 170 spends an output and pays two scripts.
	block := blocks[170]

	genesis, _, err := hdrStore.ChainTip()
	if err != nil {
		t.Fatal(err)
	}
	blockHashes := make([]chainhash.Hash, tip+1)
	blockHashes[0] = genesis.BlockHash()
	heights := map[chainhash.Hash]uint32{blockHashes[0]: 0}
	blockHeaders := make([]headerfs.BlockHeader, 0, tip)
	for height := uint32(1); height <= tip; height++ {
		header := &wire.BlockHeader{
			PrevBlock: blockHashes[height-1],
			Timestamp: genesis.Timestamp,
			Nonce:     height,
		}
		if synced {
			header.Timestamp = time.Unix(time.Now().Unix(), 0)
		}
		if height == divergence {
			header = &block.MsgBlock().Header
		}

		blockHashes[height] = header.BlockHash()
		heights[blockHashes[height]] = height
		blockHeaders = append(blockHeaders, headerfs.BlockHeader{
			BlockHeader: header,
			Height:      height,
		})
	}
	if err := hdrStore.WriteHeaders(blockHeaders...); err != nil {
		t.Fatalf("unable to write block headers: %v", err)
	}
	bm.headerTip = tip
	bm.headerTipHash = blockHashes[tip]

	genesisFilterHeader, _, err := cfStore.ChainTip()
	if err != nil {
		t.Fatal(err)
	}
	hashes := make([]chainhash.Hash, tip+1)
	for height := range hashes {
		hashes[height] = chainhash.HashH(
			[]byte{byte(height), byte(height >> 8)},
		)
	}
	chainWith := func(filter *gcs.Filter) *filterChain {
		chainHashes := slices.Clone(hashes)
		filterHash, err := builder.GetFilterHash(filter)
		if err != nil {
			t.Fatalf("unable to hash filter: %v", err)
		}
		chainHashes[divergence] = filterHash

		return newFilterChain(
			*genesisFilterHeader, chainHashes,
			map[uint32]*gcs.Filter{divergence: filter},
		)
	}
	right, err := builder.BuildBasicFilter(block.MsgBlock())
	if err != nil {
		t.Fatalf("unable to build filter: %v", err)
	}

	cs := bm.server
	cs.blockManager = bm
	cs.timeSource = blockchain.NewMedianTime()
	cs.FilterCache = lru.NewCache(1 << 20)
	cs.FilterDB = newTestFilterDB(t)

	// Only the block whose header is real can be fetched.
	cs.queryPeers = func(msg wire.Message, f func(*ServerPeer,
		wire.Message, chan<- struct{}), _ ...QueryOption) {

		getData, ok := msg.(*wire.MsgGetData)
		if !ok || len(getData.InvList) != 1 ||
			getData.InvList[0].Hash != *block.Hash() {

			return
		}
		f(nil, block.MsgBlock(), make(chan struct{}))
	}

	peers := &mockFilterPeers{
		blockHashes: blockHashes,
		heights:     heights,
		chains:      make(map[string]*filterChain),
		checkpoints: make(map[string]*filterChain),
	}
	bm.queries = peers

	return &repairTest{
		bm:          bm,
		cfStore:     cfStore,
		peers:       peers,
		block:       block,
		divergence:  divergence,
		tip:         tip,
		blockHashes: blockHashes,
		right:       chainWith(right),
		wrong:       chainWith(filterWithout(t, block, 0)),
		other:       chainWith(filterWithout(t, block, 1)),
	}
}

// storeFilterChain stores the filter headers of the given chain up to the given
// height.
func (rt *repairTest) storeFilterChain(t *testing.T, chain *filterChain,
	tip uint32) {

	t.Helper()

	headers := make([]headerfs.FilterHeader, 0, tip)
	for height := uint32(1); height <= tip; height++ {
		headers = append(headers, headerfs.FilterHeader{
			HeaderHash: rt.blockHashes[height],
			FilterHash: chain.headers[height],
			Height:     height,
		})
	}
	if err := rt.cfStore.WriteHeaders(headers...); err != nil {
		t.Fatalf("unable to write filter headers: %v", err)
	}

	rt.bm.filterHeaderTip = tip
	rt.bm.filterHeaderTipHash = rt.blockHashes[tip]
}

// assertFilterTip asserts that the stored filter headers end with the given
// chain's at the given height, and that the block manager knows it.
func (rt *repairTest) assertFilterTip(t *testing.T, chain *filterChain,
	tip uint32) {

	t.Helper()

	header, height, err := rt.cfStore.ChainTip()
	if err != nil {
		t.Fatalf("unable to get filter tip: %v", err)
	}
	if height != tip || *header != chain.headers[tip] {
		t.Fatalf("expected filter tip %v at height %d, got %v at "+
			"height %d", chain.headers[tip], tip, header, height)
	}

	rt.bm.newFilterHeadersMtx.RLock()
	defer rt.bm.newFilterHeadersMtx.RUnlock()
	if rt.bm.filterHeaderTip != tip ||
		rt.bm.filterHeaderTipHash != rt.blockHashes[tip] {

		t.Fatalf("block manager has filter tip %v at height %d, "+
			"expected %v at height %d", rt.bm.filterHeaderTipHash,
			rt.bm.filterHeaderTip, rt.blockHashes[tip], tip)
	}
}

// assertBanned asserts that exactly the given peers are banned.
func (rt *repairTest) assertBanned(t *testing.T, banned ...string) {
	t.Helper()

	for _, addr := range []string{
		repairPeer1, repairPeer2, repairPeer3, repairPeer4,
	} {
		want := slices.Contains(banned, addr)
		if got := rt.bm.server.IsBanned(addr); got != want {
			t.Fatalf("expected peer %v banned %v, got %v", addr,
				want, got)
		}
	}
}

// TestRepairFilterHeaders checks that a repair of the filter headers finds the
// first block whose filter header differs between us and a peer, and settles
// who's right from the block's filter: it rolls the filter headers back to the
// block before when a peer is right, bans the peers that are wrong, and does
// neither when no peer differs or no one is right.
func TestRepairFilterHeaders(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		divergence uint32

		// store and peers name the chains stored and served, and
		// checkpoints the chains peers serve their checkpoints from
		// instead, if any.
		store       string
		peers       map[string]string
		checkpoints map[string]string

		wantErr      bool
		wantRollback bool
		wantBanned   []string
	}{
		{
			name:       "wrong filter headers before a checkpoint",
			divergence: 500,
			store:      "wrong",
			peers: map[string]string{
				repairPeer1: "right", repairPeer2: "right",
				repairPeer3: "wrong",
			},
			wantRollback: true,
			wantBanned:   []string{repairPeer3},
		},
		{
			name:       "wrong filter headers between checkpoints",
			divergence: 1500,
			store:      "wrong",
			peers: map[string]string{
				repairPeer1: "right", repairPeer2: "right",
				repairPeer3: "wrong",
			},
			wantRollback: true,
			wantBanned:   []string{repairPeer3},
		},
		{
			name:       "wrong filter headers after the last checkpoint",
			divergence: 2300,
			store:      "wrong",
			peers: map[string]string{
				repairPeer1: "right", repairPeer3: "wrong",
			},
			wantRollback: true,
			wantBanned:   []string{repairPeer3},
		},
		{
			name:       "a peer serving filter headers that are wrong",
			divergence: 1500,
			store:      "right",
			peers: map[string]string{
				repairPeer1: "right", repairPeer3: "wrong",
			},
			wantBanned: []string{repairPeer3},
		},
		{
			name:       "a peer whose filter headers don't follow its checkpoints",
			divergence: 1500,
			store:      "wrong",
			peers: map[string]string{
				repairPeer1: "right", repairPeer3: "wrong",
				repairPeer4: "early",
			},
			checkpoints: map[string]string{
				repairPeer4: "right",
			},
			wantRollback: true,
			wantBanned:   []string{repairPeer3},
		},
		{
			name:       "no peer differs",
			divergence: 1500,
			store:      "right",
			peers: map[string]string{
				repairPeer1: "right", repairPeer2: "right",
			},
			wantErr: true,
		},
		{
			name:       "no one matches the block",
			divergence: 1500,
			store:      "wrong",
			peers: map[string]string{
				repairPeer1: "other",
			},
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			rt := setupRepairTest(t, test.divergence)

			// The early chain diverges from the right one before
			// the filter checkpoint below the divergence.
			early := slices.Clone(rt.right.hashes)
			early[wire.CFCheckptInterval-100] = chainhash.Hash{1}
			chains := map[string]*filterChain{
				"right": rt.right,
				"wrong": rt.wrong,
				"other": rt.other,
				"early": newFilterChain(
					rt.right.headers[0], early, nil,
				),
			}
			for addr, name := range test.peers {
				rt.peers.chains[addr] = chains[name]
			}
			for addr, name := range test.checkpoints {
				rt.peers.checkpoints[addr] = chains[name]
			}

			stored := chains[test.store]
			rt.storeFilterChain(t, stored, repairTestTip)
			rollbacks, cancel := rt.bm.subscribeFilterHeaderRollbacks()
			defer cancel()

			// Looking up the peers to disconnect returns right
			// away once the ChainService quits.
			rt.bm.server.quit = make(chan struct{})
			close(rt.bm.server.quit)

			err := rt.bm.repairFilterHeaders()
			if (err != nil) != test.wantErr {
				t.Fatalf("expected error %v, got %v",
					test.wantErr, err)
			}

			rt.assertBanned(t, test.wantBanned...)

			if !test.wantRollback {
				rt.assertFilterTip(t, stored, repairTestTip)
				select {
				case height := <-rollbacks:
					t.Fatalf("unexpected rollback to height "+
						"%d", height)
				default:
				}
				return
			}

			rt.assertFilterTip(t, stored, test.divergence-1)
			select {
			case height := <-rollbacks:
				if height != test.divergence-1 {
					t.Fatalf("expected rollback to height "+
						"%d, got %d", test.divergence-1,
						height)
				}
			default:
				t.Fatal("expected a rollback notification")
			}
		})
	}
}

// TestRollBackFilterHeaders checks that rolling back the filter headers drops
// them and the filters held for their blocks above the height, in batches
// when there are many, leaves the rest, lets them be synced again, and notifies
// subscribers, and that rolling back to the tip or above does nothing.
func TestRollBackFilterHeaders(t *testing.T) {
	t.Parallel()

	rt := setupRepairTest(t, 1500)
	rt.storeFilterChain(t, rt.right, repairTestTip)
	cs := rt.bm.server

	// We'll hold filters for blocks below and above where we roll back
	// to, more than a batch of blocks apart.
	const rollBackTo = 100
	heights := []uint32{rollBackTo, rollBackTo + 1, 2100, repairTestTip}
	filter, err := builder.BuildBasicFilter(rt.block.MsgBlock())
	if err != nil {
		t.Fatal(err)
	}
	for _, height := range heights {
		hash := rt.blockHashes[height]
		_, err := cs.putFilterToCache(&hash, filterdb.RegularFilter, filter)
		if err != nil {
			t.Fatal(err)
		}
		err = cs.FilterDB.PutFilter(&hash, filter, filterdb.RegularFilter)
		if err != nil {
			t.Fatal(err)
		}
	}

	rollbacks, cancel := rt.bm.subscribeFilterHeaderRollbacks()
	defer cancel()

	for _, height := range []uint32{repairTestTip, repairTestTip + 5} {
		if err := rt.bm.rollBackFilterHeaders(height); err != nil {
			t.Fatalf("unable to roll back to height %d: %v", height,
				err)
		}
	}
	rt.assertFilterTip(t, rt.right, repairTestTip)
	select {
	case height := <-rollbacks:
		t.Fatalf("unexpected rollback to height %d", height)
	default:
	}

	if err := rt.bm.rollBackFilterHeaders(rollBackTo); err != nil {
		t.Fatalf("unable to roll back: %v", err)
	}
	rt.assertFilterTip(t, rt.right, rollBackTo)
	select {
	case height := <-rollbacks:
		if height != rollBackTo {
			t.Fatalf("expected rollback to height %d, got %d",
				rollBackTo, height)
		}
	default:
		t.Fatal("expected a rollback notification")
	}

	for _, height := range heights {
		hash := rt.blockHashes[height]
		want := height <= rollBackTo

		cached, err := cs.getFilterFromCache(
			&hash, filterdb.RegularFilter,
		)
		if (err == nil && cached != nil) != want {
			t.Fatalf("expected filter for height %d cached %v, "+
				"got %v, %v", height, want, cached, err)
		}

		stored, err := cs.FilterDB.FetchFilter(
			&hash, filterdb.RegularFilter,
		)
		if (err == nil && stored != nil) != want {
			t.Fatalf("expected filter for height %d stored %v, "+
				"got %v, %v", height, want, stored, err)
		}
	}

	// The filter headers can be synced again from there.
	headers := make([]headerfs.FilterHeader, 0, repairTestTip-rollBackTo)
	for height := uint32(rollBackTo + 1); height <= repairTestTip; height++ {
		headers = append(headers, headerfs.FilterHeader{
			HeaderHash: rt.blockHashes[height],
			FilterHash: rt.right.headers[height],
			Height:     height,
		})
	}
	if err := rt.cfStore.WriteHeaders(headers...); err != nil {
		t.Fatalf("unable to write filter headers: %v", err)
	}
	tip, height, err := rt.cfStore.ChainTip()
	if err != nil || height != repairTestTip ||
		*tip != rt.right.headers[repairTestTip] {

		t.Fatalf("expected filter tip at height %d, got %v at height "+
			"%d: %v", repairTestTip, tip, height, err)
	}
}

// TestFilterHeaderRepairRequests checks that a request to repair the filter
// headers wakes up a waiting cfHandler and is taken once, and that requests
// are dropped while we back off after a repair that couldn't settle it.
func TestFilterHeaderRepairRequests(t *testing.T) {
	t.Parallel()

	bm, _, _, cleanUp, err := setupBlockManager()
	if err != nil {
		t.Fatalf("unable to set up ChainService: %v", err)
	}
	defer cleanUp()

	if bm.repairFilterHeadersIfRequested() {
		t.Fatal("repaired the filter headers without a request")
	}

	// A request wakes up cfHandler as it waits for new headers. The waiter
	// holds the lock until it waits, so once we hold it, it's waiting.
	waiting, woken := make(chan struct{}), make(chan struct{})
	go func() {
		bm.newHeadersSignal.L.Lock()
		close(waiting)
		for !bm.filterRepairRequested {
			bm.newHeadersSignal.Wait()
		}
		bm.newHeadersSignal.L.Unlock()
		close(woken)
	}()
	<-waiting
	bm.newHeadersSignal.L.Lock()
	requested := bm.filterRepairRequested
	bm.newHeadersSignal.L.Unlock()
	if requested {
		t.Fatal("expected no repair requested yet")
	}
	bm.requestFilterHeaderRepair()
	select {
	case <-woken:
	case <-time.After(5 * time.Second):
		t.Fatal("expected the request to wake up a waiter")
	}

	// With only the genesis filter header stored, the repair can't settle
	// anything, so we back off.
	if !bm.repairFilterHeadersIfRequested() {
		t.Fatal("expected the requested repair to be made")
	}
	if bm.repairFilterHeadersIfRequested() {
		t.Fatal("expected the request to be taken once")
	}

	bm.requestFilterHeaderRepair()
	if bm.repairFilterHeadersIfRequested() {
		t.Fatal("expected a request to be dropped while backing off")
	}

	bm.newHeadersMtx.Lock()
	bm.filterRepairBackoffUntil = time.Now().Add(-time.Second)
	bm.newHeadersMtx.Unlock()
	bm.requestFilterHeaderRepair()
	if !bm.repairFilterHeadersIfRequested() {
		t.Fatal("expected a request to be taken after backing off")
	}
}

// TestResolveConflictRequestsRepair checks that resolveConflict asks for a
// repair of the filter headers when our peers agree on filter checkpoints that
// the filter headers we synced don't match, which trying again doesn't fix,
// and that it resolves the checkpoints once the repair rolls them back.
func TestResolveConflictRequestsRepair(t *testing.T) {
	t.Parallel()

	rt := setupRepairTest(t, 1500)
	rt.storeFilterChain(t, rt.wrong, repairTestTip)
	rt.peers.chains[repairPeer1] = rt.right
	rt.peers.chains[repairPeer2] = rt.right

	fType := wire.GCSFilterRegular
	tipHash := rt.blockHashes[repairTestTip]
	_, err := rt.bm.resolveConflict(
		rt.bm.getCheckpts(&tipHash, fType), rt.cfStore, fType,
	)
	if err == nil {
		t.Fatal("expected checkpoints that don't match our filter " +
			"headers to be unresolved")
	}
	if !rt.bm.repairFilterHeadersIfRequested() {
		t.Fatal("expected a repair of the filter headers requested")
	}
	rt.assertFilterTip(t, rt.wrong, rt.divergence-1)
	rt.assertBanned(t)

	checkpoints, err := rt.bm.resolveConflict(
		rt.bm.getCheckpts(&tipHash, fType), rt.cfStore, fType,
	)
	if err != nil {
		t.Fatalf("unable to resolve checkpoints after the repair: %v",
			err)
	}
	if len(checkpoints) != 2 ||
		*checkpoints[1] != rt.right.headers[2*wire.CFCheckptInterval] {

		t.Fatalf("expected the right checkpoints, got %v", checkpoints)
	}
}

// TestUncheckpointedCFHeadersRequestRepair checks that peers whose filter
// headers don't follow ours at the tip have them left out and a repair of the
// filter headers requested, rather than being banned, as they may be right,
// and that the filter headers are synced from them once the repair rolls ours
// back.
func TestUncheckpointedCFHeadersRequestRepair(t *testing.T) {
	t.Parallel()

	rt := setupRepairTest(t, 2300)
	rt.storeFilterChain(t, rt.wrong, repairTestTip-100)
	rt.peers.chains[repairPeer1] = rt.right
	rt.peers.chains[repairPeer2] = rt.right

	err := rt.bm.getUncheckpointedCFHeaders(
		rt.cfStore, wire.GCSFilterRegular,
	)
	if err == nil {
		t.Fatal("expected no filter headers to follow ours")
	}
	rt.assertBanned(t)
	rt.assertFilterTip(t, rt.wrong, repairTestTip-100)

	if !rt.bm.repairFilterHeadersIfRequested() {
		t.Fatal("expected a repair of the filter headers requested")
	}
	rt.assertFilterTip(t, rt.wrong, rt.divergence-1)
	rt.assertBanned(t)

	stopDraining := drainBlockNtfns(rt.bm)
	defer stopDraining()
	err = rt.bm.getUncheckpointedCFHeaders(
		rt.cfStore, wire.GCSFilterRegular,
	)
	if err != nil {
		t.Fatalf("unable to sync filter headers after the repair: %v",
			err)
	}
	rt.assertFilterTip(t, rt.right, repairTestTip)
}

// TestDetectBadPeersWithWrongFilterHeaders checks that detectBadPeers settles
// which peers serve a wrong filter from the block itself, even when the filter
// header we synced for the block is the one that's wrong, which a block fetched
// with GetBlock is checked against.
func TestDetectBadPeersWithWrongFilterHeaders(t *testing.T) {
	t.Parallel()

	rt := setupRepairTest(t, 1500)
	rt.storeFilterChain(t, rt.wrong, repairTestTip)
	rt.peers.chains[repairPeer1] = rt.right
	rt.peers.chains[repairPeer3] = rt.wrong

	const start = wire.CFCheckptInterval + 1
	headers, _ := rt.bm.getCFHeadersForAllPeers(
		start, wire.GCSFilterRegular,
	)
	if len(headers) != 2 {
		t.Fatalf("expected filter headers from 2 peers, got %d",
			len(headers))
	}

	badPeers, err := rt.bm.detectBadPeers(
		headers, rt.divergence, rt.divergence-start,
		wire.GCSFilterRegular,
	)
	if err != nil {
		t.Fatalf("unable to detect bad peers: %v", err)
	}
	err = assertBadPeers(map[string]struct{}{repairPeer3: {}}, badPeers)
	if err != nil {
		t.Fatal(err)
	}
}

// serveCheckpointedCFHeaders has the checkpointed sync of the filter headers
// answered from the given chain.
func (rt *repairTest) serveCheckpointedCFHeaders(chain *filterChain) {
	rt.bm.server.queryBatch = func(msgs []wire.Message,
		f func(*ServerPeer, wire.Message, wire.Message) bool,
		_ <-chan struct{}, _ ...QueryOption) {

		for _, msg := range msgs {
			query, ok := msg.(*wire.MsgGetCFHeaders)
			if !ok {
				continue
			}
			f(nil, msg, rt.peers.cfHeaders(chain, query))
		}
	}
}

// waitForFilterTip waits for the stored filter headers to end with the given
// chain's at the given height.
func (rt *repairTest) waitForFilterTip(t *testing.T, chain *filterChain,
	tip uint32) {

	t.Helper()

	deadline := time.Now().Add(time.Minute)
	for {
		header, height, err := rt.cfStore.ChainTip()
		if err == nil && height == tip && *header == chain.headers[tip] {
			return
		}

		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for filter tip %v at height "+
				"%d, got %v at height %d: %v",
				chain.headers[tip], tip, header, height, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// runCFHandler runs the block manager's cfHandler until the test ends. Peers
// to disconnect aren't looked up, as the ChainService has quit.
func runCFHandler(t *testing.T, bm *blockManager) {
	t.Helper()

	bm.server.quit = make(chan struct{})
	close(bm.server.quit)

	stopDraining := drainBlockNtfns(bm)
	done := make(chan struct{})
	go func() {
		defer close(done)
		bm.cfHandler()
	}()

	t.Cleanup(func() {
		close(bm.quit)
		for {
			// cfHandler checks whether to quit once it's woken.
			bm.newHeadersSignal.Broadcast()

			select {
			case <-done:
				stopDraining()
				return
			case <-time.After(50 * time.Millisecond):
			}
		}
	})
}

// TestCFHandlerRepairsCheckpointConflict checks that cfHandler repairs the
// filter headers when our peers agree on filter checkpoints that ours don't
// match, rather than trying to resolve them without end, and then syncs the
// right filter headers from the checkpoints and at the tip.
func TestCFHandlerRepairsCheckpointConflict(t *testing.T) {
	t.Parallel()

	rt := setupRepairTestTo(t, 1500, repairTestTip, true)
	rt.storeFilterChain(t, rt.wrong, repairTestTip)
	rt.peers.chains[repairPeer1] = rt.right
	rt.peers.chains[repairPeer2] = rt.right
	rt.serveCheckpointedCFHeaders(rt.right)
	rollbacks, cancel := rt.bm.subscribeFilterHeaderRollbacks()
	defer cancel()

	runCFHandler(t, rt.bm)

	rt.waitForFilterTip(t, rt.right, repairTestTip)
	select {
	case height := <-rollbacks:
		if height != rt.divergence-1 {
			t.Fatalf("expected rollback to height %d, got %d",
				rt.divergence-1, height)
		}
	default:
		t.Fatal("expected a rollback notification")
	}
	rt.assertBanned(t)
}

// TestCFHandlerRepairsWhileWaitingForHeaders checks that cfHandler repairs the
// filter headers when asked to while it waits for more block headers, as it
// does before they're synced, when a block downloaded or the filters peers
// serve show them wrong.
func TestCFHandlerRepairsWhileWaitingForHeaders(t *testing.T) {
	t.Parallel()

	// Block headers that aren't recent, fewer than a filter checkpoint
	// interval past the filter headers, keep cfHandler waiting for more.
	const tip = wire.CFCheckptInterval / 2
	rt := setupRepairTestTo(t, tip/2, tip, false)
	rt.storeFilterChain(t, rt.wrong, tip)
	rt.peers.chains[repairPeer1] = rt.right
	rollbacks, cancel := rt.bm.subscribeFilterHeaderRollbacks()
	defer cancel()

	runCFHandler(t, rt.bm)
	rt.bm.requestFilterHeaderRepair()

	select {
	case height := <-rollbacks:
		if height != rt.divergence-1 {
			t.Fatalf("expected rollback to height %d, got %d",
				rt.divergence-1, height)
		}
	case <-time.After(time.Minute):
		t.Fatal("expected cfHandler to repair the filter headers")
	}
	rt.assertFilterTip(t, rt.wrong, rt.divergence-1)
	rt.assertBanned(t)
}
