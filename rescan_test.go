package neutrino

import (
	"errors"
	"fmt"
	"os"
	"reflect"
	"runtime/pprof"
	"sync"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/gcash/bchd/chaincfg"
	"github.com/gcash/bchd/chaincfg/chainhash"
	"github.com/gcash/bchd/rpcclient"
	"github.com/gcash/bchd/wire"
	"github.com/gcash/bchutil"
	"github.com/gcash/bchutil/gcs"
	"github.com/gcash/bchutil/gcs/builder"
	"github.com/gcash/bchwallet/waddrmgr"
	"github.com/gcash/neutrino/blockntfns"
)

// mockChainSource is a mock implementation of the ChainSource interface that
// aims to ease testing the behavior of the Rescan struct.
type mockChainSource struct {
	ntfnChan chan blockntfns.BlockNtfn

	// filtersQueried and blocksQueried report filter and block queries
	// made by the rescan. Tests must drain these channels, as the sends
	// block until received.
	filtersQueried chan chainhash.Hash
	blocksQueried  chan chainhash.Hash

	// filterHeaderRollbacks delivers rollbacks of the filter headers to
	// the rescan.
	filterHeaderRollbacks chan uint32

	mu                    sync.Mutex // all fields below are protected
	bestBlock             waddrmgr.BlockStamp
	blockHeightIndex      map[chainhash.Hash]uint32
	blockHashesByHeight   map[uint32]*chainhash.Hash
	blockHeaders          map[chainhash.Hash]*wire.BlockHeader
	blocks                map[chainhash.Hash]*bchutil.Block
	failGetBlock          bool // if true, returns an error in GetBlock
	failGetFilter         bool // if true, returns an error in GetCFilter
	filters               map[chainhash.Hash]*gcs.Filter
	filterHeadersByHeight map[uint32]*chainhash.Hash
	subscribeOverride     func(uint32) (*blockntfns.Subscription, error)

	// filterHeadersTip is the height of the filter headers' tip while
	// they've been rolled back below the best block, and nil otherwise.
	filterHeadersTip *int32

	// subscriptions, if set, receives the height of each attempt to
	// subscribe to block notifications.
	subscriptions chan uint32
}

var _ ChainSource = (*mockChainSource)(nil)

// newMockChainSource creates a new mock chain backed by numBlocks.
func newMockChainSource(numBlocks int) *mockChainSource {
	chain := &mockChainSource{
		ntfnChan:              make(chan blockntfns.BlockNtfn),
		filtersQueried:        make(chan chainhash.Hash),
		blocksQueried:         make(chan chainhash.Hash),
		filterHeaderRollbacks: make(chan uint32, 1),

		blockHeightIndex:      make(map[chainhash.Hash]uint32),
		blockHashesByHeight:   make(map[uint32]*chainhash.Hash),
		blockHeaders:          make(map[chainhash.Hash]*wire.BlockHeader),
		blocks:                make(map[chainhash.Hash]*bchutil.Block),
		filterHeadersByHeight: make(map[uint32]*chainhash.Hash),
		filters:               make(map[chainhash.Hash]*gcs.Filter),
	}

	genesisHash := chain.ChainParams().GenesisHash
	genesisBlock := chain.ChainParams().GenesisBlock

	chain.blockHeightIndex[*genesisHash] = 0
	chain.blockHashesByHeight[0] = genesisHash
	chain.blockHeaders[*genesisHash] = &genesisBlock.Header
	chain.blocks[*genesisHash] = bchutil.NewBlock(genesisBlock)

	filter, _ := gcs.FromBytes(0, builder.DefaultP, builder.DefaultM, nil)
	chain.filters[*genesisHash] = filter

	filterHeader, _ := builder.MakeHeaderForFilter(filter, chainhash.Hash{})
	chain.filterHeadersByHeight[0] = &filterHeader

	chain.bestBlock = waddrmgr.BlockStamp{
		Height:    0,
		Hash:      *genesisHash,
		Timestamp: genesisBlock.Header.Timestamp,
	}

	for i := 0; i < numBlocks-1; i++ {
		chain.addNewBlock(false)
	}

	return chain
}

// addNewBlock advances the chain by one block. The notify boolean can be used
// to notify the new block.
func (c *mockChainSource) addNewBlock(notify bool) waddrmgr.BlockStamp {
	return c.addNewBlockWithHeader(c.nextHeader(), notify)
}

// addNewBlockMatching advances the chain by one block whose filter matches the
// given script, such that the rescan must fetch the block to process it. The
// notify boolean can be used to notify the new block.
func (c *mockChainSource) addNewBlockMatching(script []byte,
	notify bool) waddrmgr.BlockStamp {

	return c.addNewBlockWithHeaderAndScript(c.nextHeader(), script, notify)
}

// nextHeader returns a header for the block that would follow the current best
// block.
func (c *mockChainSource) nextHeader() *wire.BlockHeader {
	c.mu.Lock()
	newHeight := uint32(c.bestBlock.Height + 1)
	prevHash := c.bestBlock.Hash
	c.mu.Unlock()

	genesisTimestamp := c.ChainParams().GenesisBlock.Header.Timestamp
	return &wire.BlockHeader{
		PrevBlock: prevHash,
		Timestamp: genesisTimestamp.Add(
			time.Duration(newHeight) * 10 * time.Minute,
		),
	}
}

// addNewBlock advances the chain by one block with the given header. The notify
// boolean can be used to notify the new block.
//
// NOTE: The header's PrevBlock should properly point to the best block in the
// chain.
func (c *mockChainSource) addNewBlockWithHeader(header *wire.BlockHeader,
	notify bool) waddrmgr.BlockStamp {

	return c.addNewBlockWithHeaderAndScript(header, nil, notify)
}

// addNewBlockWithHeaderAndScript advances the chain by one block with the given
// header. If a script is provided, the block's filter is built to match it,
// otherwise the filter is empty. The notify boolean can be used to notify the
// new block.
//
// NOTE: The header's PrevBlock should properly point to the best block in the
// chain.
func (c *mockChainSource) addNewBlockWithHeaderAndScript(
	header *wire.BlockHeader, script []byte,
	notify bool) waddrmgr.BlockStamp {

	c.mu.Lock()
	newHeight := uint32(c.bestBlock.Height + 1)
	newHash := header.BlockHash()
	c.blockHeightIndex[newHash] = newHeight
	c.blockHashesByHeight[newHeight] = &newHash
	c.blockHeaders[newHash] = header
	c.blocks[newHash] = bchutil.NewBlock(wire.NewMsgBlock(header))

	var newFilter *gcs.Filter
	if script != nil {
		newFilter, _ = builder.WithKeyHash(&newHash).AddEntry(script).
			Build()
	} else {
		newFilter, _ = gcs.FromBytes(
			0, builder.DefaultP, builder.DefaultM, nil,
		)
	}
	c.filters[newHash] = newFilter

	newFilterHeader, _ := builder.MakeHeaderForFilter(
		newFilter, *c.filterHeadersByHeight[newHeight-1],
	)
	c.filterHeadersByHeight[newHeight] = &newFilterHeader

	c.bestBlock.Height++
	c.bestBlock.Hash = newHash
	c.bestBlock.Timestamp = header.Timestamp
	bestBlock := c.bestBlock
	c.mu.Unlock()

	if notify {
		c.ntfnChan <- blockntfns.NewBlockConnected(*header, newHeight)
	}

	return bestBlock
}

// rollback rolls back the chain by one block. The notify boolean can be used to
// notify the stale block.
func (c *mockChainSource) rollback(notify bool) waddrmgr.BlockStamp {
	c.mu.Lock()
	curHeight := uint32(c.bestBlock.Height)
	curHeader := c.blockHeaders[c.bestBlock.Hash]
	prevHeader := c.blockHeaders[curHeader.PrevBlock]

	delete(c.blockHeightIndex, c.bestBlock.Hash)
	delete(c.blockHashesByHeight, curHeight)
	delete(c.blockHeaders, c.bestBlock.Hash)
	delete(c.blocks, c.bestBlock.Hash)

	delete(c.filterHeadersByHeight, curHeight)
	delete(c.filters, c.bestBlock.Hash)

	c.bestBlock.Height--
	c.bestBlock.Hash = curHeader.PrevBlock
	c.bestBlock.Timestamp = prevHeader.Timestamp
	bestBlock := c.bestBlock
	c.mu.Unlock()

	if notify {
		c.ntfnChan <- blockntfns.NewBlockDisconnected(
			*curHeader, curHeight, *prevHeader,
		)
	}

	return bestBlock
}

// rollbackToHeight rolls back the chain to the specified height. The notify
// boolean can be used to notify all stale blocks.
func (c *mockChainSource) rollbackToHeight(height int32,
	notify bool) waddrmgr.BlockStamp {

	c.mu.Lock()
	bestBlock := c.bestBlock
	c.mu.Unlock()

	for bestBlock.Height > height {
		bestBlock = c.rollback(notify)
	}

	return bestBlock
}

// ChainParams returns the parameters of the current chain.
func (c *mockChainSource) ChainParams() chaincfg.Params {
	return chaincfg.MainNetParams
}

// BestBlock retrieves the most recent block's height and hash where we have
// both the header and filter header ready.
func (c *mockChainSource) BestBlock() (*waddrmgr.BlockStamp, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.filterHeadersTip != nil {
		stamp := c.blockStampLocked(*c.filterHeadersTip)
		return &stamp, nil
	}

	// Return a copy to prevent mutating internal state.
	return &waddrmgr.BlockStamp{
		Height:    c.bestBlock.Height,
		Hash:      c.bestBlock.Hash,
		Timestamp: c.bestBlock.Timestamp,
	}, nil
}

// blockStamp returns the stamp of the block at the given height.
func (c *mockChainSource) blockStamp(height int32) waddrmgr.BlockStamp {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.blockStampLocked(height)
}

// blockStampLocked returns the stamp of the block at the given height.
//
// NOTE: The mutex must be held.
func (c *mockChainSource) blockStampLocked(height int32) waddrmgr.BlockStamp {
	hash := c.blockHashesByHeight[uint32(height)]
	return waddrmgr.BlockStamp{
		Height:    height,
		Hash:      *hash,
		Timestamp: c.blockHeaders[*hash].Timestamp,
	}
}

// setFilterMatching replaces the filter of the block with the given hash with
// one that matches the given script, as the filters checked against filter
// headers that were wrong are replaced once those are rolled back.
func (c *mockChainSource) setFilterMatching(hash chainhash.Hash,
	script []byte) {

	c.mu.Lock()
	defer c.mu.Unlock()

	c.filters[hash], _ = builder.WithKeyHash(&hash).AddEntry(script).Build()
}

// rollBackFilterHeaders rolls the filter headers back to the given height,
// leaving the blocks above it in the chain, as a repair does once it finds that
// they were wrong above it. The notify boolean can be used to notify the
// rollback.
func (c *mockChainSource) rollBackFilterHeaders(height int32, notify bool) {
	c.mu.Lock()
	c.filterHeadersTip = &height
	c.mu.Unlock()

	if notify {
		c.filterHeaderRollbacks <- uint32(height)
	}
}

// syncFilterHeader advances the filter headers' tip by a block after they were
// rolled back, as they're synced again. The notify boolean can be used to
// notify the block.
func (c *mockChainSource) syncFilterHeader(notify bool) waddrmgr.BlockStamp {
	c.mu.Lock()
	height := *c.filterHeadersTip + 1
	if height < c.bestBlock.Height {
		c.filterHeadersTip = &height
	} else {
		c.filterHeadersTip = nil
	}
	block := c.blockStampLocked(height)
	header := *c.blockHeaders[block.Hash]
	c.mu.Unlock()

	if notify {
		c.ntfnChan <- blockntfns.NewBlockConnected(header, uint32(height))
	}

	return block
}

// SubscribeFilterHeaderRollbacks returns a channel that receives the height the
// filter headers were rolled back to each time they are, along with a function
// that cancels the subscription.
func (c *mockChainSource) SubscribeFilterHeaderRollbacks() (<-chan uint32,
	func()) {

	return c.filterHeaderRollbacks, func() {}
}

// watchSubscriptions returns a channel that receives the height of each attempt
// to subscribe to block notifications from now on.
func (c *mockChainSource) watchSubscriptions() <-chan uint32 {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.subscriptions = make(chan uint32, 10)
	return c.subscriptions
}

// GetBlockHeaderByHeight returns the header of the block with the given height.
func (c *mockChainSource) GetBlockHeaderByHeight(
	height uint32) (*wire.BlockHeader, error) {

	c.mu.Lock()
	defer c.mu.Unlock()

	blockHash, ok := c.blockHashesByHeight[height]
	if !ok {
		return nil, errors.New("block height not found")
	}
	blockHeader, ok := c.blockHeaders[*blockHash]
	if !ok {
		return nil, errors.New("block header not found")
	}
	return blockHeader, nil
}

// GetBlockHeader returns the header of the block with the given hash.
func (c *mockChainSource) GetBlockHeader(
	hash *chainhash.Hash) (*wire.BlockHeader, uint32, error) {

	c.mu.Lock()
	defer c.mu.Unlock()

	blockHeader, ok := c.blockHeaders[*hash]
	if !ok {
		return nil, 0, errors.New("block header not found")
	}
	blockHeight, ok := c.blockHeightIndex[*hash]
	if !ok {
		return nil, 0, errors.New("block height not found")
	}
	return blockHeader, blockHeight, nil
}

// GetBlock returns the block with the given hash.
func (c *mockChainSource) GetBlock(hash chainhash.Hash,
	_ ...QueryOption) (*bchutil.Block, error) {

	defer func() {
		c.blocksQueried <- hash
	}()

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.failGetBlock {
		return nil, errors.New("failed block")
	}

	block, ok := c.blocks[hash]
	if !ok {
		return nil, errors.New("block not found")
	}
	return block, nil
}

// GetFilterHeaderByHeight returns the filter header of the block with the given
// height.
func (c *mockChainSource) GetFilterHeaderByHeight(
	height uint32) (*chainhash.Hash, error) {

	c.mu.Lock()
	defer c.mu.Unlock()

	filterHeader, ok := c.filterHeadersByHeight[height]
	if !ok || (c.filterHeadersTip != nil &&
		height > uint32(*c.filterHeadersTip)) {

		return nil, errors.New("filter header not found")
	}
	return filterHeader, nil
}

// setFailGetBlock determines whether we should fail to retrieve blocks.
func (c *mockChainSource) setFailGetBlock(b bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failGetBlock = b
}

// setFailGetFilter determines whether we should fail to retrieve for a block.
func (c *mockChainSource) setFailGetFilter(b bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failGetFilter = b
}

// GetCFilter returns the filter of the given type for the block with the given
// hash.
func (c *mockChainSource) GetCFilter(hash chainhash.Hash,
	filterType wire.FilterType, options ...QueryOption) (*gcs.Filter, error) {

	defer func() {
		c.filtersQueried <- hash
	}()

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.failGetFilter {
		return nil, errors.New("failed filter")
	}

	filter, ok := c.filters[hash]
	if !ok {
		return nil, errors.New("filter not found")
	}
	return filter, nil
}

// overrideSubscribe allows us to override the mockChainSource's Subscribe
// method implementation to ease testing.
func (c *mockChainSource) overrideSubscribe(
	f func(uint32) (*blockntfns.Subscription, error)) {

	c.mu.Lock()
	defer c.mu.Unlock()
	c.subscribeOverride = f
}

// Subscribe returns a block subscription that delivers block notifications in
// order. The bestHeight parameter can be used to signal that a backlog of
// notifications should be delivered from this height. When providing a
// bestHeight of 0, a backlog will not be delivered.
func (c *mockChainSource) Subscribe(
	bestHeight uint32) (*blockntfns.Subscription, error) {

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.subscriptions != nil {
		c.subscriptions <- bestHeight
	}

	// Like the subscription manager, we'll refuse to deliver notifications
	// from above the filter headers' tip.
	tip := c.bestBlock.Height
	if c.filterHeadersTip != nil {
		tip = *c.filterHeadersTip
	}
	if bestHeight > uint32(tip) {
		return nil, fmt.Errorf("request with height %d is greater "+
			"than best height known %d", bestHeight, tip)
	}

	if c.subscribeOverride != nil {
		return c.subscribeOverride(bestHeight)
	}

	for i := bestHeight + 1; i <= uint32(tip); i++ {
		hash := c.blockHashesByHeight[i]
		header := c.blockHeaders[*hash]
		c.ntfnChan <- blockntfns.NewBlockConnected(*header, i)
	}

	return &blockntfns.Subscription{
		Notifications: c.ntfnChan,
		Cancel:        func() {},
	}, nil
}

// rescanTestContext serves as a harness to aid testing the Rescan struct.
type rescanTestContext struct {
	t                  *testing.T
	chain              *mockChainSource
	blocksConnected    chan waddrmgr.BlockStamp
	blocksDisconnected chan waddrmgr.BlockStamp
	rescan             *Rescan
	errChan            <-chan error
	quit               chan struct{}
}

// newRescanTestContext creates a new test harness for the Rescan struct backed
// by a chain of numBlocks.
func newRescanTestContext(t *testing.T, numBlocks int,
	options ...RescanOption) *rescanTestContext {

	blocksConnected := make(chan waddrmgr.BlockStamp)
	blocksDisconnected := make(chan waddrmgr.BlockStamp)
	ntfnHandlers := rpcclient.NotificationHandlers{
		OnFilteredBlockConnected: func(height int32,
			header *wire.BlockHeader, _ []*bchutil.Tx) {

			blocksConnected <- waddrmgr.BlockStamp{
				Hash:      header.BlockHash(),
				Height:    height,
				Timestamp: header.Timestamp,
			}
		},
		OnFilteredBlockDisconnected: func(height int32,
			header *wire.BlockHeader) {

			blocksDisconnected <- waddrmgr.BlockStamp{
				Hash:      header.BlockHash(),
				Height:    height,
				Timestamp: header.Timestamp,
			}
		},
	}
	quit := make(chan struct{})

	chain := newMockChainSource(numBlocks)
	rescanOptions := []RescanOption{
		NotificationHandlers(ntfnHandlers), QuitChan(quit),
	}
	rescanOptions = append(rescanOptions, options...)
	rescan := NewRescan(chain, rescanOptions...)

	return &rescanTestContext{
		t:                  t,
		chain:              chain,
		blocksConnected:    blocksConnected,
		blocksDisconnected: blocksDisconnected,
		rescan:             rescan,
		quit:               quit,
	}
}

// start starts the backing rescan.
func (ctx *rescanTestContext) start(waitUntilSynced bool) {
	if !waitUntilSynced {
		ctx.errChan = ctx.rescan.Start()
		return
	}

	// Override the mock chain subscribe implementation so that it delivers
	// a signal once synced.
	signal := make(chan struct{})
	deliverSyncSignal := func(bestHeight uint32) (*blockntfns.Subscription, error) {
		if bestHeight == uint32(ctx.chain.bestBlock.Height) {
			close(signal)
		}
		return &blockntfns.Subscription{
			Notifications: ctx.chain.ntfnChan,
			Cancel:        func() {},
		}, nil
	}
	ctx.chain.overrideSubscribe(deliverSyncSignal)

	// Start the rescan and wait for the signal to be delivered.
	ctx.errChan = ctx.rescan.Start()

	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		ctx.t.Fatal("expected to receive synced signal")
	}

	// Once done, we can revert to our original subscribe implementation.
	ctx.chain.overrideSubscribe(nil)
}

// stop stops the rescan test harness and ensures it exits cleanly.
func (ctx *rescanTestContext) stop() {
	close(ctx.quit)

	select {
	case err := <-ctx.errChan:
		if err != ErrRescanExit {
			ctx.t.Fatalf("expected ErrRescanExit upon shutdown, "+
				"got %v", err)
		}

	case <-time.After(time.Second):
		_ = pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
		ctx.t.Fatal("rescan did not exit cleanly")
	}

	select {
	case <-ctx.blocksConnected:
		ctx.t.Fatalf("received unexpected block connected after shutdown")
	case <-ctx.blocksDisconnected:
		ctx.t.Fatalf("received unexpected block disconnected after shutdown")
	case <-ctx.chain.filtersQueried:
		ctx.t.Fatalf("received unexpected filter query after shutdown")
	case <-ctx.chain.blocksQueried:
		ctx.t.Fatalf("received unexpected block query after shutdown")
	default:
	}
}

// recvBlockConnected is a helper method used to expect a block connected
// notification being delivered.
func (ctx *rescanTestContext) recvBlockConnected(block waddrmgr.BlockStamp) {
	ctx.t.Helper()

	select {
	case recvBlock := <-ctx.blocksConnected:
		if !reflect.DeepEqual(recvBlock, block) {
			ctx.t.Fatalf("expected block connected notification "+
				"for %v, got %v", spew.Sdump(block),
				spew.Sdump(recvBlock))
		}
	case <-time.After(time.Second):
		ctx.t.Fatalf("expected to receive block connected "+
			"notification for %v", spew.Sdump(block))
	}
}

// recvBlockDisconnected is a helper method used to expect a block disconnected
// notification being delivered.
func (ctx *rescanTestContext) recvBlockDisconnected(block waddrmgr.BlockStamp) {
	ctx.t.Helper()

	ctx.recvBlockDisconnectedWhileRetrying(block, nil)
}

// recvBlockDisconnectedWhileRetrying expects a block disconnected notification
// for the given block, servicing any filter or block queries the rescan issues
// for the block it's retrying, if one is given, in the meantime.
func (ctx *rescanTestContext) recvBlockDisconnectedWhileRetrying(
	block waddrmgr.BlockStamp, retrying *chainhash.Hash) {

	ctx.t.Helper()

	// A nil channel is never ready, so we won't service queries if we
	// aren't retrying a block.
	var filtersQueried, blocksQueried chan chainhash.Hash
	if retrying != nil {
		filtersQueried = ctx.chain.filtersQueried
		blocksQueried = ctx.chain.blocksQueried
	}

	timeout := time.After(time.Second)
	for {
		select {
		case recvBlock := <-ctx.blocksDisconnected:
			if !reflect.DeepEqual(recvBlock, block) {
				ctx.t.Fatalf("expected block disconnected "+
					"notification for %v, got %v",
					spew.Sdump(block),
					spew.Sdump(recvBlock))
			}
			return

		case hashQueried := <-filtersQueried:
			if !hashQueried.IsEqual(retrying) {
				ctx.t.Fatalf("expected to query filter %v, "+
					"got %v", retrying, hashQueried)
			}

		case hashQueried := <-blocksQueried:
			if !hashQueried.IsEqual(retrying) {
				ctx.t.Fatalf("expected to query block %v, "+
					"got %v", retrying, hashQueried)
			}

		case <-timeout:
			ctx.t.Fatalf("expected to receive block disconnected "+
				"notification for %v", spew.Sdump(block))
		}
	}
}

// assertSubscribed asserts that the next attempt to subscribe to block
// notifications, as reported by the channel watchSubscriptions returned, was
// from the given height.
func (ctx *rescanTestContext) assertSubscribed(subscriptions <-chan uint32,
	height int32) {

	ctx.t.Helper()

	select {
	case subscribed := <-subscriptions:
		if subscribed != uint32(height) {
			ctx.t.Fatalf("expected to subscribe from height %d, "+
				"got %d", height, subscribed)
		}
	case <-time.After(time.Second):
		ctx.t.Fatalf("expected to subscribe from height %d", height)
	}
}

// assertNotSubscribed asserts that there's no attempt to subscribe to block
// notifications, as reported by the channel watchSubscriptions returned, for a
// while.
func (ctx *rescanTestContext) assertNotSubscribed(
	subscriptions <-chan uint32) {

	ctx.t.Helper()

	select {
	case subscribed := <-subscriptions:
		ctx.t.Fatalf("expected no attempt to subscribe, got one from "+
			"height %d", subscribed)
	case <-time.After(300 * time.Millisecond):
	}
}

// assertFilterQueried asserts that a filter for the given block hash was
// queried for.
func (ctx *rescanTestContext) assertFilterQueried(hash chainhash.Hash) {
	ctx.t.Helper()

	var hashQueried chainhash.Hash
	select {
	case hashQueried = <-ctx.chain.filtersQueried:
		if !hashQueried.IsEqual(&hash) {
			ctx.t.Fatalf("expected to query filter %v, got %v",
				hash, hashQueried)
		}
	case <-time.After(time.Second):
		ctx.t.Fatal("expected rescan to query for filter")
	}
}

// assertBlockQueried asserts that the block with the given hash was queried
// for.
func (ctx *rescanTestContext) assertBlockQueried(hash chainhash.Hash) {
	ctx.t.Helper()

	select {
	case hashQueried := <-ctx.chain.blocksQueried:
		if !hashQueried.IsEqual(&hash) {
			ctx.t.Fatalf("expected to query block %v, got %v",
				hash, hashQueried)
		}
	case <-time.After(time.Second):
		ctx.t.Fatal("expected rescan to query for block")
	}
}

// assertRunning asserts that the rescan has not exited.
func (ctx *rescanTestContext) assertRunning() {
	ctx.t.Helper()

	select {
	case err := <-ctx.errChan:
		ctx.t.Fatalf("rescan exited unexpectedly: %v", err)
	default:
	}
}

// updateWhileRetrying sends an update to the rescan while it is retrying the
// block with the given hash. Any filter or block queries issued by the rescan
// in the meantime are serviced so that the update is guaranteed to be
// delivered without deadlocking the mock chain.
func (ctx *rescanTestContext) updateWhileRetrying(hash chainhash.Hash,
	options ...UpdateOption) {

	ctx.t.Helper()

	updateErr := make(chan error, 1)
	go func() {
		updateErr <- ctx.rescan.Update(options...)
	}()

	timeout := time.After(time.Second)
	for {
		select {
		case err := <-updateErr:
			if err != nil {
				ctx.t.Fatalf("unable to update rescan while "+
					"retrying block: %v", err)
			}
			return

		case hashQueried := <-ctx.chain.filtersQueried:
			if !hashQueried.IsEqual(&hash) {
				ctx.t.Fatalf("expected to query filter %v, "+
					"got %v", hash, hashQueried)
			}

		case hashQueried := <-ctx.chain.blocksQueried:
			if !hashQueried.IsEqual(&hash) {
				ctx.t.Fatalf("expected to query block %v, "+
					"got %v", hash, hashQueried)
			}

		case <-timeout:
			ctx.t.Fatal("expected update to be applied while " +
				"retrying block")
		}
	}
}

// recvBlockConnectedAfterRetries expects a block connected notification for
// the given block, servicing any filter or block queries the rescan issues
// for it in the meantime. This is useful once a previously failing fetch has
// been allowed to succeed, as the exact number of retries in flight is not
// deterministic.
func (ctx *rescanTestContext) recvBlockConnectedAfterRetries(
	block waddrmgr.BlockStamp) {

	ctx.t.Helper()

	timeout := time.After(time.Second)
	for {
		select {
		case recvBlock := <-ctx.blocksConnected:
			if !reflect.DeepEqual(recvBlock, block) {
				ctx.t.Fatalf("expected block connected "+
					"notification for %v, got %v",
					spew.Sdump(block),
					spew.Sdump(recvBlock))
			}
			return

		case hashQueried := <-ctx.chain.filtersQueried:
			if !hashQueried.IsEqual(&block.Hash) {
				ctx.t.Fatalf("expected to query filter %v, "+
					"got %v", block.Hash, hashQueried)
			}

		case hashQueried := <-ctx.chain.blocksQueried:
			if !hashQueried.IsEqual(&block.Hash) {
				ctx.t.Fatalf("expected to query block %v, "+
					"got %v", block.Hash, hashQueried)
			}

		case <-timeout:
			ctx.t.Fatalf("expected to receive block connected "+
				"notification for %v", spew.Sdump(block))
		}
	}
}

// TestRescanNewBlockAfterRetry ensures that the rescan can properly queue
// blocks for which we are missing a filter for.
func TestRescanNewBlockAfterRetry(t *testing.T) {
	t.Parallel()

	// Dummy options to allow the Rescan to fetch block filters.
	ctx := newRescanTestContext(t, 10, []RescanOption{
		StartTime(time.Time{}),
		WatchInputs(InputWithScript{}),
	}...)
	ctx.start(true)
	defer ctx.stop()

	// Modify the mocked chain such that it fails to retrieve block filters.
	ctx.chain.setFailGetFilter(true)

	// Adding multiple new blocks to the chain should result in the first
	// retry block being queried until successful.
	block1 := ctx.chain.addNewBlock(true)
	ctx.assertFilterQueried(block1.Hash)
	block2 := ctx.chain.addNewBlock(true)
	ctx.assertFilterQueried(block1.Hash)

	// Revert the mocked chain so that block filters can be retrieved
	// successfully.
	ctx.chain.setFailGetFilter(false)

	// The blocks should now be notified in their expected order.
	ctx.assertFilterQueried(block1.Hash)
	ctx.recvBlockConnected(block1)
	ctx.assertFilterQueried(block2.Hash)
	ctx.recvBlockConnected(block2)
}

// TestRescanReorgAllButFirstRetryBlocks ensures the rescan stops retrying
// blocks for which it has received disconnected notifications for.
func TestRescanReorgAllButFirstRetryBlocks(t *testing.T) {
	t.Parallel()

	// Dummy options to allow the Rescan to fetch block filters.
	ctx := newRescanTestContext(t, 10, []RescanOption{
		StartTime(time.Time{}),
		WatchInputs(InputWithScript{}),
	}...)
	ctx.start(true)
	defer ctx.stop()

	// Modify the mocked chain such that it fails to retrieve block filters.
	ctx.chain.setFailGetFilter(true)

	// Adding multiple new blocks to the chain should result in the first
	// retry block being queried until successful.
	block := ctx.chain.addNewBlock(true)
	ctx.assertFilterQueried(block.Hash)
	ctx.chain.addNewBlock(true)
	ctx.assertFilterQueried(block.Hash)
	ctx.chain.addNewBlock(true)
	ctx.assertFilterQueried(block.Hash)

	// We'll then reorg back to the first retry block. This should cause us
	// to stop retrying the following reorged blocks.
	ctx.chain.rollbackToHeight(block.Height, true)

	// Revert the mocked chain so that block filters can be retrieved
	// successfully.
	ctx.chain.setFailGetFilter(false)

	// Since the first block wasn't reorged, we should expect a notification
	// for it.
	ctx.assertFilterQueried(block.Hash)
	ctx.recvBlockConnected(block)
}

// TestRescanReorgAllRetryBlocks ensures the rescan stops retrying blocks for
// which it has received disconnected notifications for and queries for blocks
// of the new chain instead.
func TestRescanReorgAllRetryBlocks(t *testing.T) {
	t.Parallel()

	// Dummy options to allow the Rescan to fetch block filters.
	ctx := newRescanTestContext(t, 10, []RescanOption{
		StartTime(time.Time{}),
		WatchInputs(InputWithScript{}),
	}...)
	ctx.start(true)
	defer ctx.stop()

	// Note the block which we'll revert back to.
	bestBlock, err := ctx.chain.BestBlock()
	if err != nil {
		ctx.t.Fatalf("unable to retrieve best block: %v", err)
	}

	// Modify the mocked chain such that it fails to retrieve block filters.
	ctx.chain.setFailGetFilter(true)

	// Adding multiple new blocks to the chain should result in the first
	// retry block being queried until successful.
	block1 := ctx.chain.addNewBlock(true)
	ctx.assertFilterQueried(block1.Hash)
	block2 := ctx.chain.addNewBlock(true)
	ctx.assertFilterQueried(block1.Hash)

	// We'll then reorg back to a height where the new blocks we've added
	// and retried are stale, causing us to no longer retry them. The rescan
	// won't deliver any disconnected notifications to its caller since it
	// never delivered connected notifications for them.
	ctx.chain.rollbackToHeight(bestBlock.Height, true)

	// Revert the mocked chain so that block filters can be retrieved
	// successfully.
	ctx.chain.setFailGetFilter(false)

	// We'll then add two new blocks from a different chain to ensure the
	// rescan can properly follow it.
	newBlock1 := ctx.chain.addNewBlockWithHeader(
		&wire.BlockHeader{PrevBlock: bestBlock.Hash}, true,
	)
	if newBlock1.Hash == block1.Hash {
		ctx.t.Fatal("expected different hashes for blocks on " +
			"different chains")
	}
	ctx.assertFilterQueried(newBlock1.Hash)
	ctx.recvBlockConnected(newBlock1)

	newBlock2 := ctx.chain.addNewBlock(true)
	if newBlock2.Hash == block2.Hash {
		ctx.t.Fatal("expected different hashes for blocks on " +
			"different chains")
	}
	ctx.assertFilterQueried(newBlock2.Hash)
	ctx.recvBlockConnected(newBlock2)
}

// TestRescanRetryBlocksAfterCatchingUp ensures the rescan stops retrying blocks
// after catching up with the chain. Catching up assumes that we already have an
// accurate representation of the chain.
func TestRescanRetryBlocksAfterCatchingUp(t *testing.T) {
	t.Parallel()

	// Dummy options to allow the Rescan to fetch block filters.
	ctx := newRescanTestContext(t, 10, []RescanOption{
		StartTime(time.Time{}),
		WatchInputs(InputWithScript{}),
	}...)
	ctx.start(true)
	defer ctx.stop()

	// Modify the mocked chain such that it fails to retrieve block filters.
	ctx.chain.setFailGetFilter(true)

	// Adding multiple new blocks to the chain should result in the first
	// retry block being queried until successful.
	block1 := ctx.chain.addNewBlock(true)
	ctx.assertFilterQueried(block1.Hash)

	// We'll prevent notifying new blocks and instead notify an invalid
	// block which should prompt the rescan to catch up with the chain
	// manually.
	block2 := ctx.chain.addNewBlock(false)
	block3 := ctx.chain.addNewBlock(false)
	ctx.chain.ntfnChan <- blockntfns.NewBlockConnected(wire.BlockHeader{}, 0)

	// Revert the mocked chain so that block filters can be retrieved
	// successfully.
	ctx.chain.setFailGetFilter(false)

	// The notifications for all the blocks added above should now be
	// delivered as part of catching up.
	ctx.assertFilterQueried(block1.Hash)
	ctx.recvBlockConnected(block1)
	ctx.assertFilterQueried(block2.Hash)
	ctx.recvBlockConnected(block2)
	ctx.assertFilterQueried(block3.Hash)
	ctx.recvBlockConnected(block3)
}

// TestRescanResubscribeWithRetryPending ensures that a rescan that catches up
// with the chain while it retries a block leaves the subscription it replaces
// alone. The subscription manager closes the channel of a canceled
// subscription, and the timer that used to retry a block sent the block on
// that channel, so it panicked once the rescan replaced the subscription.
func TestRescanResubscribeWithRetryPending(t *testing.T) {
	t.Parallel()

	// Dummy options to allow the Rescan to fetch block filters.
	ctx := newRescanTestContext(t, 10, []RescanOption{
		StartTime(time.Time{}),
		WatchInputs(InputWithScript{}),
	}...)

	// Hand out subscriptions that close their channel once canceled, as
	// the subscription manager's do.
	subscriptions := make(chan chan blockntfns.BlockNtfn, 10)
	ctx.chain.overrideSubscribe(func(uint32) (*blockntfns.Subscription,
		error) {

		ntfns := make(chan blockntfns.BlockNtfn)
		subscriptions <- ntfns

		var canceled sync.Once
		return &blockntfns.Subscription{
			Notifications: ntfns,
			Cancel: func() {
				canceled.Do(func() { close(ntfns) })
			},
		}, nil
	})
	nextSubscription := func() chan blockntfns.BlockNtfn {
		t.Helper()

		select {
		case ntfns := <-subscriptions:
			return ntfns
		case <-time.After(time.Second):
			t.Fatal("expected the rescan to subscribe")
			return nil
		}
	}
	notifyConnected := func(ntfns chan blockntfns.BlockNtfn,
		block waddrmgr.BlockStamp) {

		t.Helper()

		header, _, err := ctx.chain.GetBlockHeader(&block.Hash)
		if err != nil {
			t.Fatalf("unable to get block header: %v", err)
		}
		ntfns <- blockntfns.NewBlockConnected(
			*header, uint32(block.Height),
		)
	}

	ctx.errChan = ctx.rescan.Start()
	defer ctx.stop()
	ntfns := nextSubscription()

	// Modify the mocked chain such that it fails to retrieve block filters,
	// so that the rescan retries the new block.
	ctx.chain.setFailGetFilter(true)
	block1 := ctx.chain.addNewBlock(false)
	notifyConnected(ntfns, block1)
	ctx.assertFilterQueried(block1.Hash)

	// An invalid block prompts the rescan to catch up with the chain
	// manually and then replace its subscription. We'll give a retry time
	// to come due before then.
	block2 := ctx.chain.addNewBlock(false)
	ntfns <- blockntfns.NewBlockConnected(wire.BlockHeader{}, 0)
	time.Sleep(300 * time.Millisecond)

	// Revert the mocked chain so that block filters can be retrieved
	// successfully. Both blocks should then be notified in order.
	ctx.chain.setFailGetFilter(false)

	var connected []waddrmgr.BlockStamp
	for len(connected) < 2 {
		select {
		case <-ctx.chain.filtersQueried:
		case block := <-ctx.blocksConnected:
			connected = append(connected, block)
		case <-time.After(time.Second):
			t.Fatalf("expected two block connected notifications, "+
				"got %v", spew.Sdump(connected))
		}
	}
	want := []waddrmgr.BlockStamp{block1, block2}
	if !reflect.DeepEqual(connected, want) {
		t.Fatalf("expected block connected notifications for %v, "+
			"got %v", spew.Sdump(want), spew.Sdump(connected))
	}

	// The rescan should then follow the chain on its new subscription.
	ntfns = nextSubscription()
	block3 := ctx.chain.addNewBlock(false)
	notifyConnected(ntfns, block3)
	ctx.assertFilterQueried(block3.Hash)
	ctx.recvBlockConnected(block3)
}

// testWatchScript is a script watched by the rescan in tests that need blocks
// whose filters match, forcing the rescan to fetch the block itself.
var testWatchScript = []byte{0x51} // OP_TRUE

// TestRescanRetryBlockFetchWhenCurrent ensures that a failure to fetch a
// matching block while the rescan is current with the chain results in the
// block being retried rather than the rescan falling back to catching up and
// ending.
func TestRescanRetryBlockFetchWhenCurrent(t *testing.T) {
	t.Parallel()

	ctx := newRescanTestContext(t, 10, []RescanOption{
		StartTime(time.Time{}),
		WatchInputs(InputWithScript{PkScript: testWatchScript}),
	}...)
	ctx.start(true)
	defer ctx.stop()

	// Modify the mocked chain such that it fails to retrieve blocks.
	ctx.chain.setFailGetBlock(true)

	// A new block matching our watch list should have its filter and then
	// its block queried. As the block fetch fails, the rescan should keep
	// retrying the block.
	block := ctx.chain.addNewBlockMatching(testWatchScript, true)
	ctx.assertFilterQueried(block.Hash)
	ctx.assertBlockQueried(block.Hash)
	ctx.assertFilterQueried(block.Hash)
	ctx.assertBlockQueried(block.Hash)
	ctx.assertRunning()

	// Updates must still be applied while the block is being retried, as
	// wallets rely on this to register new addresses.
	ctx.updateWhileRetrying(block.Hash, AddInputs(InputWithScript{}))

	// Revert the mocked chain so that blocks can be retrieved successfully.
	// The block should then be notified.
	ctx.chain.setFailGetBlock(false)
	ctx.recvBlockConnectedAfterRetries(block)

	// Subsequent blocks should be processed as usual.
	block2 := ctx.chain.addNewBlock(true)
	ctx.assertFilterQueried(block2.Hash)
	ctx.recvBlockConnected(block2)

	ctx.assertRunning()
	if err := ctx.rescan.Update(AddInputs(InputWithScript{})); err != nil {
		t.Fatalf("unable to update rescan: %v", err)
	}
}

// TestRescanRetryBlockFetchWhileCatchingUp ensures that a failure to fetch a
// matching block while the rescan is catching up with the chain results in the
// block being retried rather than the rescan ending.
func TestRescanRetryBlockFetchWhileCatchingUp(t *testing.T) {
	t.Parallel()

	ctx := newRescanTestContext(t, 10, []RescanOption{
		StartTime(time.Time{}),
		WatchInputs(InputWithScript{PkScript: testWatchScript}),
	}...)
	ctx.start(true)
	defer ctx.stop()

	// Modify the mocked chain such that it fails to retrieve blocks.
	ctx.chain.setFailGetBlock(true)

	// We'll add a matching block without notifying it and instead notify
	// an invalid block, which should prompt the rescan to catch up with
	// the chain manually.
	block := ctx.chain.addNewBlockMatching(testWatchScript, false)
	ctx.chain.ntfnChan <- blockntfns.NewBlockConnected(
		wire.BlockHeader{}, 0,
	)

	// While catching up, the block's filter and then the block itself
	// should be queried. As the block fetch fails, the rescan should keep
	// retrying the block.
	ctx.assertFilterQueried(block.Hash)
	ctx.assertBlockQueried(block.Hash)
	ctx.assertFilterQueried(block.Hash)
	ctx.assertBlockQueried(block.Hash)
	ctx.assertRunning()

	// Updates must still be applied while the block is being retried, as
	// wallets rely on this to register new addresses.
	ctx.updateWhileRetrying(block.Hash, AddInputs(InputWithScript{}))

	// Revert the mocked chain so that blocks can be retrieved successfully.
	// The block should then be notified.
	ctx.chain.setFailGetBlock(false)
	ctx.recvBlockConnectedAfterRetries(block)

	ctx.assertRunning()
	if err := ctx.rescan.Update(AddInputs(InputWithScript{})); err != nil {
		t.Fatalf("unable to update rescan: %v", err)
	}
}

// TestRescanRetryFilterFetchWhileCatchingUp ensures that a failure to fetch a
// block's filter while the rescan is catching up with the chain results in the
// block being retried rather than the rescan ending.
func TestRescanRetryFilterFetchWhileCatchingUp(t *testing.T) {
	t.Parallel()

	ctx := newRescanTestContext(t, 10, []RescanOption{
		StartTime(time.Time{}),
		WatchInputs(InputWithScript{}),
	}...)
	ctx.start(true)
	defer ctx.stop()

	// Modify the mocked chain such that it fails to retrieve block filters.
	ctx.chain.setFailGetFilter(true)

	// We'll add a block without notifying it and instead notify an invalid
	// block, which should prompt the rescan to catch up with the chain
	// manually.
	block := ctx.chain.addNewBlock(false)
	ctx.chain.ntfnChan <- blockntfns.NewBlockConnected(
		wire.BlockHeader{}, 0,
	)

	// While catching up, the block's filter should be queried until it
	// succeeds.
	ctx.assertFilterQueried(block.Hash)
	ctx.assertFilterQueried(block.Hash)
	ctx.assertRunning()

	// Revert the mocked chain so that block filters can be retrieved
	// successfully. The block should then be notified.
	ctx.chain.setFailGetFilter(false)
	ctx.recvBlockConnectedAfterRetries(block)

	ctx.assertRunning()
	if err := ctx.rescan.Update(AddInputs(InputWithScript{})); err != nil {
		t.Fatalf("unable to update rescan: %v", err)
	}
}

// newRollbackTestContext creates a test harness for a rescan watching
// testWatchScript from the tip of a chain of 10 blocks, with the given start
// time, and starts it once it's current with the chain.
func newRollbackTestContext(t *testing.T,
	startTime time.Time) *rescanTestContext {

	ctx := newRescanTestContext(t, 10, []RescanOption{
		StartTime(startTime),
		WatchInputs(InputWithScript{PkScript: testWatchScript}),
	}...)
	ctx.start(true)

	return ctx
}

// scanNewBlock adds a block to the chain and expects the rescan to scan it.
func (ctx *rescanTestContext) scanNewBlock() waddrmgr.BlockStamp {
	ctx.t.Helper()

	block := ctx.chain.addNewBlock(true)
	ctx.assertFilterQueried(block.Hash)
	ctx.recvBlockConnected(block)

	return block
}

// TestRescanFilterHeaderRollbackWhenCurrent ensures that a rescan that's current
// with the chain rewinds when the filter headers are rolled back below blocks
// it has scanned, and scans those blocks again as the filter headers are synced
// again, with the filters checked against them.
func TestRescanFilterHeaderRollbackWhenCurrent(t *testing.T) {
	t.Parallel()

	ctx := newRollbackTestContext(t, time.Time{})
	defer ctx.stop()

	// The rescan scans three new blocks. The second pays us, although the
	// filter it's served doesn't match, as when it was checked against
	// filter headers that were wrong.
	block1 := ctx.scanNewBlock()
	block2 := ctx.scanNewBlock()
	block3 := ctx.scanNewBlock()

	// A repair then finds the filter headers wrong above the first block
	// and rolls them back to it. The second block's filter, checked against
	// the filter headers that replace them, matches.
	subscriptions := ctx.chain.watchSubscriptions()
	ctx.chain.setFilterMatching(block2.Hash, testWatchScript)
	ctx.chain.rollBackFilterHeaders(block1.Height, true)

	// The rescan should rewind to the first block, notifying the blocks
	// above it as disconnected, and follow the chain from there.
	ctx.recvBlockDisconnected(block3)
	ctx.recvBlockDisconnected(block2)
	ctx.assertSubscribed(subscriptions, block1.Height)

	// As the filter headers are synced again, the rescan should scan the
	// blocks again, fetching the second as its filter now matches.
	ctx.chain.syncFilterHeader(true)
	ctx.assertFilterQueried(block2.Hash)
	ctx.assertBlockQueried(block2.Hash)
	ctx.recvBlockConnected(block2)
	ctx.chain.syncFilterHeader(true)
	ctx.assertFilterQueried(block3.Hash)
	ctx.recvBlockConnected(block3)

	// Subsequent blocks should be processed as usual.
	ctx.scanNewBlock()
}

// TestRescanFilterHeaderRollbackAboveScannedBlocks ensures that a rescan
// ignores a rollback of the filter headers to the block it's at or above it, as
// it hasn't scanned any blocks above it.
func TestRescanFilterHeaderRollbackAboveScannedBlocks(t *testing.T) {
	t.Parallel()

	ctx := newRollbackTestContext(t, time.Time{})
	defer ctx.stop()

	block := ctx.scanNewBlock()

	// The rescan should neither notify any blocks as disconnected, which
	// would block it, nor stop following the chain.
	subscriptions := ctx.chain.watchSubscriptions()
	ctx.chain.filterHeaderRollbacks <- uint32(block.Height)
	ctx.chain.filterHeaderRollbacks <- uint32(block.Height + 1)

	ctx.scanNewBlock()
	ctx.assertNotSubscribed(subscriptions)
}

// TestRescanFilterHeaderRollbackWhileCatchingUp ensures that a rescan that's
// catching up with the chain rewinds when the filter headers are rolled back
// below blocks it has scanned, even while it's retrying a block whose filter it
// can't fetch, as it can't when the filters peers serve don't match filter
// headers that are wrong.
func TestRescanFilterHeaderRollbackWhileCatchingUp(t *testing.T) {
	t.Parallel()

	ctx := newRollbackTestContext(t, time.Time{})
	defer ctx.stop()

	block1 := ctx.scanNewBlock()
	block2 := ctx.scanNewBlock()

	// We'll add a block without notifying it and instead notify an invalid
	// block, which should prompt the rescan to catch up with the chain
	// manually, retrying the new block as its filter can't be fetched.
	ctx.chain.setFailGetFilter(true)
	block3 := ctx.chain.addNewBlock(false)
	ctx.chain.ntfnChan <- blockntfns.NewBlockConnected(
		wire.BlockHeader{}, 0,
	)
	ctx.assertFilterQueried(block3.Hash)
	ctx.assertFilterQueried(block3.Hash)

	// A repair then finds the filter headers wrong above the first block,
	// rolls them back to it, and they're synced again. The second block's
	// filter, checked against the filter headers that replace them,
	// matches.
	subscriptions := ctx.chain.watchSubscriptions()
	ctx.chain.setFilterMatching(block2.Hash, testWatchScript)
	ctx.chain.filterHeaderRollbacks <- uint32(block1.Height)

	// The rescan should rewind to the first block, notifying the second as
	// disconnected, and scan the blocks above it again once their filters
	// can be fetched, fetching the second as its filter now matches.
	ctx.recvBlockDisconnectedWhileRetrying(block2, &block3.Hash)
	ctx.chain.setFailGetFilter(false)
	ctx.recvBlockConnectedAfterRetries(block2)
	ctx.assertFilterQueried(block3.Hash)
	ctx.recvBlockConnected(block3)

	// Once it follows the chain again, subsequent blocks should be
	// processed as usual.
	ctx.assertSubscribed(subscriptions, block3.Height)
	ctx.scanNewBlock()
}

// TestRescanFilterHeaderRollbackBeforeStart ensures that a rescan rewinds below
// the block it starts from when the filter headers were rolled back below it
// before it started, as the caller may have scanned the blocks above it with
// the wrong filters by then.
func TestRescanFilterHeaderRollbackBeforeStart(t *testing.T) {
	t.Parallel()

	ctx := newRescanTestContext(t, 10, []RescanOption{
		StartTime(time.Time{}),
		WatchInputs(InputWithScript{PkScript: testWatchScript}),
	}...)

	// The filter headers were rolled back to height 5 before the rescan
	// started from height 9, so it should rewind to height 5, notifying
	// the blocks above it as disconnected, and scan them again.
	subscriptions := ctx.chain.watchSubscriptions()
	ctx.chain.filterHeaderRollbacks <- 5
	ctx.start(false)
	defer ctx.stop()

	for height := int32(9); height > 5; height-- {
		ctx.recvBlockDisconnected(ctx.chain.blockStamp(height))
	}
	for height := int32(6); height <= 9; height++ {
		block := ctx.chain.blockStamp(height)
		ctx.assertFilterQueried(block.Hash)
		ctx.recvBlockConnected(block)
	}

	// Once it follows the chain, subsequent blocks should be processed as
	// usual.
	ctx.assertSubscribed(subscriptions, 9)
	ctx.scanNewBlock()
}

// TestRescanFilterHeaderRollbackStopsAtStartTime ensures that a rescan doesn't
// rewind past the blocks from before its start time, which it doesn't scan,
// when the filter headers are rolled back below them, and that it waits for
// the filter headers to be synced up to the block it's at before it follows the
// chain from there.
func TestRescanFilterHeaderRollbackStopsAtStartTime(t *testing.T) {
	t.Parallel()

	// The rescan starts from height 9, and scans the blocks after the one
	// at height 11, as each is mined 10 minutes after the last.
	genesis := chaincfg.MainNetParams.GenesisBlock.Header.Timestamp
	ctx := newRollbackTestContext(t, genesis.Add(11*10*time.Minute))
	defer ctx.stop()

	block10 := ctx.chain.addNewBlock(true)
	ctx.recvBlockConnected(block10)
	block11 := ctx.chain.addNewBlock(true)
	ctx.recvBlockConnected(block11)
	block12 := ctx.scanNewBlock()
	block13 := ctx.scanNewBlock()

	// A repair then finds the filter headers wrong above height 9 and rolls
	// them back to it. The filter of the block at height 12, checked
	// against the filter headers that replace them, matches.
	subscriptions := ctx.chain.watchSubscriptions()
	ctx.chain.setFilterMatching(block12.Hash, testWatchScript)
	ctx.chain.rollBackFilterHeaders(9, true)

	// The rescan should only rewind to height 11, notifying the blocks it
	// scanned as disconnected, and wait there for the filter headers.
	ctx.recvBlockDisconnected(block13)
	ctx.recvBlockDisconnected(block12)
	ctx.assertNotSubscribed(subscriptions)
	ctx.assertRunning()

	// Once the filter headers reach height 11, the rescan should follow the
	// chain from there, scanning the blocks above it again, and fetch the
	// one at height 12 as its filter now matches.
	ctx.chain.syncFilterHeader(false)
	ctx.chain.syncFilterHeader(false)
	ctx.assertSubscribed(subscriptions, block11.Height)
	ctx.chain.syncFilterHeader(true)
	ctx.assertFilterQueried(block12.Hash)
	ctx.assertBlockQueried(block12.Hash)
	ctx.recvBlockConnected(block12)
	ctx.chain.syncFilterHeader(true)
	ctx.assertFilterQueried(block13.Hash)
	ctx.recvBlockConnected(block13)
}

// TestRescanWaitsForFilterHeadersRolledBack ensures that a rescan catching up
// with the chain waits for the filter headers when they were rolled back below
// the block it's at before it's notified of that, rather than trying to follow
// the chain from above their tip, which ended it.
func TestRescanWaitsForFilterHeadersRolledBack(t *testing.T) {
	t.Parallel()

	ctx := newRollbackTestContext(t, time.Time{})
	defer ctx.stop()

	block1 := ctx.scanNewBlock()
	block2 := ctx.scanNewBlock()

	// The filter headers are rolled back to the first block, and an invalid
	// block prompts the rescan to catch up with the chain manually before
	// it's notified of the rollback.
	subscriptions := ctx.chain.watchSubscriptions()
	ctx.chain.rollBackFilterHeaders(block1.Height, false)
	ctx.chain.ntfnChan <- blockntfns.NewBlockConnected(
		wire.BlockHeader{}, 0,
	)
	ctx.assertNotSubscribed(subscriptions)
	ctx.assertRunning()

	// Once it's notified of the rollback, it should rewind to the first
	// block and follow the chain from there.
	ctx.chain.filterHeaderRollbacks <- uint32(block1.Height)
	ctx.recvBlockDisconnected(block2)
	ctx.assertSubscribed(subscriptions, block1.Height)

	ctx.chain.syncFilterHeader(true)
	ctx.assertFilterQueried(block2.Hash)
	ctx.recvBlockConnected(block2)
}

// TestRescanResubscribesAfterFilterHeaderRollback ensures that a rescan that
// fails to subscribe to block notifications, as the filter headers were rolled
// back below the block it's at just then, tries again once it has rewound
// rather than ending.
func TestRescanResubscribesAfterFilterHeaderRollback(t *testing.T) {
	t.Parallel()

	ctx := newRollbackTestContext(t, time.Time{})
	defer ctx.stop()

	block1 := ctx.scanNewBlock()
	block2 := ctx.scanNewBlock()

	// An invalid block prompts the rescan to catch up with the chain
	// manually, and the filter headers are rolled back to the first block
	// just as it subscribes to block notifications again, before it's
	// notified of the rollback.
	subscriptions := ctx.chain.watchSubscriptions()
	var rolledBack bool
	ctx.chain.overrideSubscribe(func(uint32) (*blockntfns.Subscription,
		error) {

		// The mock chain's mutex is held while this runs.
		if !rolledBack {
			rolledBack = true
			height := block1.Height
			ctx.chain.filterHeadersTip = &height

			return nil, errors.New("filter headers rolled back")
		}

		return &blockntfns.Subscription{
			Notifications: ctx.chain.ntfnChan,
			Cancel:        func() {},
		}, nil
	})
	ctx.chain.ntfnChan <- blockntfns.NewBlockConnected(
		wire.BlockHeader{}, 0,
	)
	ctx.assertSubscribed(subscriptions, block2.Height)
	ctx.assertNotSubscribed(subscriptions)
	ctx.assertRunning()

	// Once it's notified of the rollback, it should rewind to the first
	// block and follow the chain from there.
	ctx.chain.filterHeaderRollbacks <- uint32(block1.Height)
	ctx.recvBlockDisconnected(block2)
	ctx.assertSubscribed(subscriptions, block1.Height)

	ctx.chain.syncFilterHeader(true)
	ctx.assertFilterQueried(block2.Hash)
	ctx.recvBlockConnected(block2)
}

// TestWaitForRetryShutdown ensures a retry wait does not delay shutdown.
func TestWaitForRetryShutdown(t *testing.T) {
	quit := make(chan struct{})
	close(quit)
	ro := &rescanOptions{quit: quit}
	err := waitForRetry(
		nil, ro, &waddrmgr.BlockStamp{}, &wire.BlockHeader{}, time.Hour,
		nil, nil,
	)
	if !errors.Is(err, ErrRescanExit) {
		t.Fatalf("expected rescan exit, got %v", err)
	}
}

// awaitFilterQueried expects the filter of the block with the given hash to be
// queried within timeout, and returns when it was.
func (ctx *rescanTestContext) awaitFilterQueried(hash chainhash.Hash,
	timeout time.Duration) time.Time {

	ctx.t.Helper()

	select {
	case hashQueried := <-ctx.chain.filtersQueried:
		if !hashQueried.IsEqual(&hash) {
			ctx.t.Fatalf("expected to query filter %v, got %v",
				hash, hashQueried)
		}
	case <-time.After(timeout):
		ctx.t.Fatalf("expected rescan to query for filter %v", hash)
	}
	return time.Now()
}

// assertRetryBackoff expects the filter of the block with the given hash to be
// queried the given number of times, each after waiting blockRetryInterval, and
// then twice as long as before. The scheduler can delay when we read the clock
// after a query, so a wait may come out a little shorter, but not as short as
// the one before.
func (ctx *rescanTestContext) assertRetryBackoff(hash chainhash.Hash,
	tries int) {

	ctx.t.Helper()

	last := ctx.awaitFilterQueried(hash, 10*time.Second)
	wait := blockRetryInterval
	for try := 2; try <= tries; try++ {
		queried := ctx.awaitFilterQueried(hash, 10*time.Second)
		if waited := queried.Sub(last); waited < wait*3/4 {
			ctx.t.Fatalf("try %d of filter %v came %v after the "+
				"one before, want %v", try, hash, waited, wait)
		}
		last = queried
		wait *= 2
	}
}

// TestRescanRetryBackoff ensures that the rescan waits twice as long before
// each try of a block whose filter it keeps failing to fetch, both while it
// follows the chain and while it catches up with it, rather than asking for the
// filter or block ten times a second, and goes on once it gets the filter.
func TestRescanRetryBackoff(t *testing.T) {
	t.Parallel()

	ctx := newRescanTestContext(t, 10, []RescanOption{
		StartTime(time.Time{}),
		WatchInputs(InputWithScript{}),
	}...)
	ctx.start(true)
	defer ctx.stop()

	ctx.chain.setFailGetFilter(true)
	block1 := ctx.chain.addNewBlock(true)
	ctx.assertRetryBackoff(block1.Hash, 4)
	ctx.chain.setFailGetFilter(false)
	ctx.awaitFilterQueried(block1.Hash, 10*time.Second)
	ctx.recvBlockConnected(block1)

	// We'll add a block without notifying it and instead notify an invalid
	// block, which should prompt the rescan to catch up with the chain
	// manually.
	ctx.chain.setFailGetFilter(true)
	block2 := ctx.chain.addNewBlock(false)
	ctx.chain.ntfnChan <- blockntfns.NewBlockConnected(
		wire.BlockHeader{}, 0,
	)
	ctx.assertRetryBackoff(block2.Hash, 4)
	ctx.chain.setFailGetFilter(false)
	ctx.awaitFilterQueried(block2.Hash, 10*time.Second)
	ctx.recvBlockConnected(block2)
}

// TestBlockRetryBackoff ensures that the wait before a block is tried again
// doubles with each try of the same block up to maxBlockRetryInterval, and
// starts over for another block, or once a block went through.
func TestBlockRetryBackoff(t *testing.T) {
	var backoff blockRetryBackoff
	block1, block2 := chainhash.Hash{1}, chainhash.Hash{2}

	want := blockRetryInterval
	for try := 2; try <= 12; try++ {
		if got := backoff.next(block1); got != want {
			t.Fatalf("try %d: waited %v, want %v", try, got, want)
		}
		want *= 2
		if want > maxBlockRetryInterval {
			want = maxBlockRetryInterval
		}
	}

	if got := backoff.next(block2); got != blockRetryInterval {
		t.Fatalf("another block waited %v, want %v", got,
			blockRetryInterval)
	}
	backoff.next(block2)
	backoff.reset()
	if got := backoff.next(block2); got != blockRetryInterval {
		t.Fatalf("after a block went through, waited %v, want %v",
			got, blockRetryInterval)
	}
}

// TestWaitForRetryRollback ensures that a rollback of the filter headers ends a
// retry wait at once and rewinds the rescan, so that a long wait doesn't hold
// up scanning the blocks again.
func TestWaitForRetryRollback(t *testing.T) {
	rollbacks := make(chan uint32, 1)
	rollbacks <- 5

	var rewound []uint32
	rewindTo := func(height uint32) (bool, error) {
		rewound = append(rewound, height)
		return true, nil
	}

	ro := &rescanOptions{quit: make(chan struct{})}
	done := make(chan error, 1)
	go func() {
		done <- waitForRetry(
			nil, ro, &waddrmgr.BlockStamp{}, &wire.BlockHeader{},
			time.Hour, rollbacks, rewindTo,
		)
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the rollback didn't end the wait")
	}
	if len(rewound) != 1 || rewound[0] != 5 {
		t.Fatalf("rewound to %v, want to block 5", rewound)
	}
}
