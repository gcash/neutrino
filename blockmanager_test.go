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
