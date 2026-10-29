package neutrino

import (
	"bytes"
	"compress/bzip2"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/gcash/bchd/blockchain"
	"github.com/gcash/bchd/chaincfg"
	"github.com/gcash/bchd/chaincfg/chainhash"
	"github.com/gcash/bchd/wire"
	"github.com/gcash/bchutil/gcs"
	"github.com/gcash/bchutil/gcs/builder"
	"github.com/gcash/neutrino/cache/lru"
	"github.com/gcash/neutrino/chainsync"
	"github.com/gcash/neutrino/headerfs"
)

// block100000Hex is mainnet block 100000.
const block100000Hex = "" +
	"0100000050120119172a610421a6c3011dd330d9df07b63616c2cc1f1cd00200" +
	"000000006657a9252aacd5c0b2940996ecff952228c3067cc38d4885efb5a4ac" +
	"4247e9f337221b4d4c86041b0f2b571004010000000100000000000000000000" +
	"00000000000000000000000000000000000000000000ffffffff08044c86041b" +
	"020602ffffffff0100f2052a010000004341041b0e8c2567c12536aa13357b79" +
	"a073dc4444acb83c4ec7a0e2f99dd7457516c5817242da796924ca4e99947d08" +
	"7fedf9ce467cb9f7c6287078f801df276fdf84ac000000000100000001032e38" +
	"e9c0a84c6046d687d10556dcacc41d275ec55fc00779ac88fdf357a187000000" +
	"008c493046022100c352d3dd993a981beba4a63ad15c209275ca9470abfcd57d" +
	"a93b58e4eb5dce82022100840792bc1f456062819f15d33ee7055cf7b5ee1af1" +
	"ebcc6028d9cdb1c3af7748014104f46db5e9d61a9dc27b8d64ad23e7383a4e6c" +
	"a164593c2527c038c0857eb67ee8e825dca65046b82c9331586c82e0fd1f633f" +
	"25f87c161bc6f8a630121df2b3d3ffffffff0200e32321000000001976a914c3" +
	"98efa9c392ba6013c5e04ee729755ef7f58b3288ac000fe208010000001976a9" +
	"14948c765a6914d43f2a7ac177da2c2f6b52de3d7c88ac000000000100000001" +
	"c33ebff2a709f13d9f9a7569ab16a32786af7d7e2de09265e41c61d078294ecf" +
	"010000008a4730440220032d30df5ee6f57fa46cddb5eb8d0d9fe8de6b342d27" +
	"942ae90a3231e0ba333e02203deee8060fdc70230a7f5b4ad7d7bc3e628cbe21" +
	"9a886b84269eaeb81e26b4fe014104ae31c31bf91278d99b8377a35bbce5b27d" +
	"9fff15456839e919453fc7b3f721f0ba403ff96c9deeb680e5fd341c0fc3a7b9" +
	"0da4631ee39560639db462e9cb850fffffffff0240420f00000000001976a914" +
	"b0dcbf97eabf4404e31d952477ce822dadbe7e1088acc060d211000000001976" +
	"a9146b1281eec25ab4e1e0793ff4e08ab1abb3409cd988ac0000000001000000" +
	"010b6072b386d4a773235237f64c1126ac3b240c84b917a3909ba1c43ded5f51" +
	"f4000000008c493046022100bb1ad26df930a51cce110cf44f7a48c3c561fd97" +
	"7500b1ae5d6b6fd13d0b3f4a022100c5b42951acedff14abba2736fd574bdb46" +
	"5f3e6f8da12e2c5303954aca7f78f3014104a7135bfe824c97ecc01ec7d7e336" +
	"185c81e2aa2c41ab175407c09484ce9694b44953fcb751206564a9c24dd094d4" +
	"2fdbfdd5aad3e063ce6af4cfaaea4ea14fbbffffffff0140420f000000000019" +
	"76a91439aa3d569e06a1d7926dc4be1193c99bf2eb9ee088ac00000000"

// mustHash parses a hash string or fails the test.
func mustHash(t *testing.T, s string) chainhash.Hash {
	t.Helper()
	hash, err := chainhash.NewHashFromStr(s)
	if err != nil {
		t.Fatal(err)
	}
	return *hash
}

// filterHeader builds the regular filter of a block and returns its filter
// header.
func filterHeader(t *testing.T, block *wire.MsgBlock,
	prev chainhash.Hash) chainhash.Hash {

	t.Helper()
	filter, err := builder.BuildBasicFilter(block)
	if err != nil {
		t.Fatal(err)
	}
	header, err := builder.MakeHeaderForFilter(filter, prev)
	if err != nil {
		t.Fatal(err)
	}
	return header
}

// TestBasicFilterMatchesBCHD checks that the filters built from real mainnet
// blocks give the filter headers BCHD serves for them, so a block can be
// checked against its filter header without false mismatches.
func TestBasicFilterMatchesBCHD(t *testing.T) {
	t.Parallel()

	// Blocks 0 to 255 give filter headers 0 and 255 as served by BCHD
	// 0.22.4.
	blocks, err := loadBlocks(t, blockDataFile, blockDataNet)
	if err != nil {
		t.Fatal(err)
	}
	var header chainhash.Hash
	for _, b := range blocks {
		header = filterHeader(t, b.MsgBlock(), header)
		if b.Height() == 0 && header != mustHash(t, "02c2392180d0ce2b5b6f8b08d39a11ffe831c673311a3ecf77b97fc3f0303c9f") {
			t.Fatalf("genesis filter header %v", header)
		}
	}
	if len(blocks) != 256 || header != mustHash(t, "bb2bff1b4a277de1b10770c7c995f369f584c6b979e24e0b0c2e220cc4327ac3") {
		t.Fatalf("filter header %v after %d blocks", header, len(blocks))
	}

	// Block 100000 gives the filter header checkpoint at its height from
	// the filter header before it.
	raw, err := hex.DecodeString(block100000Hex)
	if err != nil {
		t.Fatal(err)
	}
	var block100000 wire.MsgBlock
	if err := block100000.Deserialize(bytes.NewReader(raw)); err != nil {
		t.Fatal(err)
	}
	if block100000.BlockHash() != mustHash(t, "000000000003ba27aa200b1cecaad478d2b00432346c3f1f3986da1afd33e506") {
		t.Fatalf("wrong block %v", block100000.BlockHash())
	}
	header = filterHeader(t, &block100000, mustHash(t, "7d6352c7339076935189e2dbb5ba29efdc2a21ff1ac773d88ff37493f464ede4"))
	err = chainsync.ControlCFHeader(chaincfg.MainNetParams, wire.GCSFilterRegular, 100000, &header)
	if err != nil {
		t.Fatalf("filter header %v at 100000: %v", header, err)
	}
	err = chainsync.ControlCFHeader(chaincfg.MainNetParams, wire.GCSFilterRegular, 100000, &chainhash.Hash{})
	if !errors.Is(err, chainsync.ErrCheckpointMismatch) {
		t.Fatalf("no filter header checkpoint at 100000: %v", err)
	}

	// Block 800000 holds CashTokens outputs, whose token prefix isn't part
	// of the script the filter holds. Its filter headers are as served by
	// BCHD 0.22.4.
	f, err := os.Open(filepath.Join("testdata", "block800000.bz2"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	raw, err = io.ReadAll(bzip2.NewReader(f))
	if err != nil {
		t.Fatal(err)
	}
	var block800000 wire.MsgBlock
	if err := block800000.Deserialize(bytes.NewReader(raw)); err != nil {
		t.Fatal(err)
	}
	if block800000.BlockHash() != mustHash(t, "000000000000000001012aaf4b6aa033ddda9d9bbf832b7be597705be5635311") {
		t.Fatalf("wrong block %v", block800000.BlockHash())
	}
	tokens := 0
	for _, tx := range block800000.Transactions {
		for _, out := range tx.TxOut {
			if !out.TokenData.IsEmpty() {
				tokens++
			}
		}
	}
	if tokens == 0 {
		t.Fatal("block 800000 holds no token outputs")
	}
	header = filterHeader(t, &block800000, mustHash(t, "7b6210c203fa33e2f5fd948c43b24e27e25bf1e227f5e5513600837f40804c79"))
	if header != mustHash(t, "a8efcdf5f9b365d487d63d2d06d20d3bf3021d1ae4d8c60de1a24d9f37b9d3b1") {
		t.Fatalf("filter header %v at 800000", header)
	}
}

// TestGetBlockVerifiesFilter checks that GetBlock checks a downloaded block
// against its filter header, and on a mismatch bans the peers serving a
// filter that isn't the one built from the block.
func TestGetBlockVerifiesFilter(t *testing.T) {
	t.Parallel()

	blocks, err := loadBlocks(t, blockDataFile, blockDataNet)
	if err != nil {
		t.Fatal(err)
	}

	// Block 170 spends an output and pays two scripts. The short filter
	// leaves out the script of its first payment.
	const target = 170
	targetBlock := blocks[target]
	correct, err := builder.BuildBasicFilter(targetBlock.MsgBlock())
	if err != nil {
		t.Fatal(err)
	}
	spend := targetBlock.MsgBlock().Transactions[1].Copy()
	spend.TxOut[0].PkScript = nil
	shortBlock := &wire.MsgBlock{
		Header: targetBlock.MsgBlock().Header,
		Transactions: []*wire.MsgTx{
			targetBlock.MsgBlock().Transactions[0], spend,
		},
	}
	short, err := builder.BuildBasicFilter(shortBlock)
	if err != nil {
		t.Fatal(err)
	}
	payScript := targetBlock.MsgBlock().Transactions[1].TxOut[0].PkScript
	key := builder.DeriveKey(targetBlock.Hash())
	if match, _ := short.Match(key, payScript); match {
		t.Fatal("short filter matches the script it leaves out")
	}

	tests := []struct {
		name    string
		synced  uint32
		filter  *gcs.Filter
		allSame bool
		wantErr bool
	}{
		{
			name:   "correct filter",
			synced: target,
			filter: correct,
		},
		{
			name:    "filter missing an output script",
			synced:  target,
			filter:  short,
			wantErr: true,
		},
		{
			name:    "every peer serving the same other filter",
			synced:  target,
			filter:  short,
			allSame: true,
			wantErr: true,
		},
		{
			name:   "filter headers not synced yet",
			synced: target - 1,
			filter: short,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bm, hdrStore, cfStore, cleanUp, err := setupBlockManager()
			if err != nil {
				t.Fatal(err)
			}
			defer cleanUp()

			// Store the headers of blocks 1 to 170 and the filter
			// headers up to the synced height, the last of them
			// committing to the test's filter.
			prev, _, err := cfStore.ChainTip()
			if err != nil {
				t.Fatal(err)
			}
			for i := uint32(1); i <= target; i++ {
				b := blocks[i]
				err := hdrStore.WriteHeaders(headerfs.BlockHeader{
					BlockHeader: &b.MsgBlock().Header,
					Height:      i,
				})
				if err != nil {
					t.Fatal(err)
				}
				if i > test.synced {
					continue
				}

				filter, err := builder.BuildBasicFilter(b.MsgBlock())
				if err != nil {
					t.Fatal(err)
				}
				if i == target {
					filter = test.filter
				}
				header, err := builder.MakeHeaderForFilter(filter, *prev)
				if err != nil {
					t.Fatal(err)
				}
				err = cfStore.WriteHeaders(headerfs.FilterHeader{
					HeaderHash: b.MsgBlock().BlockHash(),
					FilterHash: header,
					Height:     i,
				})
				if err != nil {
					t.Fatal(err)
				}
				prev = &header
			}

			// One peer serves the test's filter, the other the
			// correct one.
			const testPeer, goodPeer = "10.0.0.1:8333", "10.0.0.2:8333"
			testBytes, _ := test.filter.NBytes()
			goodBytes, _ := correct.NBytes()
			if test.allSame {
				goodBytes = testBytes
			}
			queried := false
			bm.queries = &mockQueryAccess{answers: map[string]wire.Message{
				testPeer: wire.NewMsgCFilter(
					wire.GCSFilterRegular, targetBlock.Hash(), testBytes,
				),
				goodPeer: wire.NewMsgCFilter(
					wire.GCSFilterRegular, targetBlock.Hash(), goodBytes,
				),
			}}

			cs := bm.server
			cs.blockManager = bm
			cs.BlockCache = lru.NewCache(1 << 20)
			cs.timeSource = blockchain.NewMedianTime()
			cs.quit = make(chan struct{})
			close(cs.quit)
			cs.queryPeers = func(_ wire.Message, f func(*ServerPeer,
				wire.Message, chan<- struct{}), _ ...QueryOption) {

				queried = true
				f(nil, targetBlock.MsgBlock(), make(chan struct{}))
			}

			found, err := cs.GetBlock(*targetBlock.Hash())
			if test.wantErr {
				if !errors.Is(err, ErrBlockFilterMismatch) {
					t.Fatalf("want filter mismatch, got %v", err)
				}
				banned := cs.IsBanned(testPeer)
				if banned == test.allSame {
					t.Fatalf("peer serving the short filter "+
						"banned: %v", banned)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if *found.Hash() != *targetBlock.Hash() {
					t.Fatalf("got block %v", found.Hash())
				}
				if cs.IsBanned(testPeer) {
					t.Fatal("peer banned")
				}
			}
			if cs.IsBanned(goodPeer) {
				t.Fatal("peer serving the correct filter banned")
			}

			// A block that failed the check isn't cached, so it's
			// downloaded and checked again.
			queried = false
			_, err = cs.GetBlock(*targetBlock.Hash())
			if queried != test.wantErr || (err != nil) != test.wantErr {
				t.Fatalf("second fetch: queried %v, err %v",
					queried, err)
			}
		})
	}
}
