package neutrino

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/gcash/bchd/blockchain"
	"github.com/gcash/bchd/chaincfg/chainhash"
	"github.com/gcash/bchd/wire"
	"github.com/gcash/bchutil"
	"github.com/gcash/bchutil/gcs/builder"
)

// disputeBlock is a block of the test chain, with what a node needs to prove
// its transactions.
type disputeBlock struct {
	header  wire.BlockHeader
	height  uint32
	txs     []*bchutil.Tx
	merkles []*chainhash.Hash
}

// disputeChain is a small chain: an older block whose outputs the block in
// dispute spends, and that block.
type disputeChain struct {
	blocks map[chainhash.Hash]*disputeBlock
	txLoc  map[chainhash.Hash]struct {
		block *disputeBlock
		index int
	}
	block  *disputeBlock
	leaves []builder.ScriptLeaf

	// smallTxUntil is the last height at which a block may hold a
	// transaction of 64 bytes.  The zero value is below the blocks
	// newDisputeChain builds, so those tests still refuse one.
	smallTxUntil uint32
}

func disputeScript(n byte) []byte {
	return append(append([]byte{0x76, 0xa9, 0x14}, bytes.Repeat([]byte{n}, 20)...), 0x88, 0xac)
}

func (c *disputeChain) addBlock(height uint32, msgTxs ...*wire.MsgTx) *disputeBlock {
	b := &disputeBlock{height: height}
	for _, msgTx := range msgTxs {
		b.txs = append(b.txs, bchutil.NewTx(msgTx))
	}
	b.merkles = blockchain.BuildMerkleTreeStore(b.txs)
	b.header = wire.BlockHeader{Version: 4, Nonce: height,
		MerkleRoot: *b.merkles[len(b.merkles)-1]}
	c.blocks[b.header.BlockHash()] = b
	for i, tx := range b.txs {
		c.txLoc[*tx.Hash()] = struct {
			block *disputeBlock
			index int
		}{b, i}
	}
	return b
}

func (c *disputeChain) headers(hash *chainhash.Hash) (*wire.BlockHeader, uint32, bool) {
	b, ok := c.blocks[*hash]
	if !ok {
		return nil, 0, false
	}
	return &b.header, b.height, true
}

// newDisputeChain builds the chain.  The block in dispute has three
// transactions, so that the last one has no partner in the Merkle tree: a
// coinbase, one that spends four older outputs, one with tokens, and pays
// eight scripts, and one that spends two more.
func newDisputeChain(t *testing.T) *disputeChain {
	t.Helper()
	c := &disputeChain{
		blocks: make(map[chainhash.Hash]*disputeBlock),
		txLoc: make(map[chainhash.Hash]struct {
			block *disputeBlock
			index int
		}),
	}
	coinbase := func(n byte) *wire.MsgTx {
		tx := wire.NewMsgTx(1)
		tx.AddTxIn(wire.NewTxIn(wire.NewOutPoint(&chainhash.Hash{}, wire.MaxPrevOutIndex), []byte{n, 0x51}))
		tx.AddTxOut(wire.NewTxOut(5000, disputeScript(n), wire.TokenData{}))
		return tx
	}
	var category [32]byte
	category[0] = 0xcc
	token := wire.TokenData{CategoryID: category, Amount: 7, BitField: wire.HAS_AMOUNT}

	// The older block: six outputs to spend, spread over three
	// transactions after its coinbase.
	var funding []*wire.MsgTx
	for i := byte(0); i < 3; i++ {
		tx := wire.NewMsgTx(2)
		tx.AddTxIn(wire.NewTxIn(wire.NewOutPoint(&chainhash.Hash{0xf0, i}, 0), nil))
		tx.AddTxOut(wire.NewTxOut(1000, disputeScript(0x10+2*i), wire.TokenData{}))
		tx.AddTxOut(wire.NewTxOut(1000, disputeScript(0x11+2*i), wire.TokenData{}))
		funding = append(funding, tx)
	}
	funding[0].TxOut[1].TokenData = token
	c.addBlock(5, append([]*wire.MsgTx{coinbase(1)}, funding...)...)

	spend := func(tx *wire.MsgTx, from *wire.MsgTx, index uint32) {
		hash := from.TxHash()
		tx.AddTxIn(wire.NewTxIn(wire.NewOutPoint(&hash, index), nil))
	}
	first := wire.NewMsgTx(2)
	spend(first, funding[0], 0)
	spend(first, funding[0], 1)
	spend(first, funding[1], 0)
	spend(first, funding[1], 1)
	for i := byte(0); i < 8; i++ {
		first.AddTxOut(wire.NewTxOut(100, disputeScript(0x30+i), wire.TokenData{}))
	}
	first.TxOut[2].TokenData = token
	first.AddTxOut(wire.NewTxOut(0, []byte{0x6a, 0x01, 0x02}, wire.TokenData{}))
	second := wire.NewMsgTx(2)
	spend(second, funding[2], 0)
	spend(second, funding[2], 1)
	second.AddTxOut(wire.NewTxOut(100, disputeScript(0x30), wire.TokenData{}))
	c.block = c.addBlock(9, coinbase(2), first, second)

	// The scripts of the spent outputs, with their token prefixes, as a
	// node's spend journal gives them.
	var prevOutScripts [][]byte
	for _, tx := range c.block.txs[1:] {
		for _, txIn := range tx.MsgTx().TxIn {
			loc := c.txLoc[txIn.PreviousOutPoint.Hash]
			out := loc.block.txs[loc.index].MsgTx().TxOut[txIn.PreviousOutPoint.Index]
			script := out.PkScript
			if !out.TokenData.IsEmpty() {
				prefix := out.TokenData.TokenDataBuffer()
				script = append(prefix.Bytes(), script...)
			}
			prevOutScripts = append(prevOutScripts, script)
		}
	}
	msgBlock := wire.NewMsgBlock(&c.block.header)
	for _, tx := range c.block.txs {
		msgBlock.Transactions = append(msgBlock.Transactions, tx.MsgTx())
	}
	leaves, err := builder.ScriptLeaves(msgBlock, prevOutScripts)
	if err != nil {
		t.Fatal(err)
	}
	c.leaves = leaves
	return c
}

// disputePeer serves the proofs of a tree of leaves, as a node does.
type disputePeer struct {
	chain  *disputeChain
	leaves []builder.ScriptLeaf
	silent bool
	asked  int

	// tamper changes a proof before it is sent.
	tamper func(*wire.MsgCFProof)

	// filter, when set, is the filter the peer serves in place of the
	// one its leaves give.
	filter []byte

	// askedRanges counts the cfrange requests, and tamperRange changes a
	// cfrange before it is sent.
	askedRanges int
	tamperRange func(*wire.MsgCFRange)
}

func (p *disputePeer) root() chainhash.Hash {
	return builder.ScriptLeavesRoot(p.leaves)
}

func (p *disputePeer) proof(index uint32) *wire.MsgCFProof {
	p.asked++
	if p.silent || index >= uint32(len(p.leaves)) {
		return nil
	}
	leaf := p.leaves[index]
	branch, _ := builder.ScriptRangeHashes(p.leaves, int(index), int(index)+1)
	block := p.chain.block
	msg := &wire.MsgCFProof{
		FilterType: wire.GCSFilterScripts,
		BlockHash:  block.header.BlockHash(),
		LeafCount:  uint32(len(p.leaves)),
		LeafIndex:  index,
		Leaf:       wire.CFLeaf(leaf.Bytes()),
		LeafBranch: branch,
		Tx:         wire.NewMsgTx(1),
	}
	if int(leaf.TxIndex) < len(block.txs) {
		tx := block.txs[leaf.TxIndex].MsgTx()
		msg.Tx = tx
		msg.TxBranch = blockchain.MerkleBranch(block.merkles, int(leaf.TxIndex))
		if msg.Leaf.Spent() {
			msg.PrevTx = wire.NewMsgTx(1)
			if int(leaf.Index) < len(tx.TxIn) {
				prevOut := tx.TxIn[leaf.Index].PreviousOutPoint
				if loc, ok := p.chain.txLoc[prevOut.Hash]; ok {
					msg.PrevBlockHash = loc.block.header.BlockHash()
					msg.PrevTxIndex = uint32(loc.index)
					msg.PrevTx = loc.block.txs[loc.index].MsgTx()
					msg.PrevTxBranch = blockchain.MerkleBranch(loc.block.merkles, loc.index)
				}
			}
		}
	} else if msg.Leaf.Spent() {
		msg.PrevTx = wire.NewMsgTx(1)
	}
	if p.tamper != nil {
		p.tamper(msg)
	}
	return msg
}

// settle runs a dispute among peers and returns the peers found at fault,
// sorted.
func (c *disputeChain) settle(peers map[string]*disputePeer) []string {
	roots := make(map[string]chainhash.Hash)
	for name, p := range peers {
		roots[name] = p.root()
	}
	fetch := func(peer string, index uint32) *wire.MsgCFProof {
		msg := peers[peer].proof(index)
		if msg == nil {
			return nil
		}
		// Through the wire encoding, as a peer's message arrives.
		var buf bytes.Buffer
		if err := msg.BchEncode(&buf, wire.ProtocolVersion, wire.BaseEncoding); err != nil {
			return nil
		}
		read := new(wire.MsgCFProof)
		if err := read.BchDecode(&buf, wire.ProtocolVersion, wire.BaseEncoding); err != nil {
			return nil
		}
		return read
	}
	bad := settleScriptFilterDispute(c.block.header.BlockHash(), &c.block.header,
		c.block.height, roots, fetch, c.headers, c.smallTxUntil)
	slices.Sort(bad)
	return bad
}

func (c *disputeChain) honest() *disputePeer {
	return &disputePeer{chain: c, leaves: slices.Clone(c.leaves)}
}

// liar returns a peer whose tree is the true leaves changed by edit, sorted
// again unless the edit asks for another order.
func (c *disputeChain) liar(sorted bool, edit func([]builder.ScriptLeaf) []builder.ScriptLeaf) *disputePeer {
	leaves := edit(slices.Clone(c.leaves))
	if sorted {
		slices.SortFunc(leaves, func(a, b builder.ScriptLeaf) int {
			return compareScriptLeaves(&a, &b)
		})
	}
	return &disputePeer{chain: c, leaves: leaves}
}

// TestSettleScriptFilterDispute pits a peer that serves the true entry tree
// against peers that serve a wrong one, in each way a tree can be wrong, and
// checks that the wrong one is found from a few proofs.
func TestSettleScriptFilterDispute(t *testing.T) {
	c := newDisputeChain(t)
	n := len(c.leaves)
	if n < 16 {
		t.Fatalf("only %d leaves", n)
	}
	kinds := make(map[builder.ScriptLeafKind]int)
	for _, leaf := range c.leaves {
		kinds[leaf.Kind]++
	}
	if len(kinds) != 4 {
		t.Fatalf("the block lacks a kind of leaf: %v", kinds)
	}

	check := func(name string, liar *disputePeer) {
		t.Helper()
		honest := c.honest()
		bad := c.settle(map[string]*disputePeer{"honest": honest, "liar": liar})
		if len(bad) != 1 || bad[0] != "liar" {
			t.Errorf("%s: peers at fault %v, want the liar", name, bad)
		}
		// A search over n leaves, the leaf before the one found, and
		// the first leaf for the count.
		if limit := 8 + 3; honest.asked > limit || liar.asked > limit {
			t.Errorf("%s: %d and %d proofs asked for, want at most %d each",
				name, honest.asked, liar.asked, limit)
		}
	}

	// Every leaf left out, in turn: outputs, spent outputs and token
	// categories of both.
	for i := range c.leaves {
		check(fmt.Sprintf("leaf %d (kind %d) left out", i, c.leaves[i].Kind),
			c.liar(true, func(l []builder.ScriptLeaf) []builder.ScriptLeaf {
				return slices.Delete(l, i, i+1)
			}))
	}

	// Every leaf replaced by one that doesn't exist, in turn.  The trees
	// then have as many leaves, and every leaf after the replaced one is
	// the same in both, so only the hashes left of a leaf show which side
	// of it the difference is on.
	for i := range c.leaves {
		check(fmt.Sprintf("leaf %d replaced", i),
			c.liar(true, func(l []builder.ScriptLeaf) []builder.ScriptLeaf {
				l[i].Index += 1000
				return l
			}))
	}

	// A leaf for a script the block doesn't have, pointing at a real
	// output, then at a real input.
	for _, kind := range []builder.ScriptLeafKind{builder.ScriptLeafOutput, builder.ScriptLeafSpent,
		builder.ScriptLeafOutputCategory, builder.ScriptLeafSpentCategory} {

		for _, first := range []byte{0x00, 0x7f, 0xff} {
			check(fmt.Sprintf("made-up leaf of kind %d, key %x..", kind, first),
				c.liar(true, func(l []builder.ScriptLeaf) []builder.ScriptLeaf {
					fake := builder.ScriptLeaf{TxIndex: 1, Index: 1, Kind: kind}
					fake.Key[0], fake.Key[1] = first, 0x55
					return append(l, fake)
				}))
		}
	}

	// A true leaf twice, and two leaves in the wrong order.
	check("a leaf repeated", c.liar(false, func(l []builder.ScriptLeaf) []builder.ScriptLeaf {
		return slices.Insert(l, 6, l[5])
	}))
	check("two leaves out of order", c.liar(false, func(l []builder.ScriptLeaf) []builder.ScriptLeaf {
		l[8], l[9] = l[9], l[8]
		return l
	}))

	// A second leaf for an output of the last transaction, at the index
	// after it, where the Merkle tree hashes that transaction with itself:
	// the branch of that copy leads to the Merkle root too.
	lastHash := c.block.txs[2].Hash()
	mirrored := []chainhash.Hash{*lastHash, *c.block.merkles[4]}
	if blockchain.MerkleBranchRoot(*lastHash, 3, mirrored) != c.block.header.MerkleRoot {
		t.Fatal("the mirrored branch does not lead to the Merkle root")
	}
	copied := c.liar(true, func(l []builder.ScriptLeaf) []builder.ScriptLeaf {
		for i := range l {
			if l[i].TxIndex == 2 && l[i].Kind == builder.ScriptLeafOutput {
				extra := l[i]
				extra.TxIndex = 3
				return append(l, extra)
			}
		}
		t.Fatal("no output leaf of the last transaction")
		return nil
	})
	copied.tamper = func(msg *wire.MsgCFProof) {
		if leaf, _ := builder.ParseScriptLeaf(msg.Leaf[:]); leaf.TxIndex == 3 {
			msg.Tx = c.block.txs[2].MsgTx()
			msg.TxBranch = mirrored
		}
	}
	check("a leaf for the copy of the last transaction", copied)

	// A leaf for a script an input is said to spend, proved with a
	// transaction made up to pay that script.
	forgedScript := disputeScript(0xee)
	forged := c.liar(true, func(l []builder.ScriptLeaf) []builder.ScriptLeaf {
		return append(l, builder.ScriptLeaf{Key: builder.ScriptKey(forgedScript),
			TxIndex: 1, Index: 0, Kind: builder.ScriptLeafSpent})
	})
	forged.tamper = func(msg *wire.MsgCFProof) {
		if leaf, _ := builder.ParseScriptLeaf(msg.Leaf[:]); leaf.Key == builder.ScriptKey(forgedScript) {
			made := msg.PrevTx.Copy()
			made.TxOut[msg.Tx.TxIn[0].PreviousOutPoint.Index].PkScript = forgedScript
			msg.PrevTx = made
		}
	}
	check("a spent output's leaf with a made-up transaction", forged)

	// No leaves at all, and a peer that doesn't answer.
	check("an empty tree", c.liar(true, func([]builder.ScriptLeaf) []builder.ScriptLeaf {
		return nil
	}))
	silent := c.liar(true, func(l []builder.ScriptLeaf) []builder.ScriptLeaf {
		return l[1:]
	})
	silent.silent = true
	check("a peer that doesn't answer", silent)

	// A proof whose branch belongs to another tree.
	foreign := c.liar(true, func(l []builder.ScriptLeaf) []builder.ScriptLeaf {
		return l[:n-1]
	})
	foreign.tamper = func(msg *wire.MsgCFProof) {
		if len(msg.LeafBranch) > 0 {
			msg.LeafBranch[0][0] ^= 1
		}
	}
	check("a branch that doesn't rebuild the root", foreign)
}

// TestSettleScriptFilterDisputePeers checks that one honest peer is enough
// among several wrong ones, that peers serving the same wrong root fall
// together, and that peers that agree are left alone.
func TestSettleScriptFilterDisputePeers(t *testing.T) {
	c := newDisputeChain(t)
	omit := func(i int) *disputePeer {
		return c.liar(true, func(l []builder.ScriptLeaf) []builder.ScriptLeaf {
			return slices.Delete(l, i, i+1)
		})
	}
	bad := c.settle(map[string]*disputePeer{
		"honest": c.honest(), "liar1": omit(3), "liar2": omit(3), "liar3": omit(10),
		"liar4": c.liar(true, func(l []builder.ScriptLeaf) []builder.ScriptLeaf { return l[:1] }),
	})
	if want := []string{"liar1", "liar2", "liar3", "liar4"}; !slices.Equal(bad, want) {
		t.Errorf("peers at fault %v, want %v", bad, want)
	}

	if bad := c.settle(map[string]*disputePeer{"a": c.honest(), "b": c.honest()}); len(bad) != 0 {
		t.Errorf("peers that agree found at fault: %v", bad)
	}
}

// TestCheckTxMerkleBranch checks every transaction of blocks of 1 to 9
// transactions at its own index, and refuses any other index.
func TestCheckTxMerkleBranch(t *testing.T) {
	for n := 1; n <= 9; n++ {
		txs := make([]*bchutil.Tx, n)
		for i := range txs {
			msgTx := wire.NewMsgTx(1)
			msgTx.LockTime = uint32(i)
			txs[i] = bchutil.NewTx(msgTx)
		}
		merkles := blockchain.BuildMerkleTreeStore(txs)
		root := merkles[len(merkles)-1]
		for i, tx := range txs {
			branch := blockchain.MerkleBranch(merkles, i)
			for index := uint32(0); index < 16; index++ {
				got := checkTxMerkleBranch(*tx.Hash(), index, branch, root)
				if got != (index == uint32(i)) {
					t.Errorf("%d transactions, transaction %d at index %d: %v",
						n, i, index, got)
				}
			}
		}
	}
}

// TestIsTrueScriptLeaf checks the proof of every leaf of the test block as a
// node serves it, and refuses each one after any part of it is changed.
func TestIsTrueScriptLeaf(t *testing.T) {
	c := newDisputeChain(t)
	peer := c.honest()
	block := c.block
	for i := range c.leaves {
		fresh := func() *scriptLeafProof {
			tree := newScriptTree("peer", block.header.BlockHash(), peer.root(),
				func(_ string, index uint32) *wire.MsgCFProof { return peer.proof(index) })
			proof, err := tree.leaf(uint32(i))
			if err != nil {
				t.Fatalf("leaf %d: %v", i, err)
			}
			// A copy, so that a change doesn't reach the chain.
			msg := *proof.msg
			msg.Tx = msg.Tx.Copy()
			msg.TxBranch = slices.Clone(msg.TxBranch)
			if msg.PrevTx != nil {
				msg.PrevTx = msg.PrevTx.Copy()
				msg.PrevTxBranch = slices.Clone(msg.PrevTxBranch)
			}
			return &scriptLeafProof{leaf: proof.leaf, msg: &msg}
		}
		ok := func(proof *scriptLeafProof, headers scriptHeaderSource) bool {
			return isTrueScriptLeaf(proof, &block.header, block.height, headers,
				c.smallTxUntil)
		}
		if !ok(fresh(), c.headers) {
			t.Fatalf("leaf %d (kind %d) of the block refused", i, c.leaves[i].Kind)
		}

		changes := map[string]func(*scriptLeafProof){
			"another key":               func(p *scriptLeafProof) { p.leaf.Key[0] ^= 1 },
			"another transaction index": func(p *scriptLeafProof) { p.leaf.TxIndex ^= 1 },
			"an index past the outputs": func(p *scriptLeafProof) { p.leaf.Index = 99 },
			"a changed transaction":     func(p *scriptLeafProof) { p.msg.Tx.LockTime++ },
			"a changed Merkle branch":   func(p *scriptLeafProof) { p.msg.TxBranch[0][0] ^= 1 },
			"a longer Merkle branch": func(p *scriptLeafProof) {
				p.msg.TxBranch = append(p.msg.TxBranch, chainhash.Hash{})
			},
			"no transaction": func(p *scriptLeafProof) { p.msg.Tx = nil },
		}
		if c.leaves[i].Kind == builder.ScriptLeafSpent || c.leaves[i].Kind == builder.ScriptLeafSpentCategory {
			changes["a changed spent transaction"] = func(p *scriptLeafProof) { p.msg.PrevTx.LockTime++ }
			changes["another index of the spent transaction"] = func(p *scriptLeafProof) { p.msg.PrevTxIndex ^= 1 }
			changes["a changed spent Merkle branch"] = func(p *scriptLeafProof) { p.msg.PrevTxBranch[0][0] ^= 1 }
			changes["a block we don't have"] = func(p *scriptLeafProof) { p.msg.PrevBlockHash[0] ^= 1 }
			changes["no spent transaction"] = func(p *scriptLeafProof) { p.msg.PrevTx = nil }
		}
		for name, change := range changes {
			proof := fresh()
			change(proof)
			if ok(proof, c.headers) {
				t.Errorf("leaf %d (kind %d) with %s accepted", i, c.leaves[i].Kind, name)
			}
		}

		// A spent output from a block above the one that spends it.
		if proof := fresh(); proof.msg.PrevTx != nil {
			above := func(hash *chainhash.Hash) (*wire.BlockHeader, uint32, bool) {
				header, _, found := c.headers(hash)
				return header, block.height + 1, found
			}
			if ok(proof, above) {
				t.Errorf("leaf %d: a spent output from a higher block accepted", i)
			}
		}
	}
}

// rangeOf answers a getcfrange the way a node does.
func (p *disputePeer) rangeOf(bits uint16, prefix [32]byte) *wire.MsgCFRange {
	p.askedRanges++
	if p.silent {
		return nil
	}
	first, end, err := builder.ScriptLeavesWithPrefix(p.leaves, &prefix, bits)
	if err != nil {
		return nil
	}
	msg := &wire.MsgCFRange{
		FilterType: wire.GCSFilterScripts,
		BlockHash:  p.chain.block.header.BlockHash(),
		LeafCount:  uint32(len(p.leaves)),
		FirstIndex: uint32(first),
	}
	for i := first; i < end; i++ {
		msg.Leaves = append(msg.Leaves, wire.CFLeaf(p.leaves[i].Bytes()))
	}
	lo, hi := first, end
	if first > 0 {
		before := wire.CFLeaf(p.leaves[first-1].Bytes())
		msg.Before = &before
		lo--
	}
	if end < len(p.leaves) {
		after := wire.CFLeaf(p.leaves[end].Bytes())
		msg.After = &after
		hi++
	}
	if lo < hi {
		msg.Hashes, _ = builder.ScriptRangeHashes(p.leaves, lo, hi)
	}
	if p.tamperRange != nil {
		p.tamperRange(msg)
	}
	return msg
}

// servedFilter returns what the peer serves for the block: its root, and the
// filter of its leaves' keys unless it serves another.
func (p *disputePeer) servedFilter(t *testing.T) servedScriptFilter {
	t.Helper()
	if p.filter != nil {
		return servedScriptFilter{root: p.root(), filter: p.filter}
	}
	hash := p.chain.block.header.BlockHash()
	filter, err := scriptFilterOfLeaves(&hash, p.leaves)
	if err != nil {
		t.Fatal(err)
	}
	data, err := filter.NBytes()
	if err != nil {
		t.Fatal(err)
	}
	return servedScriptFilter{root: p.root(), filter: data}
}

// settleFilters runs a dispute over the filters peers serve, and returns the
// peers found at fault, sorted.
func (c *disputeChain) settleFilters(t *testing.T, peers map[string]*disputePeer) []string {
	t.Helper()
	served := make(map[string]servedScriptFilter)
	for name, p := range peers {
		served[name] = p.servedFilter(t)
	}
	proofs := func(peer string, index uint32) *wire.MsgCFProof {
		return peers[peer].proof(index)
	}
	ranges := func(peer string, bits uint16, prefix [32]byte) *wire.MsgCFRange {
		msg := peers[peer].rangeOf(bits, prefix)
		if msg == nil {
			return nil
		}
		var buf bytes.Buffer
		if err := msg.BchEncode(&buf, wire.ProtocolVersion, wire.BaseEncoding); err != nil {
			return nil
		}
		read := new(wire.MsgCFRange)
		if err := read.BchDecode(&buf, wire.ProtocolVersion, wire.BaseEncoding); err != nil {
			return nil
		}
		return read
	}
	bad := settleScriptFilters(c.block.header.BlockHash(), &c.block.header,
		c.block.height, served, proofs, ranges, c.headers, c.smallTxUntil)
	slices.Sort(bad)
	return bad
}

// TestSettleScriptFiltersSameRoot pits a peer that serves the true filter
// against peers that serve the true root in front of another filter, and
// checks that the leaves, fetched in several runs, tell them apart.
func TestSettleScriptFiltersSameRoot(t *testing.T) {
	c := newDisputeChain(t)
	hash := c.block.header.BlockHash()
	defer func(n uint32) { scriptLeavesPerRange = n }(scriptLeavesPerRange)

	// A filter without one of the tree's keys, and one with a key more.
	filterOf := func(leaves []builder.ScriptLeaf) []byte {
		filter, err := scriptFilterOfLeaves(&hash, leaves)
		if err != nil {
			t.Fatal(err)
		}
		data, _ := filter.NBytes()
		return data
	}
	var fewer []builder.ScriptLeaf
	for _, leaf := range c.leaves {
		if leaf.Key != c.leaves[0].Key {
			fewer = append(fewer, leaf)
		}
	}
	extra := append(slices.Clone(c.leaves), builder.ScriptLeaf{Key: [32]byte{0xab}})

	for _, perRange := range []uint32{100000, 4, 1} {
		scriptLeavesPerRange = perRange
		for name, filter := range map[string][]byte{
			"a key left out": filterOf(fewer), "a key more": filterOf(extra),
			"no keys": {0x00}, "not a filter": {0xff, 0x01},
		} {
			honest, liar, liar2 := c.honest(), c.honest(), c.honest()
			liar.filter, liar2.filter = filter, filter
			bad := c.settleFilters(t, map[string]*disputePeer{
				"honest": honest, "liar": liar, "liar2": liar2,
			})
			if !slices.Equal(bad, []string{"liar", "liar2"}) {
				t.Errorf("%d leaves a run, %s: peers at fault %v", perRange, name, bad)
			}
			// The leaves are fetched once, from one peer, and no leaf
			// is proved when the roots agree.
			runs := honest.askedRanges + liar.askedRanges + liar2.askedRanges
			if runs < 2 || (perRange == 1 && runs < 17) || honest.asked+liar.asked+liar2.asked != 0 {
				t.Errorf("%d leaves a run, %s: %d runs and %d proofs asked for",
					perRange, name, runs, honest.asked+liar.asked+liar2.asked)
			}
		}
	}

	// Peers that agree on the root and the filter are left alone.
	scriptLeavesPerRange = 4
	if bad := c.settleFilters(t, map[string]*disputePeer{"a": c.honest(), "b": c.honest()}); len(bad) != 0 {
		t.Errorf("peers that agree found at fault: %v", bad)
	}

	// A wrong tree and a wrong filter under the true root, together.
	wrongFilter := c.honest()
	wrongFilter.filter = filterOf(fewer)
	wrongTree := c.liar(true, func(l []builder.ScriptLeaf) []builder.ScriptLeaf { return l[1:] })
	bad := c.settleFilters(t, map[string]*disputePeer{
		"honest": c.honest(), "wrongFilter": wrongFilter, "wrongTree": wrongTree,
	})
	if !slices.Equal(bad, []string{"wrongFilter", "wrongTree"}) {
		t.Errorf("a wrong tree and a wrong filter: peers at fault %v", bad)
	}
}

// TestFetchScriptLeaves fetches the leaves of the test block's tree in runs
// of every size, and refuses a peer that leaves a run out, changes a leaf,
// answers for another tree, or doesn't answer.
func TestFetchScriptLeaves(t *testing.T) {
	c := newDisputeChain(t)
	hash := c.block.header.BlockHash()
	defer func(n uint32) { scriptLeavesPerRange = n }(scriptLeavesPerRange)

	fetchFrom := func(p *disputePeer) ([]builder.ScriptLeaf, error) {
		return fetchScriptLeaves("peer", hash, builder.ScriptLeavesRoot(c.leaves),
			func(_ string, bits uint16, prefix [32]byte) *wire.MsgCFRange {
				return p.rangeOf(bits, prefix)
			})
	}
	for _, perRange := range []uint32{100000, 8, 3, 1} {
		scriptLeavesPerRange = perRange
		leaves, err := fetchFrom(c.honest())
		if err != nil || !slices.Equal(leaves, c.leaves) {
			t.Fatalf("%d leaves a run: %d leaves, %v", perRange, len(leaves), err)
		}
	}

	scriptLeavesPerRange = 3
	bad := map[string]func(p *disputePeer){
		"a run without its last leaf": func(p *disputePeer) {
			p.tamperRange = func(msg *wire.MsgCFRange) {
				if n := len(msg.Leaves); n > 1 {
					msg.Leaves = msg.Leaves[:n-1]
				}
			}
		},
		"a changed leaf": func(p *disputePeer) {
			p.tamperRange = func(msg *wire.MsgCFRange) {
				if len(msg.Leaves) > 0 {
					msg.Leaves[0][35] ^= 1
				}
			}
		},
		"another leaf count": func(p *disputePeer) {
			p.tamperRange = func(msg *wire.MsgCFRange) { msg.LeafCount++ }
		},
		"another tree": func(p *disputePeer) { p.leaves = p.leaves[1:] },
		"no answer":    func(p *disputePeer) { p.silent = true },
	}
	for name, change := range bad {
		p := c.honest()
		change(p)
		if _, err := fetchFrom(p); !errors.Is(err, errScriptProof) {
			t.Errorf("%s: got %v", name, err)
		}
	}
}

// newBareDisputeChain is an empty chain the small-transaction tests fill in.
func newBareDisputeChain() *disputeChain {
	return &disputeChain{
		blocks: make(map[chainhash.Hash]*disputeBlock),
		txLoc: make(map[chainhash.Hash]struct {
			block *disputeBlock
			index int
		}),
	}
}

// blockLeaves is the entry tree of one block.  prevOutScripts lines up with
// the inputs of every transaction after the coinbase.
func blockLeaves(t *testing.T, block *disputeBlock, prevOutScripts [][]byte) []builder.ScriptLeaf {
	t.Helper()
	msg := wire.NewMsgBlock(&block.header)
	for _, tx := range block.txs {
		msg.Transactions = append(msg.Transactions, tx.MsgTx())
	}
	leaves, err := builder.ScriptLeaves(msg, prevOutScripts)
	if err != nil {
		t.Fatal(err)
	}
	return leaves
}

// mustTx64 is a transaction of 64 bytes: version, one input with an empty
// signature script, one output paying script, and a lock time.  script is 4
// bytes and does not start with OP_RETURN.
func mustTx64(t *testing.T, script []byte) *wire.MsgTx {
	t.Helper()
	tx := wire.NewMsgTx(1)
	tx.AddTxIn(wire.NewTxIn(wire.NewOutPoint(&chainhash.Hash{}, 0), nil))
	tx.AddTxOut(wire.NewTxOut(1000, script, wire.TokenData{}))
	tx.LockTime = 1
	if got := tx.SerializeSize(); got != 64 {
		t.Fatalf("SerializeSize() = %d, want 64", got)
	}
	return tx
}

// TestScriptTreeLeafCountWraps checks that a peer whose tree claims
// 4294967295 leaves, with the true leaves first and one padding leaf after
// them, is the peer at fault against the true tree.  The walk over that width
// has to use an integer wider than the width, or adding one wraps and the two
// peers can no longer be told apart.
func TestScriptTreeLeafCountWraps(t *testing.T) {
	c := newDisputeChain(t)
	n := len(c.leaves)
	if n < 5 {
		t.Fatalf("only %d leaves", n)
	}
	const count uint64 = 4294967295
	pad := builder.ScriptLeaf{Key: [32]byte{0xff}}
	padHash := builder.ScriptLeafHash(&pad)
	leafHash := make([]chainhash.Hash, n)
	for i := range c.leaves {
		leafHash[i] = builder.ScriptLeafHash(&c.leaves[i])
	}

	// The tree is not stored leaf by leaf.  A padding subtree of 2^level
	// equal leaves is the padding hash paired with itself, and a last node
	// without a partner moves up unchanged.  4294967295 leaves take 32
	// such steps to reach one node.
	cache := make(map[[2]uint64]chainhash.Hash)
	var node func(level, index uint64) chainhash.Hash
	node = func(level, index uint64) chainhash.Hash {
		key := [2]uint64{level, index}
		if h, ok := cache[key]; ok {
			return h
		}
		var h chainhash.Hash
		if level < 64 {
			size := uint64(1) << level
			if index <= ^uint64(0)/size && index*size <= ^uint64(0)-size {
				start, end := index*size, index*size+size
				if start >= uint64(n) && end <= count-1 {
					h = padHash
					for i := uint64(0); i < level; i++ {
						h = builder.ScriptNodeHash(&h, &h)
					}
					cache[key] = h
					return h
				}
			}
		}
		if level == 0 {
			if index < uint64(n) {
				h = leafHash[index]
			} else {
				h = padHash
			}
		} else {
			width := count
			for i := uint64(0); i < level-1; i++ {
				width = (width + 1) / 2
			}
			left := node(level-1, index*2)
			if index*2+1 < width {
				right := node(level-1, index*2+1)
				h = builder.ScriptNodeHash(&left, &right)
			} else {
				h = left
			}
		}
		cache[key] = h
		return h
	}
	root := node(32, 0)
	honest := c.honest()
	if root == honest.root() {
		t.Fatal("the padded tree has the true root")
	}
	branchOf := func(index uint64) []chainhash.Hash {
		var branch []chainhash.Hash
		at, width := index, count
		for level := uint64(0); width > 1; level++ {
			switch {
			case at%2 == 1:
				branch = append(branch, node(level, at-1))
			case at+1 < width:
				branch = append(branch, node(level, at+1))
			}
			at, width = at/2, (width+1)/2
		}
		return branch
	}
	checkBranch := func(index uint32, leaf builder.ScriptLeaf) {
		t.Helper()
		branch := branchOf(uint64(index))
		if len(branch) > 32 {
			t.Fatalf("leaf %d: branch has %d hashes", index, len(branch))
		}
		got, err := builder.ScriptRangeRoot(uint32(count), index,
			[]builder.ScriptLeaf{leaf}, branch)
		if err != nil || got != root {
			t.Fatalf("leaf %d does not rebuild the padded root: %v", index, err)
		}
	}
	checkBranch(0, c.leaves[0])
	checkBranch(2, c.leaves[2])
	checkBranch(uint32(n), pad)

	var asked []uint32
	fetch := func(peer string, index uint32) *wire.MsgCFProof {
		if peer == "honest" {
			return honest.proof(index)
		}
		asked = append(asked, index)
		if index > uint32(n) {
			return nil
		}
		leaf := pad
		if int(index) < n {
			leaf = c.leaves[index]
		}
		return &wire.MsgCFProof{
			FilterType: wire.GCSFilterScripts,
			BlockHash:  c.block.header.BlockHash(),
			LeafCount:  uint32(count),
			LeafIndex:  index,
			Leaf:       wire.CFLeaf(leaf.Bytes()),
			LeafBranch: branchOf(uint64(index)),
			Tx:         wire.NewMsgTx(1),
		}
	}
	bad := settleScriptFilterDispute(c.block.header.BlockHash(), &c.block.header,
		c.block.height, map[string]chainhash.Hash{
			"honest": honest.root(), "liar": root,
		}, fetch, c.headers, c.smallTxUntil)
	slices.Sort(bad)
	if !slices.Equal(bad, []string{"liar"}) {
		t.Fatalf("peers at fault %v, proofs asked of the liar %v", bad, asked)
	}
	if !slices.Contains(asked, uint32(n)) {
		t.Fatalf("the leaf after the true ones was not proved, asked %v", asked)
	}
}

// TestFetchScriptLeavesClaimedCount checks that a peer whose first cfrange
// claims far more leaves than the message carries is refused, and that the
// refusal returns without sizing storage by that count.
func TestFetchScriptLeavesClaimedCount(t *testing.T) {
	c := newDisputeChain(t)
	p := c.honest()
	p.tamperRange = func(msg *wire.MsgCFRange) {
		if p.askedRanges == 1 {
			msg.LeafCount = 4294967295
		}
	}
	_, err := fetchScriptLeaves("peer", c.block.header.BlockHash(), p.root(),
		func(_ string, bits uint16, prefix [32]byte) *wire.MsgCFRange {
			return p.rangeOf(bits, prefix)
		})
	if !errors.Is(err, errScriptProof) {
		t.Fatalf("got %v", err)
	}
}

// TestIsTrueScriptLeafSmallTx checks that a real 64-byte transaction is
// accepted in a block at smallTxUntil and refused above it.  The same bound
// applies to a spent output whose previous transaction is 64 bytes, judged
// at that previous block's height, while the spending transaction is not 64
// bytes.
func TestIsTrueScriptLeafSmallTx(t *testing.T) {
	// 0x51 is not OP_RETURN, so the output is an entry in the script filter.
	script := []byte{0x51, 0x01, 0x02, 0x03}
	small := mustTx64(t, script)
	c := newBareDisputeChain()

	// H is the block that creates the 64-byte transaction.
	const heightH = 5
	funded := c.addBlock(heightH, small)
	fundLeaves := blockLeaves(t, funded, nil)
	if len(fundLeaves) != 1 {
		t.Fatalf("%d leaves in the 64-byte transaction's block", len(fundLeaves))
	}
	outProof := &scriptLeafProof{
		leaf: fundLeaves[0],
		msg: &wire.MsgCFProof{
			Leaf:     wire.CFLeaf(fundLeaves[0].Bytes()),
			Tx:       small,
			TxBranch: blockchain.MerkleBranch(funded.merkles, 0),
		},
	}
	if !isTrueScriptLeaf(outProof, &funded.header, heightH, c.headers, heightH) {
		t.Fatal("64-byte transaction at smallTxUntil refused")
	}
	if isTrueScriptLeaf(outProof, &funded.header, heightH, c.headers, heightH-1) {
		t.Fatal("64-byte transaction above smallTxUntil accepted")
	}

	// P is the block of the 64-byte transaction a later block spends.
	const heightP = heightH
	const spendHeight = 9
	coinbase := wire.NewMsgTx(1)
	coinbase.AddTxIn(wire.NewTxIn(
		wire.NewOutPoint(&chainhash.Hash{}, wire.MaxPrevOutIndex), []byte{0x04, 0x51}))
	coinbase.AddTxOut(wire.NewTxOut(5000, disputeScript(9), wire.TokenData{}))
	spend := wire.NewMsgTx(2)
	prevHash := small.TxHash()
	spend.AddTxIn(wire.NewTxIn(wire.NewOutPoint(&prevHash, 0), nil))
	spend.AddTxOut(wire.NewTxOut(900, disputeScript(4), wire.TokenData{}))
	if spend.SerializeSize() == 64 {
		t.Fatal("spending transaction is 64 bytes")
	}
	c.block = c.addBlock(spendHeight, coinbase, spend)
	c.leaves = blockLeaves(t, c.block, [][]byte{script})
	spent := -1
	for i, leaf := range c.leaves {
		if leaf.Kind == builder.ScriptLeafSpent {
			spent = i
			break
		}
	}
	if spent < 0 {
		t.Fatal("no spent leaf")
	}
	msg := c.honest().proof(uint32(spent))
	if msg == nil || msg.Tx == nil || msg.PrevTx == nil || msg.PrevTx.SerializeSize() != 64 ||
		msg.Tx.SerializeSize() == 64 {
		t.Fatal("spent proof does not carry the 64-byte previous transaction")
	}
	spentProof := &scriptLeafProof{leaf: c.leaves[spent], msg: msg}
	if !isTrueScriptLeaf(spentProof, &c.block.header, spendHeight, c.headers, heightP) {
		t.Fatal("spend of a 64-byte transaction at smallTxUntil refused")
	}
	if isTrueScriptLeaf(spentProof, &c.block.header, spendHeight, c.headers, heightP-1) {
		t.Fatal("spend of a 64-byte transaction above smallTxUntil accepted")
	}
}

// TestSettleSmallTxLeaf checks that an honest peer whose tree holds the leaf
// of a 64-byte transaction, in a block at smallTxUntil, is kept, and the peer
// who leaves that leaf out is the one at fault.
func TestSettleSmallTxLeaf(t *testing.T) {
	const height = 20
	script := []byte{0x51, 0x01, 0x02, 0x03}
	small := mustTx64(t, script)
	c := newBareDisputeChain()
	other := wire.NewMsgTx(2)
	other.AddTxIn(wire.NewTxIn(wire.NewOutPoint(&chainhash.Hash{0xab}, 0), nil))
	other.AddTxOut(wire.NewTxOut(500, disputeScript(3), wire.TokenData{}))
	// The 64-byte transaction is the coinbase, so its input needs no spent
	// script.  The other transaction's input pays OP_RETURN and adds no leaf.
	c.block = c.addBlock(height, small, other)
	c.leaves = blockLeaves(t, c.block, [][]byte{{0x6a}})
	c.smallTxUntil = height
	if len(c.leaves) < 2 {
		t.Fatalf("only %d leaves", len(c.leaves))
	}
	liar := c.liar(true, func(l []builder.ScriptLeaf) []builder.ScriptLeaf {
		for i := range l {
			if l[i].TxIndex == 0 {
				return slices.Delete(l, i, i+1)
			}
		}
		t.Fatal("no leaf of the 64-byte transaction")
		return nil
	})
	bad := c.settle(map[string]*disputePeer{"honest": c.honest(), "liar": liar})
	if !slices.Equal(bad, []string{"liar"}) {
		t.Fatalf("peers at fault %v", bad)
	}
}

// TestFetchScriptLeavesMost checks that the leaves of a tree are fetched while
// it has no more of them than a block can give, and refused with one more.
func TestFetchScriptLeavesMost(t *testing.T) {
	c := newDisputeChain(t)
	hash := c.block.header.BlockHash()
	defer func(most func() uint64) { maxScriptLeaves = most }(maxScriptLeaves)

	asked := 0
	fetch := func() ([]builder.ScriptLeaf, error) {
		p := c.honest()
		return fetchScriptLeaves("peer", hash, builder.ScriptLeavesRoot(c.leaves),
			func(_ string, bits uint16, prefix [32]byte) *wire.MsgCFRange {
				asked++
				return p.rangeOf(bits, prefix)
			})
	}

	count := uint64(len(c.leaves))
	maxScriptLeaves = func() uint64 { return count }
	if leaves, err := fetch(); err != nil || !slices.Equal(leaves, c.leaves) {
		t.Fatalf("as many leaves as a block gives: %d leaves, %v", len(leaves), err)
	}

	// The count comes with the first range, and no other is asked for.
	maxScriptLeaves = func() uint64 { return count - 1 }
	asked = 0
	if _, err := fetch(); !errors.Is(err, errScriptProof) {
		t.Fatalf("one leaf more than a block gives: got %v", err)
	}
	if asked != 1 {
		t.Fatalf("%d ranges were asked for, want 1", asked)
	}
}
