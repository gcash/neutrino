package neutrino

import (
	"bytes"
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
		c.block.height, roots, fetch, c.headers)
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
			return isTrueScriptLeaf(proof, &block.header, block.height, headers)
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
