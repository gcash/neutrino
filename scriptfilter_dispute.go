package neutrino

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/gcash/bchd/blockchain"
	"github.com/gcash/bchd/chaincfg/chainhash"
	"github.com/gcash/bchd/wire"
	"github.com/gcash/bchutil/gcs/builder"
)

// A script filter (filter type 1) lists the scripts of the outputs a block
// spends, which sit in older blocks, so a filter can't be rebuilt from its
// block to tell which of two peers serves a wrong one, as the regular filter
// is.  Each script filter is served with the root of a Merkle tree over the
// block's sorted entries, its leaves, and a peer proves any leaf of its tree
// with a cfproof message.  Two peers that serve different roots are told
// apart with those proofs alone:
//
//   - The first leaf at which their trees differ is found by a binary search
//     over leaf indexes.  The branch of leaf i holds the hashes of the full
//     blocks of leaves to its left, which are the same in both trees however
//     many leaves each has, so two peers agree on leaves 0 to i exactly when
//     they give the same leaf i and the same hashes to its left.
//   - Of the two leaves there, the one that sorts first can't be anywhere in
//     the other peer's tree if that tree is sorted.  Its proof shows whether
//     it is a true entry of the block: the transaction it names under the
//     block's Merkle root, and for a spent output, the output's transaction
//     under the Merkle root of its own block.  If it is, the other peer left
//     it out or misplaced it.  If it isn't, the peer that holds it made it
//     up.
//
// Either way one of the two serves a wrong filter, and a few dozen small
// messages decide which, whatever the size of the block.  The requests name
// leaf indexes, so they say nothing of what we watch.

// scriptProofSource asks a peer for the cfproof of one leaf of the entry tree
// of the block in dispute.  It returns nil when the peer doesn't answer.
type scriptProofSource func(peer string, index uint32) *wire.MsgCFProof

// scriptHeaderSource returns the header and height of a block of our best
// chain, or false when the block isn't in it.
type scriptHeaderSource func(hash *chainhash.Hash) (*wire.BlockHeader, uint32, bool)

// errScriptProof is returned for a cfproof that doesn't fit the tree of the
// peer that sent it.
var errScriptProof = errors.New("cfproof does not fit the peer's entry tree")

// scriptLeafProof is a leaf a peer proved to be in its tree.
type scriptLeafProof struct {
	leaf builder.ScriptLeaf

	// lefts are the hashes in the leaf's branch that stand left of its
	// path, from the leaves up: the roots of the full blocks of leaves
	// before it.
	lefts []chainhash.Hash

	msg *wire.MsgCFProof
}

// scriptTree is what we know of one peer's entry tree for the block.
type scriptTree struct {
	peer   string
	block  chainhash.Hash
	root   chainhash.Hash
	fetch  scriptProofSource
	proofs map[uint32]*scriptLeafProof

	// count is the number of leaves the peer's first proof gave, which
	// its later proofs must give too.  A tree with the zero root has no
	// leaves.
	count      uint32
	countKnown bool
}

// newScriptTree returns the tree of a peer that serves root for the block.
func newScriptTree(peer string, block, root chainhash.Hash, fetch scriptProofSource) *scriptTree {
	t := &scriptTree{
		peer:   peer,
		block:  block,
		root:   root,
		fetch:  fetch,
		proofs: make(map[uint32]*scriptLeafProof),
	}
	if root == (chainhash.Hash{}) {
		t.countKnown = true
	}
	return t
}

// leafCount returns the number of leaves in the peer's tree.
func (t *scriptTree) leafCount() (uint32, error) {
	if !t.countKnown {
		if _, err := t.leaf(0); err != nil {
			return 0, err
		}
	}
	return t.count, nil
}

// leaf returns the proof of the leaf at index in the peer's tree, checked
// against the root the peer serves.
func (t *scriptTree) leaf(index uint32) (*scriptLeafProof, error) {
	if proof, ok := t.proofs[index]; ok {
		return proof, nil
	}
	msg := t.fetch(t.peer, index)
	if msg == nil {
		return nil, fmt.Errorf("%w: no cfproof for leaf %d", errScriptProof, index)
	}
	if msg.FilterType != wire.GCSFilterScripts || msg.BlockHash != t.block ||
		msg.LeafIndex != index || msg.LeafIndex >= msg.LeafCount ||
		(t.countKnown && msg.LeafCount != t.count) {

		return nil, fmt.Errorf("%w: leaf %d of %d", errScriptProof,
			msg.LeafIndex, msg.LeafCount)
	}
	leaf, err := builder.ParseScriptLeaf(msg.Leaf[:])
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errScriptProof, err)
	}
	root, err := builder.ScriptRangeRoot(
		msg.LeafCount, index, []builder.ScriptLeaf{leaf}, msg.LeafBranch,
	)
	if err != nil || root != t.root {
		return nil, fmt.Errorf("%w: leaf %d does not rebuild the root",
			errScriptProof, index)
	}

	// The branch lists one hash for each level at which the path has a
	// partner: a left one where the path's index is odd, a right one
	// where it is even and not the last of its level.
	proof := &scriptLeafProof{leaf: leaf, msg: msg}
	branch := msg.LeafBranch
	for at, width := index, msg.LeafCount; width > 1; at, width = at/2, (width+1)/2 {
		switch {
		case at%2 == 1:
			proof.lefts = append(proof.lefts, branch[0])
			branch = branch[1:]
		case at+1 < width:
			branch = branch[1:]
		}
	}

	t.count, t.countKnown = msg.LeafCount, true
	t.proofs[index] = proof
	return proof, nil
}

// compareScriptLeaves orders leaves as the entry tree does: by key, then
// transaction index, then input or output index, then kind.
func compareScriptLeaves(a, b *builder.ScriptLeaf) int {
	if c := bytes.Compare(a.Key[:], b.Key[:]); c != 0 {
		return c
	}
	for _, pair := range [][2]uint32{
		{a.TxIndex, b.TxIndex}, {a.Index, b.Index}, {uint32(a.Kind), uint32(b.Kind)},
	} {
		switch {
		case pair[0] < pair[1]:
			return -1
		case pair[0] > pair[1]:
			return 1
		}
	}
	return 0
}

// checkTxMerkleBranch checks that the transaction with the given ID is the
// one at index under a block's Merkle root.
func checkTxMerkleBranch(txid chainhash.Hash, index uint32, branch []chainhash.Hash,
	merkleRoot *chainhash.Hash) bool {

	if len(branch) > wire.MaxCFTreeDepth || uint64(index)>>uint(len(branch)) != 0 {
		return false
	}
	hash := txid
	for at, i := index, 0; i < len(branch); at, i = at/2, i+1 {
		// The last node of a level without a partner is hashed with
		// itself, as a left child.  A right child equal to its partner
		// would be that copy, at an index the block doesn't have.
		if at%2 == 1 && branch[i] == hash {
			return false
		}
		if at%2 == 1 {
			hash = *blockchain.HashMerkleBranches(&branch[i], &hash)
		} else {
			hash = *blockchain.HashMerkleBranches(&hash, &branch[i])
		}
	}
	return hash == *merkleRoot
}

// scriptLeafOf returns whether an output gives a leaf's key: its script for a
// leaf of a script, its token category for a leaf of a category.
func scriptLeafOf(txOut *wire.TxOut, key *[32]byte, category bool) bool {
	if category {
		return !txOut.TokenData.IsEmpty() &&
			builder.CategoryKey(txOut.TokenData.CategoryID) == *key
	}
	script := txOut.PkScript
	return len(script) > 0 && script[0] != 0x6a && builder.ScriptKey(script) == *key
}

// isTrueScriptLeaf checks what a proved leaf points to: whether the block
// with the given header and height really has the entry.  The transaction the
// leaf names must be under the block's Merkle root, and its output, or for a
// leaf of a spent output the output its input spends, must give the leaf's
// key.  A spent output's transaction must be under the Merkle root of a
// block of our chain no higher than the block.
func isTrueScriptLeaf(proof *scriptLeafProof, header *wire.BlockHeader, height uint32,
	headers scriptHeaderSource) bool {

	leaf, msg := &proof.leaf, proof.msg
	tx := msg.Tx

	// A transaction of 64 bytes could be an inner node of the Merkle tree.
	if tx == nil || tx.SerializeSize() == 64 ||
		!checkTxMerkleBranch(tx.TxHash(), leaf.TxIndex, msg.TxBranch, &header.MerkleRoot) {

		return false
	}
	category := leaf.Kind == builder.ScriptLeafOutputCategory ||
		leaf.Kind == builder.ScriptLeafSpentCategory

	if !msg.Leaf.Spent() {
		return leaf.Index < uint32(len(tx.TxOut)) &&
			scriptLeafOf(tx.TxOut[leaf.Index], &leaf.Key, category)
	}

	// The coinbase spends no output.
	if leaf.TxIndex == 0 || leaf.Index >= uint32(len(tx.TxIn)) || msg.PrevTx == nil {
		return false
	}
	prevOut := &tx.TxIn[leaf.Index].PreviousOutPoint
	prevTx := msg.PrevTx
	if prevTx.SerializeSize() == 64 || prevTx.TxHash() != prevOut.Hash ||
		prevOut.Index >= uint32(len(prevTx.TxOut)) {

		return false
	}
	prevHeader, prevHeight, ok := headers(&msg.PrevBlockHash)
	if !ok || prevHeight > height || !checkTxMerkleBranch(
		prevOut.Hash, msg.PrevTxIndex, msg.PrevTxBranch, &prevHeader.MerkleRoot,
	) {
		return false
	}
	return scriptLeafOf(prevTx.TxOut[prevOut.Index], &leaf.Key, category)
}

// scriptVerdict is how a comparison of two peers' entry trees ended.
type scriptVerdict struct {
	// bad is the peer found at fault, or empty when the two couldn't be
	// told apart.
	bad string

	// wrongTree is whether bad's tree is wrong, so that every peer that
	// serves the same root is at fault too.  It is false when bad only
	// failed to prove what its tree holds.
	wrongTree bool
}

// settleScriptTrees finds which of two peers that serve different roots for
// a block is at fault.
func settleScriptTrees(a, b *scriptTree, header *wire.BlockHeader, height uint32,
	headers scriptHeaderSource) scriptVerdict {

	countA, err := a.leafCount()
	if err != nil {
		return scriptVerdict{bad: a.peer}
	}
	countB, err := b.leafCount()
	if err != nil {
		return scriptVerdict{bad: b.peer}
	}

	// Find the first index at which the trees differ: the lowest one below
	// both counts where the leaves, or the hashes left of them, differ, or
	// else the end of the shorter tree.
	lo, hi := uint32(0), min(countA, countB)
	for lo < hi {
		mid := lo + (hi-lo)/2
		proofA, err := a.leaf(mid)
		if err != nil {
			return scriptVerdict{bad: a.peer}
		}
		proofB, err := b.leaf(mid)
		if err != nil {
			return scriptVerdict{bad: b.peer}
		}
		same := proofA.leaf == proofB.leaf && len(proofA.lefts) == len(proofB.lefts)
		for i := 0; same && i < len(proofA.lefts); i++ {
			same = proofA.lefts[i] == proofB.lefts[i]
		}
		if same {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	first := lo

	// Take the leaf there that sorts first, or the only one when one tree
	// ends there.
	holder, other := a, b
	switch {
	case first == countA && first == countB:
		// The same leaves under different roots: one of the two gave
		// proofs for a tree it doesn't serve, and we can't tell which.
		return scriptVerdict{}
	case first == countA:
		holder, other = b, a
	case first < countB:
		proofA, errA := a.leaf(first)
		proofB, errB := b.leaf(first)
		switch {
		case errA != nil:
			return scriptVerdict{bad: a.peer}
		case errB != nil:
			return scriptVerdict{bad: b.peer}
		}
		switch c := compareScriptLeaves(&proofA.leaf, &proofB.leaf); {
		case c == 0:
			return scriptVerdict{}
		case c > 0:
			holder, other = b, a
		}
	}
	proof, err := holder.leaf(first)
	if err != nil {
		return scriptVerdict{bad: holder.peer}
	}

	// The leaf must come after the one before it, or the holder's tree
	// repeats a leaf or is out of order.
	if first > 0 {
		before, err := holder.leaf(first - 1)
		if err != nil {
			return scriptVerdict{bad: holder.peer}
		}
		if compareScriptLeaves(&before.leaf, &proof.leaf) >= 0 {
			return scriptVerdict{bad: holder.peer, wrongTree: true}
		}
	}

	// A true entry that sorts before the other peer's leaf there, with
	// the same leaves before both, is not in the other peer's tree where
	// a sorted tree would have it.
	if isTrueScriptLeaf(proof, header, height, headers) {
		return scriptVerdict{bad: other.peer, wrongTree: true}
	}
	return scriptVerdict{bad: holder.peer, wrongTree: true}
}

// settleScriptFilterDispute returns the peers at fault among those that serve
// the given entry tree roots for one block, when the roots are not all the
// same.  Peers are compared two at a time until those left serve one root.
// One peer that serves the true filter and proves its leaves is enough for
// every other root to be found wrong.
func settleScriptFilterDispute(block chainhash.Hash, header *wire.BlockHeader,
	height uint32, roots map[string]chainhash.Hash, fetch scriptProofSource,
	headers scriptHeaderSource) []string {

	trees := make(map[string]*scriptTree, len(roots))
	for peer, root := range roots {
		trees[peer] = newScriptTree(peer, block, root, fetch)
	}

	var badPeers []string
	for {
		// Pick two peers that serve different roots.
		var a, b *scriptTree
		for _, t := range trees {
			switch {
			case a == nil:
				a = t
			case t.root != a.root:
				b = t
			}
			if b != nil {
				break
			}
		}
		if b == nil {
			return badPeers
		}

		verdict := settleScriptTrees(a, b, header, height, headers)
		if verdict.bad == "" {
			// Neither can be shown wrong by the other.  Both are
			// dropped, so that the dispute ends.
			log.Warnf("Could not tell which of %v and %v serves a "+
				"wrong script filter for block %v", a.peer, b.peer, block)
			badPeers = append(badPeers, a.peer, b.peer)
			delete(trees, a.peer)
			delete(trees, b.peer)
			continue
		}
		badRoot := trees[verdict.bad].root
		for peer, t := range trees {
			if peer == verdict.bad || (verdict.wrongTree && t.root == badRoot) {
				badPeers = append(badPeers, peer)
				delete(trees, peer)
			}
		}
	}
}
