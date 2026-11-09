package neutrino

import (
	"bytes"

	"github.com/gcash/bchd/chaincfg/chainhash"
	"github.com/gcash/bchd/wire"
)

// FoundOutput is a transaction that pays to one of the scripts that
// SearchOutputs looks for.
type FoundOutput struct {
	// Tx is the transaction, and Output the first of its outputs that pays to
	// one of the scripts.
	Tx     *wire.MsgTx
	Output uint32

	// Height and BlockHash are those of the transaction's block.
	Height    int32
	BlockHash chainhash.Hash
}

// SearchOutputs scans block filters for the blocks that pay to the passed output
// scripts, from height from to height to, both included, in that direction: up,
// or down when from is above to.  A filter holds every output script its block
// pays to, so a search needs nothing but the scripts.
//
// found is called with each transaction that pays to one of the scripts, in the
// order of their blocks and then within each block, and returns whether to go
// on.  A search that found returns false for ends at that block.
//
// progress and the height returned are as for SearchSpends.  A search that quit
// closes, or that the service shuts down, returns ErrSearchCancelled.
func (s *ChainService) SearchOutputs(scripts [][]byte, from, to int32,
	found func(FoundOutput) bool, progress func(height int32),
	quit <-chan struct{}) (int32, error) {

	return searchOutputs(s, scripts, from, to, found, progress, quit, s.quit)
}

// searchOutputs is SearchOutputs on a source.
func searchOutputs(source searchSource, scripts [][]byte, from, to int32,
	found func(FoundOutput) bool, progress func(height int32),
	quit, shutdown <-chan struct{}) (int32, error) {

	entries := func() ([][]byte, error) {
		return scripts, nil
	}
	onBlock := func(block *wire.MsgBlock, hash *chainhash.Hash, height int32) (scanStep, bool) {
		if !findBlockOutputs(block, hash, height, scripts, found) {
			return scanStopped, false
		}
		return scanOn, false
	}
	return scanFilters(source, from, to, entries, onBlock, progress, quit, shutdown)
}

// findBlockOutputs tells found of each transaction of the block that pays to one
// of the scripts, and returns whether to go on.  A filter can match where the
// block has nothing, which then tells found nothing.
func findBlockOutputs(block *wire.MsgBlock, hash *chainhash.Hash, height int32,
	scripts [][]byte, found func(FoundOutput) bool) bool {

	for _, tx := range block.Transactions {
		for i, out := range tx.TxOut {
			if !paysTo(out.PkScript, scripts) {
				continue
			}
			if found != nil && !found(FoundOutput{
				Tx: tx, Output: uint32(i), Height: height, BlockHash: *hash,
			}) {
				return false
			}
			break
		}
	}
	return true
}

// paysTo returns whether an output script is one of the scripts.
func paysTo(script []byte, scripts [][]byte) bool {
	for _, s := range scripts {
		if bytes.Equal(script, s) {
			return true
		}
	}
	return false
}
