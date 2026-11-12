package neutrino

import (
	"bytes"

	"github.com/gcash/bchd/chaincfg/chainhash"
	"github.com/gcash/bchd/wire"
	"github.com/gcash/bchutil/gcs/builder"
)

// FoundOutput is a transaction that pays to one of the scripts that
// SearchOutputs looks for.
type FoundOutput struct {
	// Tx is the transaction, and Output the first of its outputs that pays to
	// one of the scripts, or for SearchCategoryOutputs, that carries tokens
	// of one of the categories.
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
// on.  A search that found returns false for ends at that block, and returns
// its height, though the block's later transactions weren't looked at and
// progress isn't called with it.
//
// progress and the height returned are otherwise as for SearchSpends, and so
// are what the filters can hide and what a reorganization while it runs can do.
// A search that quit closes, or that the service shuts down, returns
// ErrSearchCancelled.
func (s *ChainService) SearchOutputs(scripts [][]byte, from, to int32,
	found func(FoundOutput) bool, progress func(height int32),
	quit <-chan struct{}) (int32, error) {

	return searchOutputs(s, scripts, from, to, found, progress, quit, s.quit)
}

// searchOutputs is SearchOutputs on a source.
func searchOutputs(source searchSource, scripts [][]byte, from, to int32,
	found func(FoundOutput) bool, progress func(height int32),
	quit, shutdown <-chan struct{}) (int32, error) {

	// A regular filter holds the scripts a block pays, a script filter
	// their keys.
	match := scripts
	if filterTypeOf(source) == wire.GCSFilterScripts {
		match = make([][]byte, len(scripts))
		for i, script := range scripts {
			key := builder.ScriptKey(script)
			match[i] = key[:]
		}
	}
	entries := func() ([][]byte, error) {
		return match, nil
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

// SearchCategoryOutputs scans block filters for the blocks that create an
// output of one of the passed token categories, from height from to height
// to, both included, in that direction: up, or down when from is above to.
//
// Only script filters hold token categories, so on a service that syncs
// regular filters it returns ErrCategoriesNeedScriptFilters.  A script filter
// also matches the blocks that spend an output of the category, which are
// fetched and report nothing unless they also create one.  Going up, the
// first transaction found for a category is the one that created it, as
// tokens of a category exist only from its genesis on.
//
// found is called with each transaction that creates an output of one of the
// categories, in the order of their blocks and then within each block, and
// returns whether to go on.  A transaction is reported once even when several
// of its outputs carry watched categories.  A search that found returns false
// for ends at that block, and returns its height, though the block's later
// transactions weren't looked at and progress isn't called with it.
//
// progress and the height returned are otherwise as for SearchSpends, and so
// are what the filters can hide and what a reorganization while it runs can
// do.  A search that quit closes, or that the service shuts down, returns
// ErrSearchCancelled.
func (s *ChainService) SearchCategoryOutputs(categories [][32]byte, from, to int32,
	found func(FoundOutput) bool, progress func(height int32),
	quit <-chan struct{}) (int32, error) {

	return searchCategoryOutputs(s, categories, from, to, found, progress, quit, s.quit)
}

// searchCategoryOutputs is SearchCategoryOutputs on a source.
func searchCategoryOutputs(source searchSource, categories [][32]byte, from, to int32,
	found func(FoundOutput) bool, progress func(height int32),
	quit, shutdown <-chan struct{}) (int32, error) {

	if filterTypeOf(source) != wire.GCSFilterScripts {
		reached := from - 1
		if from > to {
			reached = from + 1
		}
		return reached, ErrCategoriesNeedScriptFilters
	}

	// A script filter holds the key of each token category a block creates
	// or spends.
	match := make([][]byte, len(categories))
	for i, category := range categories {
		key := builder.CategoryKey(category)
		match[i] = key[:]
	}
	entries := func() ([][]byte, error) {
		return match, nil
	}
	onBlock := func(block *wire.MsgBlock, hash *chainhash.Hash, height int32) (scanStep, bool) {
		if !findBlockCategoryOutputs(block, hash, height, categories, found) {
			return scanStopped, false
		}
		return scanOn, false
	}
	return scanFilters(source, from, to, entries, onBlock, progress, quit, shutdown)
}

// findBlockCategoryOutputs tells found of each transaction of the block that
// creates an output of one of the categories, with the first such output, and
// returns whether to go on.  A transaction is reported once even when several
// of its outputs carry watched categories.  A filter can match where the block has nothing, which
// then tells found nothing.
func findBlockCategoryOutputs(block *wire.MsgBlock, hash *chainhash.Hash, height int32,
	categories [][32]byte, found func(FoundOutput) bool) bool {

	for _, tx := range block.Transactions {
		for i, txOut := range tx.TxOut {
			if !carriesCategory(txOut, categories) {
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

// carriesCategory returns whether the output carries tokens of one of the
// categories.
func carriesCategory(txOut *wire.TxOut, categories [][32]byte) bool {
	if txOut.TokenData.IsEmpty() {
		return false
	}
	category := [32]byte(txOut.TokenData.CategoryID)
	for _, watched := range categories {
		if category == watched {
			return true
		}
	}
	return false
}
