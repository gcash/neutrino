package neutrino

import (
	"bytes"
	"errors"

	"github.com/gcash/bchd/chaincfg/chainhash"
	"github.com/gcash/bchd/wire"
	"github.com/gcash/bchutil/gcs/builder"
)

// FoundSpend is a spend that SearchSpends found.
type FoundSpend struct {
	// OutPoint is the outpoint that was spent.
	OutPoint wire.OutPoint

	// SpendingTx is the transaction that spent it, and SpendingInputIndex
	// the input that did.
	SpendingTx         *wire.MsgTx
	SpendingInputIndex uint32

	// Height and BlockHash are those of the transaction's block.
	Height    int32
	BlockHash chainhash.Hash
}

// SearchSpends scans block filters for the blocks that spend the passed
// outpoints, from height from to height to, both included, in that direction:
// up, or down when from is above to.  A filter holds every outpoint its block
// spends, so a search needs nothing but the outpoints, whose outputs it never
// looks at, and starts at any height.  All the outpoints share one pass, and it
// ends once none are left.
//
// found, if it isn't nil, is called with each spend, and in a search up
// returns the outpoints to watch from there on, so that one pass follows a
// chain of spends.  Those can be spent in the same block.  A search down
// ignores what it returns.
//
// progress, if it isn't nil, is called with each height that is done.  The
// height the search got to is also returned, with an error too, and a search
// resumes from the height after it, in its direction.  It is from less one step
// if no height was finished, and short of to with no error if no outpoint was
// left to watch.  A search that quit closes, or that the service shuts down,
// returns ErrSearchCancelled.
//
// An outpoint that wasn't found was unspent in the range as far as the filters
// tell, which is as far as the filter headers that the peers agreed on are
// right: peers that agree on wrong ones can hide a spend until a peer that
// disagrees has them rolled back.  The search follows the chain as it is at
// each height, so a reorganization while it runs can leave blocks it passed off
// the chain; a caller checks that the chain didn't change, as by the hash of the
// block it got to, before it counts the range as searched.
//
// A service that syncs script filters can't search this way, as a script
// filter holds the script of each output its block spends in place of the
// outpoint, and returns ErrSpendSearchNeedsScripts: SearchScriptSpends takes
// the scripts too.
func (s *ChainService) SearchSpends(outpoints []wire.OutPoint, from, to int32,
	found func(FoundSpend) []wire.OutPoint, progress func(height int32),
	quit <-chan struct{}) (int32, error) {

	return searchSpends(s, outpoints, from, to, found, progress, quit, s.quit)
}

// ErrSpendSearchNeedsScripts is returned by SearchSpends on a service that
// syncs script filters, which match a spend by the script of the output
// spent, not by its outpoint.
var ErrSpendSearchNeedsScripts = errors.New("a spend search on script " +
	"filters needs the script of each outpoint's output")

// SearchScriptSpends is SearchSpends for outpoints that come with the scripts
// of their outputs, which is what a service that syncs script filters matches
// a spend by.  found returns the outpoints to watch from there on with their
// scripts too.  On a service that syncs regular filters it searches by the
// outpoints, as SearchSpends does.
//
// A script filter matches every block that pays or spends an output with one
// of the scripts, so a search for an outpoint whose script is used often
// fetches more blocks than one on regular filters.
func (s *ChainService) SearchScriptSpends(inputs []InputWithScript, from, to int32,
	found func(FoundSpend) []InputWithScript, progress func(height int32),
	quit <-chan struct{}) (int32, error) {

	return searchScriptSpends(s, inputs, from, to, found, progress, quit, s.quit)
}

// searchScriptSpends is SearchScriptSpends on a source.
func searchScriptSpends(source searchSource, inputs []InputWithScript, from, to int32,
	found func(FoundSpend) []InputWithScript, progress func(height int32),
	quit, shutdown <-chan struct{}) (int32, error) {

	scriptFilters := filterTypeOf(source) == wire.GCSFilterScripts
	watched := make(map[wire.OutPoint]struct{}, len(inputs))
	scripts := make(map[wire.OutPoint][]byte, len(inputs))
	for _, input := range inputs {
		watched[input.OutPoint] = struct{}{}
		scripts[input.OutPoint] = input.PkScript
	}

	// A script filter is matched with the keys of the scripts of the
	// outpoints still watched, each once.
	entries := func() ([][]byte, error) {
		if !scriptFilters {
			return spendFilterEntries(watched)
		}
		keys := make(map[[32]byte]struct{}, len(watched))
		match := make([][]byte, 0, len(watched))
		for outpoint := range watched {
			key := builder.ScriptKey(scripts[outpoint])
			if _, ok := keys[key]; !ok {
				keys[key] = struct{}{}
				match = append(match, key[:])
			}
		}
		return match, nil
	}

	// The outpoints to watch from a spend on come with their scripts,
	// which are kept for the filters that follow.
	var follow func(FoundSpend) []wire.OutPoint
	if found != nil {
		follow = func(spend FoundSpend) []wire.OutPoint {
			next := found(spend)
			outpoints := make([]wire.OutPoint, len(next))
			for i, input := range next {
				outpoints[i] = input.OutPoint
				scripts[input.OutPoint] = input.PkScript
			}
			return outpoints
		}
	}
	onBlock := func(block *wire.MsgBlock, hash *chainhash.Hash, height int32) (scanStep, bool) {
		changed := findBlockSpends(block, hash, height, watched, follow, from <= to)
		if len(watched) == 0 {
			return scanFinished, changed
		}
		return scanOn, changed
	}
	return scanFilters(source, from, to, entries, onBlock, progress, quit, shutdown)
}

// searchSpends is SearchSpends on a source.
func searchSpends(source searchSource, outpoints []wire.OutPoint, from, to int32,
	found func(FoundSpend) []wire.OutPoint, progress func(height int32),
	quit, shutdown <-chan struct{}) (int32, error) {

	if filterTypeOf(source) == wire.GCSFilterScripts {
		reached := from - 1
		if from > to {
			reached = from + 1
		}
		return reached, ErrSpendSearchNeedsScripts
	}

	watched := make(map[wire.OutPoint]struct{}, len(outpoints))
	for _, outpoint := range outpoints {
		watched[outpoint] = struct{}{}
	}
	entries := func() ([][]byte, error) {
		return spendFilterEntries(watched)
	}
	onBlock := func(block *wire.MsgBlock, hash *chainhash.Hash, height int32) (scanStep, bool) {
		changed := findBlockSpends(block, hash, height, watched, found, from <= to)
		if len(watched) == 0 {
			return scanFinished, changed
		}
		return scanOn, changed
	}
	return scanFilters(source, from, to, entries, onBlock, progress, quit, shutdown)
}

// spendFilterEntries returns the outpoints as a block's filter holds them.
func spendFilterEntries(watched map[wire.OutPoint]struct{}) ([][]byte, error) {
	entries := make([][]byte, 0, len(watched))
	for outpoint := range watched {
		var buf bytes.Buffer
		if err := outpoint.Serialize(&buf); err != nil {
			return nil, err
		}
		entries = append(entries, buf.Bytes())
	}
	return entries, nil
}

// blockSpend is an input of a block.
type blockSpend struct {
	tx       *wire.MsgTx
	input    uint32
	reported bool
}

// findBlockSpends reports the spends in block of the watched outpoints, and
// returns whether the outpoints changed.  A block needn't list a transaction
// after the one it spends, so it is indexed once, and a chain of spends in it
// is then followed by lookups, however long it is and whatever its order.
func findBlockSpends(block *wire.MsgBlock, hash *chainhash.Hash, height int32,
	watched map[wire.OutPoint]struct{}, found func(FoundSpend) []wire.OutPoint,
	follow bool) bool {

	// Most blocks a filter matches spend nothing watched, as a filter
	// matches falsely too, so the index waits for a spend.
	var pending []wire.OutPoint
	for _, tx := range block.Transactions {
		for _, txIn := range tx.TxIn {
			if _, ok := watched[txIn.PreviousOutPoint]; ok {
				pending = append(pending, txIn.PreviousOutPoint)
			}
		}
	}
	if len(pending) == 0 {
		return false
	}
	spends := make(map[wire.OutPoint]*blockSpend)
	for _, tx := range block.Transactions {
		for i, txIn := range tx.TxIn {
			spends[txIn.PreviousOutPoint] = &blockSpend{tx: tx, input: uint32(i)}
		}
	}

	changed := false
	for len(pending) > 0 {
		outpoint := pending[0]
		pending = pending[1:]
		delete(watched, outpoint)
		changed = true
		spend := spends[outpoint]
		spend.reported = true
		if found == nil {
			continue
		}

		next := found(FoundSpend{
			OutPoint:           outpoint,
			SpendingTx:         spend.tx,
			SpendingInputIndex: spend.input,
			Height:             height,
			BlockHash:          *hash,
		})
		if !follow {
			continue
		}
		for _, outpoint := range next {
			// One already watched is already queued if this block
			// spends it, and one reported can't be spent again.
			spend, inBlock := spends[outpoint]
			_, isWatched := watched[outpoint]
			if isWatched || (inBlock && spend.reported) {
				continue
			}
			watched[outpoint] = struct{}{}
			if inBlock {
				pending = append(pending, outpoint)
			}
		}
	}
	return changed
}
