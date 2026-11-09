package neutrino

import (
	"bytes"

	"github.com/gcash/bchd/chaincfg/chainhash"
	"github.com/gcash/bchd/wire"
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
// if no height was finished.  An outpoint that wasn't found was unspent in the
// range.  A search that quit closes, or that the service shuts down, returns
// ErrSearchCancelled.
func (s *ChainService) SearchSpends(outpoints []wire.OutPoint, from, to int32,
	found func(FoundSpend) []wire.OutPoint, progress func(height int32),
	quit <-chan struct{}) (int32, error) {

	return searchSpends(s, outpoints, from, to, found, progress, quit, s.quit)
}

// searchSpends is SearchSpends on a source.
func searchSpends(source searchSource, outpoints []wire.OutPoint, from, to int32,
	found func(FoundSpend) []wire.OutPoint, progress func(height int32),
	quit, shutdown <-chan struct{}) (int32, error) {

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
