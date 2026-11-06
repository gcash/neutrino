package neutrino

import (
	"bytes"
	"errors"

	"github.com/gcash/bchd/chaincfg/chainhash"
	"github.com/gcash/bchd/wire"
	"github.com/gcash/bchutil"
	"github.com/gcash/bchutil/gcs"
	"github.com/gcash/bchutil/gcs/builder"
)

// ErrSpendSearchCancelled is returned by a search that was told to stop.
var ErrSpendSearchCancelled = errors.New("spend search cancelled")

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

// spendSearchSource is what a search reads.  ChainService implements it.
type spendSearchSource interface {
	GetBlockHash(height int64) (*chainhash.Hash, error)
	GetCFilter(hash chainhash.Hash, filterType wire.FilterType,
		options ...QueryOption) (*gcs.Filter, error)
	GetBlock(hash chainhash.Hash, options ...QueryOption) (*bchutil.Block, error)
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
// ErrSpendSearchCancelled.
func (s *ChainService) SearchSpends(outpoints []wire.OutPoint, from, to int32,
	found func(FoundSpend) []wire.OutPoint, progress func(height int32),
	quit <-chan struct{}) (int32, error) {

	return searchSpends(s, outpoints, from, to, found, progress, quit, s.quit)
}

// searchSpends is SearchSpends on a source.
func searchSpends(source spendSearchSource, outpoints []wire.OutPoint, from, to int32,
	found func(FoundSpend) []wire.OutPoint, progress func(height int32),
	quit, shutdown <-chan struct{}) (int32, error) {

	step, batch := int32(1), OptimisticBatch()
	if from > to {
		step, batch = -1, OptimisticReverseBatch()
	}
	watched := make(map[wire.OutPoint]struct{}, len(outpoints))
	for _, outpoint := range outpoints {
		watched[outpoint] = struct{}{}
	}

	reached := from - step
	entries, err := spendFilterEntries(watched)
	if err != nil {
		return reached, err
	}
	for height := from; len(watched) > 0 && height != to+step; height += step {
		select {
		case <-quit:
			return reached, ErrSpendSearchCancelled
		case <-shutdown:
			return reached, ErrSpendSearchCancelled
		default:
		}

		hash, err := source.GetBlockHash(int64(height))
		if err != nil {
			return reached, err
		}
		matched, err := spendFilterMatches(source, hash, entries, batch, QueryQuit(quit))
		if err != nil {
			return reached, stopped(err, quit, shutdown)
		}
		if matched {
			block, err := source.GetBlock(*hash, QueryQuit(quit))
			if err != nil {
				return reached, stopped(err, quit, shutdown)
			}
			if block == nil {
				return reached, errors.New("no block for a filter that matched")
			}
			changed := findBlockSpends(block.MsgBlock(), hash, height, watched, found, step > 0)
			if changed {
				entries, err = spendFilterEntries(watched)
				if err != nil {
					return reached, err
				}
			}
		}
		reached = height
		if progress != nil {
			progress(height)
		}
	}
	return reached, nil
}

// stopped returns ErrSpendSearchCancelled for an error that ending the search
// brought, as a query that's told to quit just ends without what it asked for.
func stopped(err error, quit, shutdown <-chan struct{}) error {
	select {
	case <-quit:
		return ErrSpendSearchCancelled
	case <-shutdown:
		return ErrSpendSearchCancelled
	default:
		return err
	}
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

// spendFilterMatches returns whether the filter of the block matches any of the
// entries.  It only ever tells that a block may hold one.
func spendFilterMatches(source spendSearchSource, hash *chainhash.Hash, entries [][]byte,
	options ...QueryOption) (bool, error) {

	filter, err := source.GetCFilter(*hash, wire.GCSFilterRegular, options...)
	if err != nil {
		return false, err
	}
	if filter == nil {
		return false, ErrFilterFetchFailed
	}
	if filter.N() == 0 {
		return false, nil
	}
	return filter.MatchAny(builder.DeriveKey(hash), entries)
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

	spends := make(map[wire.OutPoint]*blockSpend)
	var pending []wire.OutPoint
	for _, tx := range block.Transactions {
		for i, txIn := range tx.TxIn {
			spends[txIn.PreviousOutPoint] = &blockSpend{tx: tx, input: uint32(i)}
			if _, ok := watched[txIn.PreviousOutPoint]; ok {
				pending = append(pending, txIn.PreviousOutPoint)
			}
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
