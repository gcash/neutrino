package neutrino

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/gcash/bchd/wire"
	"github.com/gcash/bchutil"
	"github.com/gcash/bchutil/gcs/builder"
	"github.com/gcash/neutrino/banman"
)

// ErrBlockFilterMismatch is returned by GetBlock when the filter built from a
// downloaded block doesn't match the filter header synced for it. The synced
// filter headers are then wrong from that block on, so filters that rely on
// them can leave out a wallet's transactions.
var ErrBlockFilterMismatch = errors.New("block doesn't match its filter " +
	"header")

// verifyBlockFilter checks that the regular filter built from a downloaded
// block matches the filter header synced for it. The regular filter holds the
// outpoints a block spends and its output scripts, so it is built from the
// block alone. On a mismatch, the peers serving a different filter for the
// block are banned, a repair of the filter headers is requested, and
// ErrBlockFilterMismatch is returned.
func (s *ChainService) verifyBlockFilter(block *bchutil.Block,
	height uint32) error {

	// The genesis filter header isn't synced, and a block whose filter
	// header isn't synced yet can't be checked.
	_, filterTip, err := s.RegFilterHeaders.ChainTip()
	if err != nil || height == 0 || height > filterTip {
		return nil
	}

	prevHeader, err := s.RegFilterHeaders.FetchHeaderByHeight(height - 1)
	if err != nil {
		return nil
	}
	filterHeader, err := s.RegFilterHeaders.FetchHeaderByHeight(height)
	if err != nil {
		return nil
	}

	filter, err := builder.BuildBasicFilter(block.MsgBlock())
	if err != nil {
		return err
	}
	expected, err := builder.MakeHeaderForFilter(filter, *prevHeader)
	if err != nil {
		return err
	}
	if expected == *filterHeader {
		return nil
	}

	// The filter headers read above belong to another block if a reorg
	// replaced this one in the meantime.
	header, err := s.BlockHeaders.FetchHeaderByHeight(height)
	if err != nil || header.BlockHash() != *block.Hash() {
		return nil
	}

	log.Warnf("Filter header %v at height %d doesn't match block %v, "+
		"checking the filters peers serve for it", filterHeader, height,
		block.Hash())

	mismatch := fmt.Errorf("%w: block %v at height %d",
		ErrBlockFilterMismatch, block.Hash(), height)

	// The filter headers we synced may be wrong from this block or one
	// before it, which a repair finds out and fixes.
	s.blockManager.requestFilterHeaderRepair()

	// Ask every peer for the block's filter. Peers are only banned when
	// another peer serves the built filter, so a block whose filter
	// another BCHD version builds differently bans no one.
	fType := wire.GCSFilterRegular
	filters := s.blockManager.fetchFilterFromAllPeers(
		height, *block.Hash(), fType,
	)
	builtBytes, err := filter.NBytes()
	if err != nil {
		return err
	}
	served := false
	for _, f := range filters {
		b, err := f.NBytes()
		if err == nil && bytes.Equal(b, builtBytes) {
			served = true
			break
		}
	}
	if !served {
		log.Warnf("No peer serves the filter built from block %v, "+
			"not banning any", block.Hash())
		return mismatch
	}

	badPeers, err := resolveFilterMismatchFromBlock(
		block.MsgBlock(), fType, filters, 0,
	)
	if err != nil {
		return err
	}
	for _, peer := range badPeers {
		log.Warnf("Peer %v served a filter that doesn't match block "+
			"%v", peer, block.Hash())

		if err := s.BanPeer(peer, banman.InvalidFilterHeader); err != nil {
			log.Errorf("Unable to ban peer %v: %v", peer, err)
		}
		if sp := s.PeerByAddr(peer); sp != nil {
			sp.Disconnect()
		}
	}

	return mismatch
}
