package headerlist

import (
	"github.com/gcash/bchd/wire"
	"github.com/gcash/neutrino/headerfs"
)

// Chain is an interface that stores a list of Nodes. Each node represents a
// header in the main chain and also includes a height along with it. This is
// meant to serve as a replacement to list.List which provides similar
// functionality, but allows implementations to use custom storage backends and
// semantics.
type Chain interface {
	// ResetHeaderState resets the state of all nodes. After this method, it will
	// be as if the chain was just newly created.
	ResetHeaderState(Node, headerfs.BlockHeaderStore) error

	// Back returns the end of the chain. If the chain is empty, then this
	// return a pointer to a nil node.
	Back() *Node

	// Front returns the head of the chain. If the chain is empty, then
	// this returns a  pointer to a nil node.
	Front() *Node

	// PushBack will push a new entry to the end of the chain. The entry
	// added to the chain is also returned in place.
	PushBack(Node) *Node

	// Fetch the given number of ancestor headers for the given node
	FetchHeaderAncestors(*Node, int) []*wire.BlockHeader
}

// Node is a node within the Chain. Each node stores a header as well as a
// height. Nodes can also be used to traverse the chain backwards via their
// Prev() method.
type Node struct {
	// Height is the height of this node within the main chain.
	Height int32

	// Header is the header that this node represents.
	Header wire.BlockHeader

	prev *Node
}

// Prev attempts to access the prior node within the header chain relative to
// this node. If this is the start of the chain, then this method will return
// nil.
func (n *Node) Prev() *Node {
	return n.prev
}

// SetPrev sets the previous node for this node
func (n *Node) SetPrev(prev *Node) {
	n.prev = prev
}