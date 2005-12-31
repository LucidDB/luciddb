/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 1999-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by the Free
// Software Foundation; either version 2 of the License, or (at your option)
// any later version approved by The Eigenbase Project.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/

#ifndef Fennel_BTreeNodeAccessor_Included
#define Fennel_BTreeNodeAccessor_Included

#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/tuple/TupleAccessor.h"
#include "fennel/tuple/TupleData.h"
#include "fennel/btree/BTreeNode.h"

FENNEL_BEGIN_NAMESPACE

/**
 * BTreeNodeAccessor is an abstract base class for accessing the sorted tuple
 * data stored on a BTreeNode.  Derived classes fill in the implementation
 * details.
 *
 *<p>
 *
 * The general idea is that low-level management of node data is orthogonal to
 * high-level BTree algorithms, so it should be easy to plug in new node
 * representations (fixed-width, variable-width, compressed, etc.).  An
 * alternate way of expressing this would be to define all of the high-level
 * BTree classes as templates, with BTreeNodeAccessor as a template parameter.
 * This would give the ultimate efficiency, but at the price of code bloat
 * (number of high-level classes times number of BTreeNodeAccessor
 * implementations) and higher complexity.  A virtual interface was chosen
 * instead, but still with performance in mind (e.g. the binarySearch method is
 * delegated to BTreeNodeAccessor so that all tuple access within one node
 * search can be computed inline).
 *
 */
class BTreeNodeAccessor
{
public:
    /**
     * Descriptor for tuples stored on nodes accessed by this.
     */
    TupleDescriptor tupleDescriptor;

    /**
     * Accessor for tuples stored on nodes accessed by this.  This is used as
     * a scratch variable in a variety of contexts, so reference with caution.
     */
    TupleAccessor tupleAccessor;

    /**
     * TupleData for tuples stored on nodes accessed by this.  This is used as
     * a scratch variable in a variety of contexts, so reference with caution.
     */
    TupleData tupleData;

    virtual ~BTreeNodeAccessor();

    /**
     * Gets the number of keys stored on a node.  This may be one less
     * than the number of tuples, since in the rightmost node on a non-leaf
     * level, we pretend the last key is +infinity, so the actual stored
     * key is ignored.  But the last tuple is still stored, and
     * its child PageId is used.
     *
     * @param node the node to access
     *
     * @return number of keys stored on node
     */
    inline uint getKeyCount(BTreeNode const &node) const;
    
    /**
     * Clears the contents of a node, converting it to an empty root.
     *
     * @param node the node to clear
     *
     * @param cbPage number of usable bytes per page in segment storing
     * BTree
     */
    virtual void clearNode(BTreeNode &node,uint cbPage);

    /**
     * Allocates space for a new tuple.
     *
     * @param node the node in which the tuple will be stored
     *
     * @param iEntry 0-based index at which tuple will be stored
     *
     * @param cbEntry number of bytes in stored tuple
     *
     * @return location at which tuple should be stored
     */
    virtual PBuffer allocateEntry(
        BTreeNode &node,uint iEntry,uint cbEntry) = 0;

    /**
     * Deallocates the space for an existing tuple.
     *
     * @param node the node from which to deallocate the tuple
     *
     * @param iEntry 0-based index of tuple to be deallocated
     */
    virtual void deallocateEntry(
        BTreeNode &node,uint iEntry) = 0;

    /**
     * Gets the location of a stored tuple.
     *
     * @param node the node to access
     *
     * @param iEntry 0-based index of tuple to access
     *
     * @return location of stored tuple
     */
    virtual PConstBuffer getEntryForRead(
        BTreeNode const &node,uint iEntry) = 0;

    virtual PConstBuffer getEntryForReadInline(
        BTreeNode const &node,uint iEntry) = 0;

    /**
     * Receives notification from BTreeAccessBase after tupleDescriptor has
     * been set up.
     */
    virtual void onInit();

    /**
     * Dumps the contents of a node.
     *
     * @param os output stream receiving the dump
     *
     * @param node the node to dump
     *
     * @param pageId PageId of the node being dumped
     */
    void dumpNode(std::ostream &os,BTreeNode const &node,PageId pageId);

    /**
     * @return true iff this NodeAccessor always stores tuples
     * in fixed-width slots (even if the tuples themselves are
     * variable-length)
     */
    virtual bool hasFixedWidthEntries() const = 0;

    /**
     * Binds tupleAccessor to a stored tuple.
     *
     * @param node the node to access
     *
     * @param iEntry 0-based index of tuple to access
     */
    virtual void accessTuple(BTreeNode const &node,uint iEntry) = 0;

    /**
     * Searches for a tuple by its key.
     *
     * @param node the node to search
     *
     * @param keyDescriptor key descriptor to be used for comparisons
     *
     * @param searchKey key to search for
     *
     * @param dupSeek what to do if duplicates are found
     *
     * @param leastUpper whether to position on least upper bound or
     * greatest lower bound
     *
     * @param scratchKey key to be used as a temp variable in comparisons
     *
     * @param found same semantics as BTreeReader::binarySearch
     */
    virtual uint binarySearch(
        BTreeNode const &node,
        TupleDescriptor const &keyDescriptor,
        TupleData const &searchKey,
        DuplicateSeek dupSeek,
        bool leastUpper,
        TupleData &scratchKey,
        bool &found) = 0;

    /**
     * Compare first key on a node to provided search key.
     *
     * @param node the node to search
     *
     * @param keyDescriptor key descriptor to be used for comparisons
     *
     * @param searchKey key to search for
     *
     * @param scratchKey key to be used as a temp variable in comparison
     *
     * @return result of comparing searchKey with the first key (0, -1, or 1)
     */
    virtual int compareFirstKey(
        BTreeNode const &node,
        TupleDescriptor const &keyDescriptor,
        TupleData const &searchKey,
        TupleData &scratchKey) = 0;

    /**
     * Sets tuple accessor to provided node entry
     *
     * @param node the current node positioned on
     *
     * @param iEntry the entry within the node to set the tuple accessor to
     */
    virtual void accessTupleInline(BTreeNode const &node, uint iEntry) = 0;

    /**
     * Enumeration used to classify the storage state of a node which
     * is targeted for insertion.
     */
    enum Capacity {
        /**
         * The tuple can fit without compaction.
         */
        CAN_FIT,
        /**
         * The tuple can fit, but only after compaction.
         */
        CAN_FIT_WITH_COMPACTION,

        /**
         * The tuple can't fit.  Compaction wouldn't help.
         */
        CAN_NOT_FIT
    };

    /**
     * Determines whether a tuple can be inserted into a node.
     *
     * @param node the target node
     *
     * @param cbEntry the number of bytes in the tuple to be inserted
     *
     * @return see Capacity
     */
    virtual Capacity calculateCapacity(
        BTreeNode const &node,uint cbEntry) = 0;

    /**
     * Determines the storage required for a tuple, including any overhead.
     *
     * @param cbTuple number of bytes without overhead
     *
     * @return number of bytes with overhead
     */
    virtual uint getEntryByteCount(uint cbTuple) = 0;

    /**
     * Unmarshals the key for the current tuple after a call to accessTuple.
     *
     * @param keyData receives the unmarshalled key
     */
    virtual void unmarshalKey(TupleData &keyData) = 0;

    /**
     * Performs compaction on a node.
     *
     * @param node the fragmented node to be compacted
     *
     * @param scratchNode receives a compacted copy of node
     */
    virtual void compactNode(BTreeNode &node,BTreeNode &scratchNode);

    /**
     * Splits a node.
     *
     * @param node the node to be split
     *
     * @param newNode an empty node which receives half of the tuple data.
     *
     * @param cbNewTuple number of bytes which will be required to store the
     * tuple which caused this split
     *
     * @param monotonic if true, inserts are always increasing so optimize
     * the split accordingly
     */
    virtual void splitNode(
        BTreeNode &node,BTreeNode &newNode,uint cbNewTuple,bool monotonic);
};

inline uint BTreeNodeAccessor::getKeyCount(BTreeNode const &node) const
{
    if (node.height && (node.rightSibling == NULL_PAGE_ID)) {
        // For non-leaf nodes on the rightmost fringe, we pretend the last
        // key is +infinity, and ignore whatever's actually stored
        // there (except for its child PageId).
        assert(node.nEntries);
        return node.nEntries - 1;
    }
    return node.nEntries;
}

FENNEL_END_NAMESPACE

#endif

// End BTreeNodeAccessor.h
