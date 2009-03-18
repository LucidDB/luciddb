/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 1999-2007 John V. Sichi
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

#ifndef Fennel_BTreeWriter_Included
#define Fennel_BTreeWriter_Included

#include "fennel/btree/BTreeReader.h"
#include "fennel/txn/LogicalTxnParticipant.h"
#include "fennel/common/FemEnums.h"

#include <boost/scoped_array.hpp>

FENNEL_BEGIN_NAMESPACE

/**
 * BTreeWriter extends BTreeReader to provide read-write access to the contents
 * of a BTree.  Optionally, it can also be used as a transaction participant.
 */
class BTreeWriter : public BTreeReader, public LogicalTxnParticipant
{
    /**
     * LogicalActionType for inserting an entry into a BTree.
     */
    static const LogicalActionType ACTION_INSERT = 1;

    /**
     * LogicalActionType for deleting an entry from a BTree.
     */
    static const LogicalActionType ACTION_DELETE = 2;

    /**
     * Accessor for scratch segment.
     */
    SegmentAccessor scratchAccessor;

    /**
     * Lock on scratch page used during splits.
     */
    BTreePageLock scratchPageLock;

    /**
     * Stack of rightmost non-leaf pages seen while searching in preparation
     * for an insertion; this is used in reverse while splitting.
     */
    std::vector<PageId> pageStack;

    /**
     * Buffer used for storing entry to be inserted into parent node
     * during split.
     */
    boost::scoped_array<FixedBuffer> splitTupleBuffer;

    /**
     * Buffer used for marshalling in insertTupleData.
     */
    boost::scoped_array<FixedBuffer> leafTupleBuffer;

    /**
     * If true, caller promises inserts will be strictly increasing.
     */
    bool monotonic;

    inline void optimizeRootLockMode();

    /**
     * Performs compaction on a node to free up space for inserting a tuple.
     * This method uses swapBuffers for efficiency; as a side-effect,
     * pointers to BTreeNodes may be invalidated, so use with caution.
     * (That's also why the parameter is a page lock rather than a
     * node reference).
     *
     * @param targetPageLock lock on node to be compacted
     */
    void compactNode(BTreePageLock &targetPageLock);

    /**
     * Splits the current node locked by pageLock to free up space for
     * inserting a tuple, and then performs the actual insertion.
     *
     * @param pTupleBuffer buffer containing the tuple being inserted
     *
     * @param cbTuple number of bytes in pTupleBuffer
     *
     * @param iNewTuple desired 0-based position for new tuple
     */
    void splitCurrentNode(
        PConstBuffer pTupleBuffer,
        uint cbTuple,
        uint iNewTuple);

    /**
     * Grows the tree by one level, preserving the root PageId.  On entry, the
     * original root (identified by pageId) should be held by pageLock.  On
     * return, the new root node will have two entries: the first entry will
     * point to a copy of the old root on a new page, and the
     * second entry will point to rightNode.
     *
     * @param rightNode the right-hand split of the old root
     *
     * @param rightPageId PageId corresponding to rightNode
     */
    void grow(
        BTreeNode const &rightNode, PageId rightPageId);

    /**
     * Finds the parent page by using the page stack (plus searches if root
     * splits are encountered).  On return, the result is in pageId, and the
     * parent page has been accquired by pageLock.
     *
     * @param height is the current height of the btree.
     *
     * @param rightMostNode true if the node being split is the rightmost
     * node at that level in the btree; thus, the entry in the parent page
     * corresponding to that node is the infinity key
     *
     * @return 0-based entry position on parent page corresponding
     * to searchKeyData
     */
    uint lockParentPage(uint height, bool rightMostNode);

    /**
     * Attempts to perform an insertion without splitting, performing
     * compaction automatically if it will allow the insertion to succeed.
     * See warnings on compactNode.
     *
     * @param targetPageLock lock on node into which tuple is to be inserted
     *
     * @param pTupleBuffer buffer containing the tuple being inserted
     *
     * @param cbTuple number of bytes in pTupleBuffer
     *
     * @param iNewTuple desired 0-based position for new tuple
     *
     * @return true if insertion succeeded; false if a split is required
     */
    bool attemptInsertWithoutSplit(
        BTreePageLock &targetPageLock,
        PConstBuffer pTupleBuffer,uint cbTuple,uint iNewTuple);

    /**
     * Inserts a tuple read from a log stream.
     *
     * @param logStream stream containing tuple image
     */
    void insertLogged(ByteInputStream &logStream);

    /**
     * Deletes a tuple read from a log stream.
     *
     * @param logStream stream containing tuple image
     */
    void deleteLogged(ByteInputStream &logStream);

    /**
     * Positions search key for insert, also detecting duplicate key values
     *
     * @param nodeAccessor node accessor for leaf node
     *
     * @return true if duplicate key found
     */
    bool positionSearchKey(BTreeNodeAccessor &nodeAccessor);

    /**
     * Checks to ensure that when monotonic insert mode is used,
     * the keys really are increasing.  Note though that the check
     * is only done for the 2nd and subsequent keys on a leaf page.
     * I.e., the check is not done across page boundaries.
     *
     * @param nodeAccessor node accessor for leaf node
     *
     * @param pTupleBuffer tuple buffer for new key to be inserted
     *
     * @return true if new key is > previous key and it will be inserted
     * in the last position in the node
     */
    bool checkMonotonicity(
        BTreeNodeAccessor &nodeAccessor, PConstBuffer pTupleBuffer);

public:
    /**
     * Creates a new BTreeWriter.
     *
     * @param descriptor descriptor for tree to be accessed
     *
     * @param scratchAccessor accessor for scratch segment used in splits
     *
     * @param monotonic if true, inserts are always increasing
     */
    explicit BTreeWriter(
        BTreeDescriptor const &descriptor,
        SegmentAccessor const &scratchAccessor,
        bool monotonic = false);

    virtual ~BTreeWriter();

    /**
     * Inserts a tuple from unmarshalled TupleData form; requires this writer
     * to already be positioned to the correct location (the caller is trusted,
     * with no verification).  See insertTupleFromBuffer for duplicate
     * handling.
     *
     * @param tupleData tuple to be inserted
     *
     * @param distinctness how to handle duplicates
     */
    void insertTupleData(
        TupleData const &tupleData,
        Distinctness distinctness);

    /**
     * Inserts a tuple from a marshalled tuple buffer.  If the key already
     * exists, and distinctness is set to DUP_FAIL, a BTreeDuplicateKeyExcn
     * will be thrown (or an assertion failure if monotonic mode is enabled).
     * If distinctness is set to DUP_DISCARD, the new tuple is not inserted.
     * Otherwise (DUP_ALLOW), the tuple is inserted with a duplicate key.
     * In monotonic mode, only DUP_FAIL is allowed.
     *
     * @param pTupleBuffer buffer containing tuple to be inserted
     *
     * @param distinctness how to handle duplicates
     */
    uint insertTupleFromBuffer(
        PConstBuffer pTupleBuffer,Distinctness distinctness);

    /**
     * Deletes the current tuple.  Can be called after one
     * of the BTreeReader search methods.  Deletion invalidates the current
     * tuple, but the next tuple can still be accessed by calling searchNext().
     */
    void deleteCurrent();

    /**
     * Attempts to update the current tuple's value without changing its key and
     * without splitting the page.  Can be called after one of the
     * BTreeReader search methods.  It is the caller's responsibility to ensure
     * that the key is preserved (otherwise index corruption results).  This
     * action is NOT logged; an assertion failure will result if logging is
     * enabled.
     *
     * @param tupleData new tuple data
     *
     * @return true if successful; false if request failed because a split
     * would have been required
     */
    bool updateCurrent(TupleData const &tupleData);

    /**
     * Releases any allocated scratch buffers.
     */
    void releaseScratchBuffers();

    // implement LogicalTxnParticipant
    virtual LogicalTxnClassId getParticipantClassId() const;
    virtual void describeParticipant(
        ByteOutputStream &logStream);
    virtual void undoLogicalAction(
        LogicalActionType actionType,
        ByteInputStream &logStream);
    virtual void redoLogicalAction(
        LogicalActionType actionType,
        ByteInputStream &logStream);
};

FENNEL_END_NAMESPACE

#endif

// End BTreeWriter.h
