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

    // TODO:  doc
    boost::scoped_array<FixedBuffer> splitTupleBuffer;
    boost::scoped_array<FixedBuffer> parentTupleBuffer;

    /**
     * Buffer used for marshalling in insertTupleData.
     */
    boost::scoped_array<FixedBuffer> leafTupleBuffer;

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
     * Splits a node to free up space for inserting a tuple, and then performs
     * the actual insertion.
     *
     * @param node the node to be split
     *
     * @param pTupleBuffer buffer containing the tuple being inserted
     *
     * @param cbTuple number of bytes in pTupleBuffer
     *
     * @param iNewTuple desired 0-based position for new tuple
     */
    void splitNode(
        BTreePageLock &pageLock,
        PConstBuffer pTupleBuffer,
        uint cbTuple,
        uint iNewTuple);

    /**
     * The tree grows by one level. The root node should have two entries
     * the first entry points to left node and the second entry points 
     * the right node. The root page id is kept.
     *
     * @param node the left node
     *
     * @param pageId the origianl root page id.
     *
     * @param rightNode the right node.
     *
     * @param rightPageId the right page id.
     */
    void grow(BTreeNode &node, PageId pageId, 
              BTreeNode &rightNode, PageId rightPageId);

    /**
     * try to find the parent page, put the result into pageId.
     * and lock the parent page inside pageLock.
     *
     * @param height is the current height of the btree.
     */
    uint lockParentPage(int height);

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

public:
    /**
     * Creates a new BTreeWriter.
     *
     * @param descriptor descriptor for tree to be accessed
     *
     * @param scratchAccessor accessor for scratch segment used in splits
     */
    explicit BTreeWriter(
        BTreeDescriptor const &descriptor,
        SegmentAccessor const &scratchAccessor);
    
    virtual ~BTreeWriter();

    /**
     * Inserts a tuple from unmarshalled TupleData form.  See
     * insertTupleFromBuffer for duplicate handling.
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
     * will be thrown.  If distinctness is set to DUP_DISCARD, the new
     * tuple is not inserted.  Otherwise (DUP_ALLOW), the tuple is inserted
     * with a duplicate key.
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
