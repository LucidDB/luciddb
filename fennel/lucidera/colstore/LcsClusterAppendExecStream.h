/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 LucidEra, Inc.
// Copyright (C) 2005-2005 The Eigenbase Project
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

#ifndef Fennel_LcsClusterAppendExecStream_Included
#define Fennel_LcsClusterAppendExecStream_Included

#include "fennel/exec/ConduitExecStream.h"
#include "fennel/tuple/TupleData.h"
#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/tuple/TupleAccessor.h"
#include "fennel/btree/BTreeWriter.h"
#include "fennel/ftrs/BTreeExecStream.h"
#include "fennel/lucidera/colstore/LcsClusterNodeWriter.h"
#include "fennel/lucidera/colstore/LcsHash.h"
#include "fennel/lucidera/colstore/LcsClusterDump.h"
#include <boost/scoped_array.hpp>

FENNEL_BEGIN_NAMESPACE

    
struct LcsClusterAppendExecStreamParams : public BTreeExecStreamParams,
                                          public ConduitExecStreamParams
{
    /**
     * True if cluster append is in overwrite mode
     */
    bool overwrite;
    explicit LcsClusterAppendExecStreamParams()
    {
        overwrite = false;
    }
};

/**
 * Given a stream of tuples corresponding to the column values in a cluster,
 * loads the cluster pages 
 */
class LcsClusterAppendExecStream : public BTreeExecStream,
                                   public ConduitExecStream
{
    /**
     * Tuple descriptor for the tuple representing all cluster columns
     */
    TupleDescriptor clusterColsTupleDesc;

    /**
     * Tuple data for the tuple datums representing all cluster columns
     */
    TupleData clusterColsTupleData;

    /**
     * Individual tuple descriptors for each column in the cluster
     */
    boost::scoped_array<TupleDescriptor> colTupleDesc;

    /**
     * Individual tuple data for each column in the cluster
     */
    boost::scoped_array<TupleData> colTupleData;

    /**
     * Scratch accessor for allocating large buffer pages
     */
    SegmentAccessor scratchAccessor;

    /**
     * Lock on scratch page
     */
    ClusterPageLock bufferLock;

    /**
     * True if overwriting all existing data
     */
    bool m_bOverwrite;

    /**
     * Output tuple containing count of number of rows loaded
     */
    TupleData outputTuple;

    /**
     * True if execute has been called at least once
     */
    bool m_bCompressCalled;

    /**
     * Array of hashes, one per cluster column
     */
    boost::scoped_array<LcsHash> m_hash;

    /**
     * Number of columns in the cluster
     */
    uint m_numColumns;

    /**
     * Array of temporary blocks for row array
     */
    boost::scoped_array<PBuffer> m_rowBlock;
    
    /**
     * Maximum number of values that can be stored in m_rowBlock
     */
    uint nRowsMax;

    /**
     * Array of temporary blocks for hash table
     */
    boost::scoped_array<PBuffer> m_hashBlock;
    
    /**
     * Array of temporary blocks used by ClusterNodeWriter
     */
    boost::scoped_array<PBuffer> m_builderBlock;

    /**
     * Number of rows loaded into the current set of batches
     */
    uint m_rowCnt;

    /**
     * True if index blocks need to be written to disk
     */
    bool m_indexBlockDirty;

    /**
     * Starting rowid in a cluster page
     */
    LcsRid m_firstRow;

    /**
     * Last rowid in the last batch
     */
    LcsRid m_lastRow;

    /* First rowid in current load
     */
    LcsRid m_startRow;

    /**
     * Page builder object
     */
    LcsClusterNodeWriter m_riBlockBuilder;

    /**
     * Row value ordinal returned from hash, one per cluster column
     */
    boost::scoped_array<LcsHashValOrd> m_vOrd;

    /**
     * Temporary buffers used by WriteBatch
     */
    boost::scoped_array<boost::scoped_array<FixedBuffer> > m_buf;

    /**
     * Max size for each column cluster used by WriteBatch
     */
    boost::scoped_array<uint> m_maxValueSize;

    /**
     * Indicates where or not we have already allocated arrays
     */
    bool m_arraysAlloced;

    /**
     * Buffer pointing to cluster page that will actually be written
     */
    PLcsClusterNode m_indexBlock;

    /**
     * Total number of rows loaded by this object
     */
    RecordNum numRowCompressed;

    /**
     * Btree writer
     */
    SharedBTreeWriter m_btree;

    /**
     * Cluster pageid
     */
    PageId clusterPageId;

    /**
     * Cluster dump
     */
    SharedLcsClusterDump clusterDump;

    /**
     * Allocate memory for arrays
     */
    void AllocArrays();
    
    /**
     * Populates row and hash arrays from existing index block
     */
    void LoadExistingBlock();
    
    /**
     * Prepare to write a fresh block
     */
    void StartNewBlock();

    /**
     * Given a TupleData representing all columns in a cluster,
     * converts each column into its own TupleData
     */
    void convertTuplesToCols();

    /**
     * Adds value ordinal to row array for new row
     */
    void addValueOrdinal(uint column, uint16_t vOrd);

    /**
     * True if row array is full
     */
    bool IsRowArrayFull();

    /**
     * Writes a batch(run) to index block.
     * Batches have a multiple of 8 rows.
     *
     * @param lastBatch true if last batch
     */
    void WriteBatch(bool lastBatch);

    /**
     * Writes block to index when the block is full or this is the last block
     * in the load
     */
    void WriteBlock();

    /**
     * Gets last block written to disk so we can append to it, reading in the
     * first rid value stored on the page
     *
     * @param pBlock returns pointer to last cluster block, NULL if cluster
     * is empty
     */
    void GetLastBlock(PLcsClusterNode &pBlock);

public:
    /**
     * Tuple data representing the btree key
     */
    TupleData btreeTupleData;
   
    /**
     * Buffer lock for the actual cluster node pages.  Shares the same 
     * segment as the btree corresponding to the cluster.
     */
    ClusterPageLock clusterLock;

    /**
     * Space available on page blocks for writing cluster data
     */
    uint m_blockSize;

    /**
     * Performs minimal initialization of object
     */
    explicit LcsClusterAppendExecStream();

    /**
     * Initializes and sets up object with content specific to the load that
     * will be carried out
     */
    void Init();

    /**
     * Processes rows for loading.  Calls WriteBatch once values cannot fit
     * into a page
     *
     * @param quantum ExecStream quantum
     *
     * @return ExecStreamResult value
     */
    ExecStreamResult Compress(ExecStreamQuantum const &quantum);

    /**
     * Writes out the last pending batches and btree pages.  Deallocates
     * temporary memory and buffer pages
     */
    void Close();

#ifdef NOT_NEEDED_YET
    void SetNewBlockCount(uint cnt) {
        m_newBlockCount = cnt;
    }
    
    uint GetNewBlockCount() const {
        return m_newBlockCount;
    }

    RecordNum GetNumRows() const {
        return m_lastRow - m_startRow;
    }

    RID GetNextRow() const {
        return m_lastRow;
    }

    void SetLastRow(LcsRid row) {
        m_lastRow = row;
    } 

#endif

    /**
     * True iff we overwrite all the existing values in the cluster
     */
    bool IsInOverwriteMode() {
        return m_bOverwrite;
    }

public:
    virtual void prepare(LcsClusterAppendExecStreamParams const &params);
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
    virtual void getResourceRequirements(
        ExecStreamResourceQuantity &minQuantity,
        ExecStreamResourceQuantity &optQuantity);
    virtual void closeImpl();

    // REVIEW jvs 28-Nov-2005:  I don't think these should be either
    // public or inline.
    /**
     * Returns RID from btree tuple
     */
    inline LcsRid readRid()
    {
        return *reinterpret_cast<LcsRid const *> (btreeTupleData[0].pData);
    }
    
    /**
     * Returns cluster pageid from btree tuple
     */
    inline PageId readClusterPageId()
    {
        return *reinterpret_cast<PageId const *> (btreeTupleData[1].pData);
    }
};


FENNEL_END_NAMESPACE

#endif

// End LcsClusterAppendExecStream.h
