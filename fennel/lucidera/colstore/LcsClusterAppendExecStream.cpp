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

#include "fennel/common/CommonPreamble.h"
#include "fennel/lucidera/colstore/LcsClusterAppendExecStream.h"
#include "fennel/lucidera/colstore/LcsClusterNode.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/tuple/StandardTypeDescriptor.h"
#include "fennel/btree/BTreeWriter.h"
#include <boost/scoped_array.hpp>

FENNEL_BEGIN_CPPFILE("$Id$");

void LcsClusterAppendExecStream::prepare(
    LcsClusterAppendExecStreamParams const &params)
{
    BTreeExecStream::prepare(params);
    ConduitExecStream::prepare(params);

    // construct individual tuple descriptors, accessors, and data for
    // each column in the cluster

    clusterColsTupleDesc = pInAccessor->getTupleDesc();

    clusterColsTupleData.compute(clusterColsTupleDesc);
    m_numColumns = clusterColsTupleData.size();
    colTupleDesc.reset(new TupleDescriptor[m_numColumns]);
    colTupleData.reset(new TupleData[m_numColumns]);
    for (int i = 0; i < m_numColumns; i++) {
        colTupleDesc[i].push_back(clusterColsTupleDesc[i]);
        colTupleData[i].compute(colTupleDesc[i]);

        // There should only be one column for each colTupleData
        assert(colTupleData[i].size() == 1);
    }

    // setup bufferLock to access temporary large page blocks

    m_bOverwrite = params.overwrite;
    scratchAccessor = params.scratchAccessor;
    bufferLock.accessSegment(scratchAccessor);

    // the output stream from the loader is a single column representing
    // the number of rows loaded
    
    StandardTypeDescriptorFactory stdTypeFactory;
    TupleAttributeDescriptor attrDesc(
        stdTypeFactory.newDataType(STANDARD_TYPE_RECORDNUM));

    TupleDescriptor outputTupleDesc;

    outputTupleDesc.push_back(attrDesc);
    pOutAccessor->setTupleShape(outputTupleDesc);
    outputTuple.compute(outputTupleDesc);
    outputTuple[0].pData = (PConstBuffer) &numRowCompressed;
}
    
void LcsClusterAppendExecStream::getResourceRequirements(
    ExecStreamResourceQuantity &minQuantity,
    ExecStreamResourceQuantity &optQuantity)
{
    ConduitExecStream::getResourceRequirements(minQuantity,optQuantity);

    // REVIEW --
    // 4 pages per cluster column
    // - 1 for indexBlock
    // - 1 for rowBlock
    // - 1 for hash,
    // - 1 for value bank
    // 5 pages for btree and then 1 for cluster page
    minQuantity.nCachePages += (m_numColumns * 4) + 6;
    
    // TODO
    optQuantity = minQuantity;
}

void LcsClusterAppendExecStream::open(bool restart)
{
    BTreeExecStream::open(restart);
    ConduitExecStream::open(restart);

    Init();
}

ExecStreamResult LcsClusterAppendExecStream::execute(
        ExecStreamQuantum const &quantum)
{
    return Compress(quantum);
}

void LcsClusterAppendExecStream::closeImpl()
{
    BTreeExecStream::closeImpl();
    ConduitExecStream::closeImpl();
    Close();
}

LcsClusterAppendExecStream::LcsClusterAppendExecStream() 
{
    m_indexBlock = 0;
    m_firstRow = Rid(0);
    m_lastRow = Rid(0);
    m_startRow = Rid(0);
    m_bClosed = true;
    clusterDump = new LcsClusterDump(TRACE_FINE, getSharedTraceTarget(),
                                     getTraceSourceName());
}

LcsClusterAppendExecStream::~LcsClusterAppendExecStream() 
{
    // REVIEW jvs 28-Nov-2005:  these explicit resets aren't necessary
    colTupleDesc.reset();
    colTupleData.reset();
    
    // TODO jvs 28-Nov-2005:  use a scoped_ptr so this won't be necessary
    delete clusterDump;
}

void LcsClusterAppendExecStream::Init()
{
    m_rowCnt = 0;
    m_bClosed = false;
    m_indexBlockDirty = false;
    m_arraysAlloced = false;
    m_bCompressCalled = false;
    numRowCompressed = 0;

    m_blockSize = scratchAccessor.pSegment->getUsablePageSize();
   
#ifdef NOT_DONE_YET
    if (IsInOverwriteMode()) {
        
        // we are in overwrite mode, so we should delete the
        // existinging btree and create a new one.  However, other
        // subsystems may be using the existing btree (e.g. update t
        // set x = x + 1), so we should defer this deletion till
        // Close() time.  So, we save the cluster info till that time.
        m_pOldCluster = new(m_memPool) BBDDCluster(*m_pCluster);
    
        // The rest of the code should think that the cluster is new,
        // i.e. positioned on the invalid blockId
        m_pCluster->rootBlock = INVALID_BLOCKID;
    }
#endif

    // Initialize a btree writer as well as the lock for acquiring new
    // cluster pages and the tuple data representing the btree records

    m_btree = newWriter();
    clusterLock.accessSegment(treeDescriptor.segmentAccessor);
    btreeTupleData.compute(m_btree->getTupleDescriptor());

#ifdef NOT_DONE_YET
    //intialize lastrow and starting row
    if (IsInOverwriteMode()) {
        m_lastRow = 0;
        m_startRow = 0;
    }
    
    // determine if the column is long, if so dispatcher/lob code is
    // responsible for setting certain things, so we don't set them
    // in compressor
    m_bIsLong = false;
    for (uint16_t idx = 0; idx < m_numColumns; idx++) {
        if (BBSqlTDef(m_baseTdefs[idx]).IsLong()) {
            m_bIsLong = true;
        }
    }
#endif
    
    AllocArrays();
    
    // get blocks from cache to use as temporary space and initialize arrays
    for (uint16_t i = 0; i < m_numColumns; i++) {
        
        bufferLock.allocatePage();
        m_rowBlock[i] = bufferLock.getPage().getWritableData();
        bufferLock.unlock();
        
        bufferLock.allocatePage();
        m_hashBlock[i] = bufferLock.getPage().getWritableData();
        bufferLock.unlock();
        
        bufferLock.allocatePage();
        m_builderBlock[i] = bufferLock.getPage().getWritableData();
        bufferLock.unlock();
        
        m_hash[i].init(m_hashBlock[i], &m_riBlockBuilder,
            colTupleDesc[i], i, m_blockSize);
    }
}


ExecStreamResult LcsClusterAppendExecStream::Compress(
        ExecStreamQuantum const &quantum)
{
    uint16_t i, j, k;
    bool canFit = false;
    bool undoInsert= false;
    
    // If this is the first time compress is called, then
    // start new block (for new table), or load last block
    // (of existing table).  We do this here rather than in
    // Init() because for INSERT into T as SELECT * from T
    // we need to make sure that we extract all the data from
    // T before modifying the blocks there; use the boolean to
    // ensure that initialization of cluster page is only done
    // once

#ifdef NOT_DONE_YET
    if (!m_bIsLong && !m_bCompressCalled) {
#else
    if (!m_bCompressCalled) {
#endif

        m_bCompressCalled = true;
    
        // if the index exists, get last block written
    
        PLcsClusterNode pExistingIndexBlock;

        GetLastBlock(pExistingIndexBlock);
        if (pExistingIndexBlock) { 
            // indicate we are updating a leaf
            m_indexBlock = pExistingIndexBlock;
        
            // extract rows and values from last batch so we can 
            // add to it.
            LoadExistingBlock();
        } else {
            // Start writing a new block
            StartNewBlock();
            m_startRow = Rid(0);
        }
    }

    // no more input; produce final row count

    if (pInAccessor->getState() == EXECBUF_EOS) {

        // since we done adding rows to index write last batch
        // and block
        if (m_rowCnt) {
            // if rowCnt < 8 force writeBatch to write a batch
            if (m_rowCnt < 8) {
                WriteBatch(true);
            } else {
                WriteBatch(false);
            }
        }

        WriteBlock();
        clusterLock.unlock();
        
        // outputTuple was already initialized to point to numRowCompressed
        // in prepare()
        if (!pOutAccessor->produceTuple(outputTuple)) {
            return EXECRC_BUF_OVERFLOW;
        }
        pOutAccessor->markEOS();
        return EXECRC_EOS;
    }

    for (i = 0; i < quantum.nTuplesMax; i++) {
        if (!pInAccessor->demandData()) {
            return EXECRC_BUF_UNDERFLOW;
        }
   
        // unmarshal the cluster tuple and convert them into individual
        // tuples, one per cluster column
        pInAccessor->unmarshalTuple(clusterColsTupleData);

StartCompressed:
        // Go through each column value for current row and insert it.
        // If we run out of space then rollback all the columns that
        // I already inserted.
        undoInsert = false;

        for (j = 0; j < m_numColumns; j++) {

            // REVIEW jvs 28-Nov-2005:  We could get rid of the
            // single-column TupleData copies if the hash took
            // a TupleDatum instead (it's just going to assert
            // that there's only one).
            
            // Use one column from the cluster tuple to initialize a
            // colTupleData which contains only one column
            (colTupleData[j])[0] = clusterColsTupleData[j];
            
            m_hash[j].insert(colTupleData[j], &m_vOrd[j], &undoInsert);
            
            if (undoInsert) {
                
                // rollback cluster columns already inserted
                // j has not been incremented yet, so the condition should be
                //     k <= j
                for (k = 0; k <= j; k++) {
                    
                    m_hash[k].undoInsert(colTupleData[k]);
                }
                break;
            }
        }
        
        // REVIEW jvs 28-Nov-2005:  The if test below is
        // a redundant conjunction, right?  The only way we get through
        // with j == m_numColumns is when undoInsert was never set.
        
        // Was there enough space to add this row?  Note that the Insert()
        // calls above accounted for the space needed by AddRow() below,
        // so we don't have to worry about AddRow() running out of space
        if (!undoInsert && (j == m_numColumns)) {
            canFit = true;
        } else {
            canFit = false;
        }
        
        if (canFit) {
                
            // Add the pointers from the batch to the data values
            for (j = 0; j < m_numColumns; j++) {
                AddRow(j, m_vOrd[j].getValOrd());
            }
            
            m_rowCnt++;
                
            // if reach max rows that can fit in row array then write batch
            if (IsRowArrayFull()) {
                WriteBatch(false);
            }
        } else {
                
            // since we can't fit anymore values write out current batch
            WriteBatch(false);
                    
            // NOTE jvs 28-Nov-2005:  It is possible to change
            // this goto to a continue, but only by wrapping an
            // pInAccessor->isTupleConsumptionPending test
            // around the unmarshalTuple at the top of the loop
            // (quantum increment off-by-one isn't important).
            
            // restart using last value retrieved from stream because it
            // could not fit in the batch
            goto StartCompressed;
        }

        // only consume the tuple after we know the row can fit
        // on the current page
        pInAccessor->consumeTuple();
        numRowCompressed++;
    }

    return EXECRC_QUANTUM_EXPIRED;
}

void LcsClusterAppendExecStream::Close() 
{
    if (m_bClosed) {
        return;
    }
    m_bClosed = true;
    
    m_btree.reset();

    // free cache blocks and the arrays pointing to them
    scratchAccessor.pSegment->deallocatePageRange(NULL_PAGE_ID, NULL_PAGE_ID);
    m_rowBlock.reset();
    m_hashBlock.reset();
    m_builderBlock.reset();

    m_hash.reset();
    m_vOrd.reset();
    m_buf.reset();
    m_maxValueSize.reset();

    // close block-builders
    m_riBlockBuilder.Close();
    
#ifdef NOT_DONE_YET
    // we have to delete the old BTree if we were in overwrite mode
    if (IsInOverwriteMode()) {
        BTTree btreeToDie;
        BB_ASSERT(m_pOldCluster, rc, Exit);
        rc =
            btreeToDie.Init(m_memPool, m_table, m_pOldCluster,
                            m_pOldCluster->rootBlock, 1,
                            (BBBaseTDef *) & RidType, m_pCx, BBBLOCK_COLUMN,
                            BBBLOCK_COLUMN_NODE, BBBLOCK_COLUMN_LEAF, 4,
                            true);
        BB_DROP(rc, Exit);
        rc =
            btreeToDie.StartMod(m_pFx, PYALLOC_RI_NODE, PYALLOC_RI_LEAF,
                                GetDatabase()->GetBlockSize());
        BB_DROP(rc, Exit);
        rc = btreeToDie.DeleteTree();
        BB_DROP(rc, Exit);
    }
#endif
}

void LcsClusterAppendExecStream::StartNewBlock() 
{
    m_firstRow = m_lastRow;
    
    // Get a new cluster page from the btree segment
    clusterPageId = clusterLock.allocatePage();
    m_indexBlock = &(clusterLock.getNodeForWrite());
    
    // Insert into the btree a tuple composed of the first rid on the
    // page followed by the cluster pageid

    btreeTupleData[0].pData = reinterpret_cast<uint8_t *> (&m_firstRow);
    btreeTupleData[1].pData = reinterpret_cast<uint8_t *> (&clusterPageId);
    m_btree->insertTupleData(btreeTupleData, DUP_FAIL);
    
    // Reset index block and block builder.
    m_riBlockBuilder.Init(scratchAccessor, m_numColumns,
                          reinterpret_cast<uint8_t *> (m_indexBlock),
                          m_builderBlock.get(), m_blockSize);
    
    // reset Hashes
    for (uint16_t i = 0; i < m_numColumns; i++) {
        m_hash[i].init(m_hashBlock[i], 
            &m_riBlockBuilder, colTupleDesc[i], i, m_blockSize);
    }
    
    // reset row count
    // NOTE:  if the rowCnt is less than eight then we know we are carrying
    //        over rows from previous block because the count did not end
    //        on a boundary of 8
    if (m_rowCnt >= 8) {
        m_rowCnt = 0;
    }
    m_indexBlockDirty = false;
    
    // Start writing a new block.
    m_riBlockBuilder.OpenNew(m_firstRow);
}

void LcsClusterAppendExecStream::GetLastBlock(PLcsClusterNode &pBlock) 
{
    pBlock = 0;
    
    if (!m_btree->searchLast()) {
        // empty btree
        return;
    }

    // read the last cluster page using the last key in the btree

    m_btree->getTupleAccessorForRead().unmarshal(btreeTupleData);
    clusterPageId = readClusterPageId();
    clusterLock.lockExclusive(clusterPageId);
    pBlock = &(clusterLock.getNodeForWrite());
    m_firstRow = readRid();
}

void LcsClusterAppendExecStream::LoadExistingBlock() 
{
    boost::scoped_array<uint16_t> numVals;      // number of values in block
    boost::scoped_array<uint16_t> lastValOff;
    // TODO jvs 28-Nov-2005:  use automatic mem mgmt for the buffers too
    // to avoid leak in case of excn
    boost::scoped_array<PBuffer> aLeftOverBufs;
                                            // array of buffers to hold
                                            // rolled back data for each
                                            // column
    
    // REVIEW jvs 28-Nov-2005:  shouldn't this be the same for each column?
    boost::scoped_array<uint> anLeftOvers;  // number of leftover rows for
                                            // each col
    boost::scoped_array<uint16_t> aiFixedSize;  // how much space was used for
                                            // each column; should be
                                            // equal for each column
    LcsHashValOrd vOrd;

    uint16_t i, j;
    RecordNum startRowCnt;
    RecordNum nrows;
    
    m_riBlockBuilder.Init(scratchAccessor, m_numColumns,
                          reinterpret_cast<uint8_t *> (m_indexBlock),
                          m_builderBlock.get(), m_blockSize);

    lastValOff.reset(new uint16_t[m_numColumns]);
    numVals.reset(new uint16_t[m_numColumns]);
    
    // REVIEW jvs 28-Nov-2005:  A simpler approach to this whole problem
    // might be to pretend we were starting an entirely new block,
    // use an LcsClusterReader to read the old one logically and append
    // the old rows into the new block, and then carry on from there with
    // the new rows.
    
    // Append to an existing cluster page.  Set the last rowid based on
    // the first rowid and the number of rows currently on the page.
    // As rows are "rolled back", m_lastRow is decremented accordingly

    m_riBlockBuilder.OpenAppend(numVals.get(), lastValOff.get(), nrows);
    m_lastRow = m_firstRow + nrows;
    m_startRow = m_lastRow;
    
    if (isTracingLevel(TRACE_FINE)) {
        FENNEL_TRACE(TRACE_FINE,
                     "Calling ClusterDump from LoadExistingBlock");
        clusterDump->dump(opaqueToInt(clusterPageId),
                          (PBuffer) m_indexBlock, m_blockSize);
    }

    // Setup structures to hold rolled back information
    anLeftOvers.reset(new uint[m_numColumns]);
    aiFixedSize.reset(new uint16_t[m_numColumns]);
    aLeftOverBufs.reset(new PBuffer[m_numColumns]);

    startRowCnt = m_rowCnt;
    
    // Rollback the final batch for each column
    // We need to rollback all
    // the batches before we can start the new batches because
    //  1) in OpenAppend() we adjust m_szLeft to not include space
    //     for m_numColumns * sizeof(RIBatch).  So if the
    //     block was full, then m_szLeft would be negative,
    //     since we decreased it by m_numColumns * sizeof(RIBatch)
    //  2) the rollback code will add sizeof(RIBatch) to m_szLeft
    //                      for each batch it rolls back
    //  3) the code to add values to a batch gets upset if
    //                      m_szLeft < 0
    for (i = 0; i < m_numColumns; i++) {
        
        //reset everytime through loop
        m_rowCnt = startRowCnt;
        m_riBlockBuilder.DescribeLastBatch(i, anLeftOvers[i], aiFixedSize[i]);
        
        // if we have left overs from the last batch (ie. batch did not end on
        // an 8 boundary), rollback and store in temporary mem
        // aLeftOverBufs[i]
        if (anLeftOvers[i] > 0) {
            aLeftOverBufs[i] = new FixedBuffer[anLeftOvers[i] * aiFixedSize[i]];
            m_riBlockBuilder.RollBackLastBatch(i, aLeftOverBufs[i]);
        }
    }
    
    // Start a new batch for each column.
    for (i = 0; i < m_numColumns; i++) {
    
        //reset everytime through loop
        m_rowCnt = startRowCnt;
    
        // Repopulate the hash table with the values already in the
        // data segment at the bottom of the block (because we didn't
        // roll back these values, we only roll back the pointers to
        // these values)
        m_hash[i].restore(numVals[i], lastValOff[i]);
    
        // if we had left overs from the last batch, start a new batch
        // NOTE: we are guaranteed to be able to add these values back
        // to the current block
        if (anLeftOvers[i] > 0) {

            uint8_t *val;
            bool undoInsert = false;
        
            // There is a very small probability that when the hash is
            // restored, using the existing values in the block, that the
            // hash will be full and some of the left over values
            // can not be stored in the hash.  If this is true then clear
            // the hash.
            if (m_hash[i].isHashFull(anLeftOvers[i])) {
                m_hash[i].startNewBatch(anLeftOvers[i]);
            }

            for (j = 0, val = aLeftOverBufs[i];
                 j < anLeftOvers[i];
                 j++, val += aiFixedSize[i])
            {
                m_hash[i].insert(val, &vOrd, &undoInsert);
                
                //If we have left overs they should fit in the block 
                assert(!undoInsert);
                AddRow(i, vOrd.getValOrd());
                m_rowCnt++;
            }
        }
    }
    
    m_lastRow -= m_rowCnt;

    // NOTE jvs 28-Nov-2005:  these explicit resets aren't necessary
    lastValOff.reset();
    numVals.reset();
    aiFixedSize.reset();
    // TODO jvs 28-Nov-2005:  see note above on scoped_array<FixedBuffer>
    for (i = 0; i < m_numColumns; i++) {
        if (anLeftOvers[i] > 0) {
            delete[] aLeftOverBufs[i];
        }
    }
    aLeftOverBufs.reset();
    anLeftOvers.reset();
}

void LcsClusterAppendExecStream::AddRow(uint16_t column, uint16_t vOrd) 
{
    uint16_t * rowWordArray = (uint16_t *) m_rowBlock[column];
    rowWordArray[m_rowCnt] = vOrd;
    
    // since we added a row mark block as dirty
    m_indexBlockDirty = true;
}

bool LcsClusterAppendExecStream::IsRowArrayFull() 
{
    // REVIEW jvs 28-Nov-2005:  can't we just precompute an nRowsMax
    // once and compare against it here?  This pointer arithmetic
    // shouldn't be necessary.  The state should be identical for
    // all columns since these are uint16_t codes, correct?
    
    for (uint i = 0; i < m_numColumns; i++) {
        uint16_t *rowWordArray = (uint16_t *) m_rowBlock[i];
        // row value ordinals when valCnt >= 256
        if ((PBuffer) &rowWordArray[m_rowCnt] >=
            (PBuffer) m_rowBlock[i] + (m_blockSize & 0xfffffff0))
        {
            return true;
        }
    }
    return false;
}

void LcsClusterAppendExecStream::WriteBatch(bool lastBatch) 
{
    uint16_t *oVals;
    uint leftOvers;
    PBuffer val;
    uint16_t mode;
    uint16_t i, j;
    uint origRowCnt, count = 0;

    m_lastRow += m_rowCnt;

    for (origRowCnt = m_rowCnt, i = 0; i < m_numColumns; i++) {
        m_rowCnt = origRowCnt;
        
        // save max value size so we can read leftovers
        m_maxValueSize[i] = m_hash[i].getMaxValueSize();
        
        // Pick which compression mode to use (fixed, variable, or compressed)
        m_riBlockBuilder.PickCompressionMode(i, m_maxValueSize[i],
                                             (uint) m_rowCnt, &oVals, mode);
        leftOvers = m_rowCnt > 8 ? m_rowCnt % 8 : 0;
        
        // all batches must end on an eight boundary so we move
        // values over eight boundary to the next batch.
        // if there are leftOvers or if the there are less than
        // eight values in this batch allocate buffer to store
        // values to be written to next batch
        if (leftOvers) {
            m_buf[i] = new uint8_t[leftOvers * m_maxValueSize[i]];
            count = leftOvers;

        } else if (origRowCnt < 8) {
            m_buf[i] = new uint8_t[origRowCnt * m_maxValueSize[i]];
            count = origRowCnt;
        } else {
            // no values to write to next batch (ie on boundary of 8)
            m_buf[i] = 0;
        }
    
        // Write out the batch and collect the leftover rows in m_buf
        if (LCS_FIXED == mode || LCS_VARIABLE == mode) {
            m_hash[i].prepareFixedOrVariableBatch((PBuffer) m_rowBlock[i],
                                                  m_rowCnt);
            m_riBlockBuilder.PutFixedVarBatch(i, (uint16_t *) m_rowBlock[i],
                                              m_buf[i]);
            if (mode == LCS_FIXED) {
                m_hash[i].clearFixedEntries();
            }

        } else {

            uint16_t numVals;
            
            // write orderVals to oVals and remap val ords in row array
            m_hash[i].prepareCompressedBatch((PBuffer) m_rowBlock[i],
                                                 m_rowCnt, &numVals, oVals);
            m_riBlockBuilder.PutCompressedBatch(i, (PBuffer) m_rowBlock[i],
                                                m_buf[i]);
        }
        
        // setup next batch
        m_rowCnt = 0;
        m_hash[i].startNewBatch(!lastBatch ? count : 0);
    }
    
    //compensate for left over and rolled back rows 
    if (!lastBatch) {
        m_lastRow -= count;
    }
    bool bStartNewBlock;
    bStartNewBlock = false;

    // REVIEW jvs 28-Nov-2005:  Comment below refers to PutBatch, but
    // that method no longer exists.
    
    // If we couldn't even fit 8 values into the batch (and this is not the
    // final batch), then the block must be full.  PutBatch() assumed that
    // this was the last batch, so it wrote out these rows in a small batch.
    // Roll back the entire batch (putting rolled back results in m_buf) and
    // move to next block
    if (!lastBatch && origRowCnt < 8) {
    
        // rollback each batch
        for (i = 0; i < m_numColumns; i++) {
            m_riBlockBuilder.RollBackLastBatch(i, m_buf[i]);
        }
        bStartNewBlock = true;
    }
    
    // Should we move to a new block?  Move if
    //    (a) bStartNewBlock (we need to move just to write the current batch)
    // or (b) m_riBlockBuilder.IsEndOfBlock() (there isn't room to even start
    // the next batch)
    if (bStartNewBlock || (!lastBatch && m_riBlockBuilder.IsEndOfBlock())) {
        WriteBlock();
        StartNewBlock();
    }
    
    // Add leftOvers or rolled back values to new batch
    if (!lastBatch) {
        for (i = 0; i < m_numColumns; i++) {
            m_rowCnt = 0;
            for (j = 0, val = m_buf[i]; j < count; j++) {
                LcsHashValOrd vOrd;
                bool undoInsert = false;
                
                m_hash[i].insert(val, &vOrd, &undoInsert);
                
                // If we have leftovers they should fit in the current block
                // (because we moved to a new block above, if it was necessary)
                assert(!undoInsert);
                AddRow(i, vOrd.getValOrd());
                m_rowCnt++;
                val += m_maxValueSize[i];
            }
        }
    }

    // TODO jvs 28-Nov-2005:  use automatic mem mgmt for the entries in
    // m_buf to avoid leak in case of excn
    for (i = 0; i < m_numColumns; i++) {
        if (m_buf[i]) {
            delete[] m_buf[i];
        }
    }
}

void LcsClusterAppendExecStream::WriteBlock() 
{
    if (m_indexBlockDirty) {
        
        // If the rowCnt is not zero, then the last batch was not on
        // a boundary of 8 so we need to write the last batch
        if (m_rowCnt) {
            WriteBatch(true);
        
            // REVIEW jvs 28-Nov-2005:  it must be possible to eliminate
            // this circularity between WriteBlock and WriteBatch.
            
            // Handle corner case. WriteBatch may have written this block 
            // to the btree
            if (!m_indexBlockDirty) {
                return;
            }
        }
    
        // Tell block builder we are done so it can wrap up writing to the
        // index block
        m_riBlockBuilder.EndBlock();

        // Dump out the page contents to trace if appropriate

        if (isTracingLevel(TRACE_FINE)) {
            FENNEL_TRACE(TRACE_FINE, "Calling ClusterDump from WriteBlock");
            clusterDump->dump(opaqueToInt(clusterPageId),
                              (PBuffer) m_indexBlock, m_blockSize);
        }

        m_indexBlockDirty = false;
    }
}

void LcsClusterAppendExecStream::AllocArrays() 
{
    // allocate arrays only if they have not been allocated already
    if (m_arraysAlloced) {
        return;
    }
    m_arraysAlloced = true;
    
    // instantiate hashes
    m_hash.reset(new LcsHash[m_numColumns]);
    
    // allocate pointers for row, hash blocks, other arrays
    m_rowBlock.reset(new PBuffer[m_numColumns]);
    m_hashBlock.reset(new PBuffer[m_numColumns]);
    
    m_builderBlock.reset(new PBuffer[m_numColumns]);
    
    m_vOrd.reset(new LcsHashValOrd[m_numColumns]);
    m_buf.reset(new PBuffer[m_numColumns]);
    m_maxValueSize.reset(new uint[m_numColumns]);
}

FENNEL_END_CPPFILE("$Id$");

// End LcsClusterAppendExecStream.cpp
