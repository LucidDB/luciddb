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
#include "fennel/btree/BTreeWriter.h"
#include <boost/scoped_array.hpp>

FENNEL_BEGIN_CPPFILE("$Id$");

void LcsClusterAppendExecStream::prepare(
    LcsClusterAppendExecStreamParams const &params)
{
    BTreeExecStream::prepare(params);
    ConduitExecStream::prepare(params);

    tableColsTupleDesc = pInAccessor->getTupleDesc();
    numColumns = params.inputProj.size();

    // setup one tuple descriptor per cluster column
    colTupleDesc.reset(new TupleDescriptor[numColumns]);
    for (int i = 0; i < numColumns; i++) {
        colTupleDesc[i].push_back(tableColsTupleDesc[params.inputProj[i]]);
    }

    // setup descriptors, accessors and data to access only the columns
    // for this cluster, based on the input projection

    pInAccessor->bindProjection(params.inputProj);
    clusterColsTupleDesc.projectFrom(tableColsTupleDesc, params.inputProj);
    clusterColsTupleData.compute(clusterColsTupleDesc);

    overwrite = params.overwrite;

    // setup bufferLock to access temporary large page blocks

    scratchAccessor = params.scratchAccessor;
    bufferLock.accessSegment(scratchAccessor);

    // The output stream from the loader is either a single column representing
    // the number of rows loaded or two columns -- number of rows loaded and
    // starting rid value.  The latter applies when there are
    // downstream create indexes
    
    TupleDescriptor outputTupleDesc;

    outputTupleDesc = pOutAccessor->getTupleDesc();
    outputTuple.compute(outputTupleDesc);
    outputTuple[0].pData = (PConstBuffer) &numRowCompressed;
    if (outputTupleDesc.size() > 1) {
        outputTuple[1].pData = (PConstBuffer) &startRow;
    }

    outputTupleAccessor = & pOutAccessor->getScratchTupleAccessor();

    blockSize = treeDescriptor.segmentAccessor.pSegment->getUsablePageSize();
    
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
    minQuantity.nCachePages += (numColumns * 4) + 6;
    
    // TODO
    optQuantity = minQuantity;
}

void LcsClusterAppendExecStream::open(bool restart)
{
    BTreeExecStream::open(restart);
    ConduitExecStream::open(restart);

    if (!restart) {
        outputTupleBuffer.reset(
            new FixedBuffer[outputTupleAccessor->getMaxByteCount()]);        
    }

    init();
    isDone = false;
}

ExecStreamResult LcsClusterAppendExecStream::execute(
        ExecStreamQuantum const &quantum)
{
    return compress(quantum);
}

void LcsClusterAppendExecStream::closeImpl()
{
    BTreeExecStream::closeImpl();
    ConduitExecStream::closeImpl();
    outputTupleBuffer.reset();
    close();
}

void LcsClusterAppendExecStream::init()
{
    pIndexBlock = 0;
    firstRow = LcsRid(0);
    lastRow = LcsRid(0);
    startRow = LcsRid(0);
    rowCnt = 0;
    indexBlockDirty = false;
    arraysAlloced = false;
    compressCalled = false;
    numRowCompressed = 0;

    // The dynamic allocated memory in lcsBlockBuilder is allocated for every
    // LcsClusterAppendExecStream.open() and deallocated for every
    // LcsClusterAppendExecStream.closeImpl(). The dynamic memory is not reused
    // across calls(e.g. when issueing the same statement twice).
    lcsBlockBuilder = SharedLcsClusterNodeWriter(
        new LcsClusterNodeWriter(
            treeDescriptor, scratchAccessor, getSharedTraceTarget(),
            getTraceSourceName()));
    
    allocArrays();

    // get blocks from cache to use as temporary space and initialize arrays
    for (uint i = 0; i < numColumns; i++) {
        
        bufferLock.allocatePage();
        rowBlock[i] = bufferLock.getPage().getWritableData();
        bufferLock.unlock();
        
        bufferLock.allocatePage();
        hashBlock[i] = bufferLock.getPage().getWritableData();
        bufferLock.unlock();
        
        bufferLock.allocatePage();
        builderBlock[i] = bufferLock.getPage().getWritableData();
        bufferLock.unlock();
        
        hash[i].init(
            hashBlock[i], lcsBlockBuilder, colTupleDesc[i], i, blockSize);
    }

    nRowsMax = blockSize / sizeof(uint16_t);
}


ExecStreamResult LcsClusterAppendExecStream::compress(
    ExecStreamQuantum const &quantum)
{
    uint i, j, k;
    bool canFit = false;
    bool undoInsert= false;
    
    if (isDone) {
        // already returned final result
        pOutAccessor->markEOS();
        return EXECRC_EOS;
    }

    // no more input; produce final row count

    if (pInAccessor->getState() == EXECBUF_EOS) {

        // since we done adding rows to index write last batch
        // and block
        if (rowCnt) {
            // if rowCnt < 8 or a multiple of 8, force writeBatch to
            // treat this as the last batch
            if (rowCnt < 8 || (rowCnt % 8) == 0) {
                writeBatch(true);
            } else {
                writeBatch(false);
            }
        }

        // Write out the last block and then free up resources
        // rather than waiting until stream close. This will keep
        // resource usage window smaller and avoid interference with 
        // downstream processing such as writing to unclustered indexes.
        writeBlock();
        lcsBlockBuilder->close();
        close();
        
        // outputTuple was already initialized to point to numRowCompressed/
        // startRow in prepare()
        // Write a single outputTuple(numRowCompressed, [startRow])
        // and indicate OVERFLOW.

        outputTupleAccessor->marshal(outputTuple, outputTupleBuffer.get());
        pOutAccessor->provideBufferForConsumption(
            outputTupleBuffer.get(), 
            outputTupleBuffer.get() +
                outputTupleAccessor->getCurrentByteCount());

        isDone = true;
        return EXECRC_BUF_OVERFLOW;
    }

    for (i = 0; i < quantum.nTuplesMax; i++) {
        if (!pInAccessor->demandData()) {
            return EXECRC_BUF_UNDERFLOW;
        }

        // If this is the first time compress is called, then
        // start new block (for new table), or load last block
        // (of existing table).  We do this here rather than in
        // init() because for INSERT into T as SELECT * from T
        // we need to make sure that we extract all the data from
        // T before modifying the blocks there; use the boolean to
        // ensure that initialization of cluster page is only done
        // once

        if (!compressCalled) {
            compressCalled = true;
        
            // if the index exists, get last block written
        
            PLcsClusterNode pExistingIndexBlock;

            bool found = getLastBlock(pExistingIndexBlock);
            if (found) { 
                // indicate we are updating a leaf
                pIndexBlock = pExistingIndexBlock;
            
                // extract rows and values from last batch so we can 
                // add to it.
                loadExistingBlock();
            } else {
                // Start writing a new block
                startNewBlock();
                startRow = LcsRid(0);
            }
        }

        // if we have finished processing the previous row, unmarshal
        // the next cluster tuple and convert them into individual
        // tuples, one per cluster column
        if (!pInAccessor->isTupleConsumptionPending())
            pInAccessor->unmarshalProjectedTuple(clusterColsTupleData);

        // Go through each column value for current row and insert it.
        // If we run out of space then rollback all the columns that
        // I already inserted.
        undoInsert = false;

        for (j = 0; j < numColumns; j++) {

            hash[j].insert(
                clusterColsTupleData[j], &hashValOrd[j], &undoInsert);
            
            if (undoInsert) {
                
                // rollback cluster columns already inserted
                // j has not been incremented yet, so the condition should be
                //     k <= j
                for (k = 0; k <= j; k++) {
                    
                    hash[k].undoInsert(clusterColsTupleData[k]);
                }
                break;
            }
        }
        
        // Was there enough space to add this row?  Note that the Insert()
        // calls above accounted for the space needed by addValueOrdinal()
        // below, so we don't have to worry about addValueOrdinal() running
        // out of space
        if (!undoInsert) {
            canFit = true;
        } else {
            canFit = false;
        }
        
        if (canFit) {
                
            // Add the pointers from the batch to the data values
            for (j = 0; j < numColumns; j++) {
                addValueOrdinal(j, hashValOrd[j].getValOrd());
            }
            
            rowCnt++;
                
            // if reach max rows that can fit in row array then write batch
            if (isRowArrayFull()) {
                writeBatch(false);
            }
        } else {
                
            // since we can't fit anymore values write out current batch
            writeBatch(false);
                    
            // restart using last value retrieved from stream because it
            // could not fit in the batch; by continuing we can avoid
            // a goto to jump back to the top of this for loop at the
            // expense of a harmless increment of the quantum
            continue;
        }

        // only consume the tuple after we know the row can fit
        // on the current page
        pInAccessor->consumeTuple();
        numRowCompressed++;
    }

    return EXECRC_QUANTUM_EXPIRED;
}

void LcsClusterAppendExecStream::close() 
{
    if (scratchAccessor.pSegment) {
        scratchAccessor.pSegment->deallocatePageRange(
            NULL_PAGE_ID, NULL_PAGE_ID);
    }
    rowBlock.reset();
    hashBlock.reset();
    builderBlock.reset();

    hash.reset();
    hashValOrd.reset();
    tempBuf.reset();
    maxValueSize.reset();

    lcsBlockBuilder.reset();
}

void LcsClusterAppendExecStream::startNewBlock() 
{
    firstRow = lastRow;
    
    // Get a new cluster page from the btree segment
    pIndexBlock = lcsBlockBuilder->allocateClusterPage(firstRow);
    
    // Reset index block and block builder.
    lcsBlockBuilder->init(
        numColumns, reinterpret_cast<uint8_t *> (pIndexBlock),
        builderBlock.get(), blockSize);
    
    // reset Hashes
    for (uint i = 0; i < numColumns; i++) {
        hash[i].init(hashBlock[i], 
            lcsBlockBuilder, colTupleDesc[i], i, blockSize);
    }
    
    // reset row count
    // NOTE:  if the rowCnt is less than eight then we know we are carrying
    //        over rows from previous block because the count did not end
    //        on a boundary of 8
    if (rowCnt >= 8) {
        rowCnt = 0;
    }
    indexBlockDirty = false;
    
    // Start writing a new block.
    lcsBlockBuilder->openNew(firstRow);
}

bool LcsClusterAppendExecStream::getLastBlock(PLcsClusterNode &pBlock) 
{
    if (!lcsBlockBuilder->getLastClusterPageForWrite(pBlock, firstRow)) {
        return false;
    } else {
        return true;
    }
}

void LcsClusterAppendExecStream::loadExistingBlock() 
{
    boost::scoped_array<uint> numVals;      // number of values in block
    boost::scoped_array<uint16_t> lastValOff;
    boost::scoped_array<boost::scoped_array<FixedBuffer> > aLeftOverBufs;
                                            // array of buffers to hold
                                            // rolled back data for each
                                            // column
    uint anLeftOvers;                       // number of leftover rows for
                                            // each col; since the value is
                                            // same for every column, no need
                                            // for this to be an array
    boost::scoped_array<uint> aiFixedSize;  // how much space was used for
                                            // each column; should be
                                            // equal for each value in a column
    LcsHashValOrd vOrd;

    uint i, j;
    RecordNum startRowCnt;
    RecordNum nrows;
    
    lcsBlockBuilder->init(
        numColumns, reinterpret_cast<uint8_t *> (pIndexBlock),
        builderBlock.get(), blockSize);

    lastValOff.reset(new uint16_t[numColumns]);
    numVals.reset(new uint[numColumns]);
    
    // REVIEW jvs 28-Nov-2005:  A simpler approach to this whole problem
    // might be to pretend we were starting an entirely new block,
    // use an LcsClusterReader to read the old one logically and append
    // the old rows into the new block, and then carry on from there with
    // the new rows.
    
    // Append to an existing cluster page.  Set the last rowid based on
    // the first rowid and the number of rows currently on the page.
    // As rows are "rolled back", lastRow is decremented accordingly

    lcsBlockBuilder->openAppend(numVals.get(), lastValOff.get(), nrows);
    lastRow = firstRow + nrows;
    startRow = lastRow;
    
    // Setup structures to hold rolled back information
    aiFixedSize.reset(new uint[numColumns]);
    aLeftOverBufs.reset(new boost::scoped_array<FixedBuffer>[numColumns]);

    startRowCnt = rowCnt;
    
    // Rollback the final batch for each column
    // We need to rollback all
    // the batches before we can start the new batches because
    //  1) in openAppend() we adjust m_szLeft to not include space
    //     for numColumns * sizeof(RIBatch).  So if the
    //     block was full, then m_szLeft would be negative,
    //     since we decreased it by numColumns * sizeof(LcsBatch)
    //  2) the rollback code will add sizeof(LcsBatch) to szLeft
    //                      for each batch it rolls back
    //  3) the code to add values to a batch gets upset if
    //                      szLeft < 0
    for (i = 0; i < numColumns; i++) {
        
        //reset everytime through loop
        rowCnt = startRowCnt;
        lcsBlockBuilder->describeLastBatch(i, anLeftOvers, aiFixedSize[i]);
        
        // if we have left overs from the last batch (ie. batch did not end on
        // an 8 boundary), rollback and store in temporary mem
        // aLeftOverBufs[i]
        if (anLeftOvers > 0) {
            aLeftOverBufs[i].reset(
                new FixedBuffer[anLeftOvers * aiFixedSize[i]]);
            lcsBlockBuilder->rollBackLastBatch(i, aLeftOverBufs[i].get());
        }
    }
    
    // Start a new batch for each column.
    for (i = 0; i < numColumns; i++) {
    
        //reset everytime through loop
        rowCnt = startRowCnt;
    
        // Repopulate the hash table with the values already in the
        // data segment at the bottom of the block (because we didn't
        // roll back these values, we only roll back the pointers to
        // these values)
        hash[i].restore(numVals[i], lastValOff[i]);
    
        // if we had left overs from the last batch, start a new batch
        // NOTE: we are guaranteed to be able to add these values back
        // to the current block
        if (anLeftOvers > 0) {

            uint8_t *val;
            bool undoInsert = false;
        
            // There is a very small probability that when the hash is
            // restored, using the existing values in the block, that the
            // hash will be full and some of the left over values
            // can not be stored in the hash.  If this is true then clear
            // the hash.
            if (hash[i].isHashFull(anLeftOvers)) {
                hash[i].startNewBatch(anLeftOvers);
            }

            for (j = 0, val = aLeftOverBufs[i].get();
                j < anLeftOvers;
                j++, val += TupleDatum().getLcsLength(val))
            {
                hash[i].insert(val, &vOrd, &undoInsert);
                
                //If we have left overs they should fit in the block 
                assert(!undoInsert);
                addValueOrdinal(i, vOrd.getValOrd());
                rowCnt++;
            }
        }
    }
    
    lastRow -= rowCnt;
}

void LcsClusterAppendExecStream::addValueOrdinal(uint column, uint16_t vOrd) 
{
    uint16_t *rowWordArray = (uint16_t *) rowBlock[column];
    rowWordArray[rowCnt] = vOrd;
    
    // since we added a row mark block as dirty
    indexBlockDirty = true;
}

bool LcsClusterAppendExecStream::isRowArrayFull() 
{
    if (rowCnt >= nRowsMax)
        return true;
    else
        return false;
}

void LcsClusterAppendExecStream::writeBatch(bool lastBatch) 
{
    uint16_t *oVals;
    uint leftOvers;
    PBuffer val;
    LcsBatchMode mode;
    uint i, j;
    uint origRowCnt, count = 0;

    lastRow += rowCnt;

    for (origRowCnt = rowCnt, i = 0; i < numColumns; i++) {
        rowCnt = origRowCnt;
        
        // save max value size so we can read leftovers
        maxValueSize[i] = hash[i].getMaxValueSize();
        
        // Pick which compression mode to use (fixed, variable, or compressed)
        lcsBlockBuilder->pickCompressionMode(
            i, maxValueSize[i], rowCnt, &oVals, mode);
        leftOvers = rowCnt > 8 ? rowCnt % 8 : 0;
        
        // all batches must end on an eight boundary so we move
        // values over eight boundary to the next batch.
        // if there are leftOvers or if the there are less than
        // eight values in this batch allocate buffer to store
        // values to be written to next batch
        if (leftOvers) {
            tempBuf[i].reset(new FixedBuffer[leftOvers * maxValueSize[i]]);
            count = leftOvers;

        } else if (origRowCnt < 8) {
            tempBuf[i].reset(new FixedBuffer[origRowCnt * maxValueSize[i]]);
            count = origRowCnt;
        } else {
            // no values to write to next batch (ie on boundary of 8)
            tempBuf[i].reset();
        }
    
        // Write out the batch and collect the leftover rows in tempBuf
        if (LCS_FIXED == mode || LCS_VARIABLE == mode) {
            hash[i].prepareFixedOrVariableBatch(
                (PBuffer) rowBlock[i], rowCnt);
            lcsBlockBuilder->putFixedVarBatch(
                i, (uint16_t *) rowBlock[i], tempBuf[i].get());
            if (mode == LCS_FIXED) {
                hash[i].clearFixedEntries();
            }

        } else {

            uint16_t numVals;
            
            // write orderVals to oVals and remap val ords in row array
            hash[i].prepareCompressedBatch(
                (PBuffer) rowBlock[i], rowCnt, (uint16_t *) &numVals, oVals);
            lcsBlockBuilder->putCompressedBatch(
                i, (PBuffer) rowBlock[i], tempBuf[i].get());
        }
        
        // setup next batch
        rowCnt = 0;
        hash[i].startNewBatch(!lastBatch ? count : 0);
    }
    
    //compensate for left over and rolled back rows 
    if (!lastBatch) {
        lastRow -= count;
    }
    bool bStartNewBlock;
    bStartNewBlock = false;

    // If we couldn't even fit 8 values into the batch (and this is not the
    // final batch), then the block must be full.  putCompressedBatch()/
    // putFixedVarBatch() assumed that this was the last batch, so they wrote
    // out these rows in a small batch.  Roll back the entire batch (putting
    // rolled back results in tempBuf) and move to next block
    if (!lastBatch && origRowCnt < 8) {
    
        // rollback each batch
        for (i = 0; i < numColumns; i++) {
            lcsBlockBuilder->rollBackLastBatch(i, tempBuf[i].get());
        }
        bStartNewBlock = true;
    }
    
    // Should we move to a new block?  Move if
    //    (a) bStartNewBlock (we need to move just to write the current batch)
    // or (b) lcsBlockBuilder->isEndOfBlock() (there isn't room to even start
    // the next batch)
    if (bStartNewBlock || (!lastBatch && lcsBlockBuilder->isEndOfBlock())) {
        writeBlock();
        startNewBlock();
    }
    
    // Add leftOvers or rolled back values to new batch
    if (!lastBatch) {
        for (i = 0; i < numColumns; i++) {
            rowCnt = 0;
            for (j = 0, val = tempBuf[i].get(); j < count; j++) {
                LcsHashValOrd vOrd;
                bool undoInsert = false;
                
                hash[i].insert(val, &vOrd, &undoInsert);
                
                // If we have leftovers they should fit in the current block
                // (because we moved to a new block above, if it was necessary)
                assert(!undoInsert);
                addValueOrdinal(i, vOrd.getValOrd());
                rowCnt++;
                val += TupleDatum().getLcsLength(val);
            }
        }
    }

    for (i = 0; i < numColumns; i++) {
        if (tempBuf[i].get()) {
            tempBuf[i].reset();
        }
    }
}

void LcsClusterAppendExecStream::writeBlock() 
{
    if (indexBlockDirty) {
        
        // If the rowCnt is not zero, then the last batch was not on
        // a boundary of 8 so we need to write the last batch
        if (rowCnt) {
            writeBatch(true);
        
            // REVIEW jvs 28-Nov-2005:  it must be possible to eliminate
            // this circularity between writeBlock and writeBatch.
            
            // Handle corner case. writeBatch may have written this block 
            // to the btree
            if (!indexBlockDirty) {
                return;
            }
        }
    
        // Tell block builder we are done so it can wrap up writing to the
        // index block
        lcsBlockBuilder->endBlock();

        // Dump out the page contents to trace if appropriate

        indexBlockDirty = false;
    }
}

void LcsClusterAppendExecStream::allocArrays() 
{
    // allocate arrays only if they have not been allocated already
    if (arraysAlloced) {
        return;
    }
    arraysAlloced = true;
    
    // instantiate hashes
    hash.reset(new LcsHash[numColumns]);
    
    // allocate pointers for row, hash blocks, other arrays
    rowBlock.reset(new PBuffer[numColumns]);
    hashBlock.reset(new PBuffer[numColumns]);
    
    builderBlock.reset(new PBuffer[numColumns]);
    
    hashValOrd.reset(new LcsHashValOrd[numColumns]);
    tempBuf.reset(new boost::scoped_array<FixedBuffer>[numColumns]);
    maxValueSize.reset(new uint[numColumns]);
}

ExecStreamBufProvision
    LcsClusterAppendExecStream::getOutputBufProvision() const
{
    return BUFPROV_PRODUCER;
}

FENNEL_END_CPPFILE("$Id$");

// End LcsClusterAppendExecStream.cpp
