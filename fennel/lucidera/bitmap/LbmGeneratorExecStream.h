/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2006-2006 LucidEra, Inc.
// Copyright (C) 2006-2006 The Eigenbase Project
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

#ifndef Fennel_LbmGeneratorExecStream_Included
#define Fennel_LbmGeneratorExecStream_Included

#include "fennel/tuple/TupleDataWithBuffer.h"
#include "fennel/exec/DynamicParam.h"
#include "fennel/lucidera/colstore/LcsRowScanBaseExecStream.h"
#include "fennel/lucidera/bitmap/LbmEntry.h"

FENNEL_BEGIN_NAMESPACE

enum LbmPendingProduceType {
    LBM_TABLEFLUSH_PENDING,
    LBM_ENTRYFLUSH_PENDING,
    LBM_NOFLUSH_PENDING
};

/**
 * Structure that associates a scratch buffer with each bitmap entry under
 * construction
 */
struct bitmapEntry
{
    LbmEntry bitmap;

    /**
     * pointer to buffer allocated to this entry; NULL if not allocated yet
     */
    PBuffer bufferPtr;

    /**
     * True if bitmap entry has an associated entry tuple
     */
    bool inuse;
};

struct LbmGeneratorExecStreamParams :
    public BTreeExecStreamParams, public LcsRowScanBaseExecStreamParams
{
    /**
     * Parameter id of dynamic parameter used to pass along number of rows
     * loaded
     */
    DynamicParamId dynParamId;
};

class LbmGeneratorExecStream : public BTreeExecStream, LcsRowScanBaseExecStream
{
    /**
     * Number of scratch pages to allocate for constructing bitmaps as
     * determined by the scheduler
     */
    uint maxNumScratchPages;

    /**
     * Dynamic parameter id used to pass along number of rows loaded
     */
    DynamicParamId dynParamId;

    /**
     * Size of a scratch page
     */
    uint scratchPageSize;

    /**
     * Size of a bitmap entry buffer
     */
    uint entrySize;

    /**
     * Max size of a bitmap entry
     */
    uint maxBitmapSize;

    /**
     * Min size of a bitmap entry
     */
    uint minBitmapSize;

    /**
     * Lock for scratch accessor
     */
    ClusterPageLock scratchLock;

    /**
     * Number of bitmap buffers per scratch page
     */
    uint nBufsPerPage;

    /**
     * Input tuple data
     */
    TupleData inputTuple;

    /**
     * Number of rows to load
     */
    RecordNum numRowsToLoad;

    /**
     * Running count of number of rows read
     */
    RecordNum rowCount;

    /**
     * Starting rid;
     */
    LcsRid startRid;

    /**
     * Current rid being loaded
     */
    LcsRid currRid;

    /**
     * Tuple data with buffer for the bitmap tuple
     */
    TupleDataWithBuffer bitmapTuple;

    /**
     * Tuple descriptor representing bitmap tuple
     */
    TupleDescriptor bitmapTupleDesc;

    /**
     * Pointer to generated tuple data.
     */
    TupleData outputTuple;

    /**
     * Number of keys in the bitmap index, excluding the starting rid
     */
    uint nIdxKeys;

    /**
     * True if current batch has been read
     */
    bool batchRead;

    /**
     * Current batch entry being processed
     */
    uint currBatch;

    /**
     * Keycodes read from a batch
     */
    std::vector<uint16_t> keyCodes;

    /**
     * Table of bitmap entries under construction
     */
    std::vector<bitmapEntry> bitmapTable;

    /**
     * Number of entries in the bitmap table
     */
    uint nBitmapEntries;

    /**
     * Index of buffer entry to flush
     */
    uint flushIdx;

    /**
     * Number of entries in the bitmap buffer table
     */
    uint nBitmapBuffers;

    /**
     * Number of scratch pages allocated
     */
    uint nScratchPagesAllocated;

    /**
     * Vector of pointers to scratch pages allocated
     */
    std::vector<PBuffer> scratchPages;

    /**
     * Produce of one or more output tuples pending: LBM_TABLEFLUSH_PENDING,
     * LBM_ENTRYFLUSH_PENDING, LBM_FINALFLUSH_PENDING, LBM_NOFLUSH_PENDING
     */
    LbmPendingProduceType producePending;

    /**
     * Index into bitmap table from which to start a pending table flush or
     * the single entry currently being flushed
     */
    uint flushStart;

    /**
     * If true, skip the initial read the next time generator is called
     * since we haven't finished processing the current rowid
     */
    bool skipRead;

    /**
     * Generates bitmaps for a single column index
     *
     * @param quantum quantum for stream
     *
     * @return either EXECRC_BUF_OVERFLOW or EXECRC_EOS
     */
    ExecStreamResult generateSingleKeyBitmaps(ExecStreamQuantum const &quantum);

    /**
     * Generates bitmaps for multi-column indexes
     *
     * @param quantum quantum for stream
     *
     * @return either EXECRC_BUF_OVERFLOW or EXECRC_EOS
     */
    ExecStreamResult generateMultiKeyBitmaps(ExecStreamQuantum const &quantum);

    /**
     * Creates a singleton bitmap entry and resets state to indicate that
     * the current row has been processed if successful
     */
    void createSingletonBitmapEntry();

    /**
     * Reads values from a single batch corresponding to a compressed batch
     * and generates bitmap entries
     *
     * @return true if successfully processed batch; false if overflow
     * occurred while writing out tuples
     */
    bool generateBitmaps();

    /**
     * Reads values from a single batch corresponding to a non-compressed
     * batch and generates singleton bitmap entries
     *
     * @return true if successfully processed batch; false if overflow
     * occurred while writing out tuples
     */
    bool generateSingletons();

    /**
     * Advances a single cluster and its corresponding column readers one rid
     * forward in the current batch
     *
     * @param pScan cluster reader to advance
     *
     * @return true if still within current batch; false if reached end of
     * batch
     */
    bool advanceReader(SharedLcsClusterReader &pScan);

    /**
     * Initializes bitmap table, increasing the size, as needed and assigns
     * buffers to bitmap entries.
     *
     * @param nEntries desired size of the table
     */
    void initBitmapTable(uint nEntries);

    /**
     * Sets rid in a bitmap tupledata.  Also initializes bitmap fields to
     * NULL
     *
     * @param bitmapTuple tupledata to be set
     *
     * @param pCurrRid pointer to the rid value that will be set in tupledata
     */
    void initRidAndBitmap(TupleData &bitmapTuple, LcsRid* pCurrRid);

    /**
     * Adds rid to a bitmap entry being constructing for a specified key.
     * Creates a new bitmap entry, as needed, and flushes out entries as buffer
     * space fills out.
     *
     * @param keycode keycode corresponding to the value of the rid being added;
     * always 0 in the case of a batch that is either non-compressed or has
     * more than 1 key
     *
     * @param initBitmap tupledata containing the initial bitmap entry value;
     * i.e., only the key value and rid are set
     *
     * @param rid rid to be added
     *
     * @return false if an error was encountered flushing out a bitmap entry
     */
    bool addRidToBitmap(uint keycode, TupleData &keyvalue, LcsRid rid);

    /**
     * Flushes out an existing buffer currently in use by another LbmEntry.
     * 
     * @return pointer to buffer if flush of entry was successful; otherwise
     * NULL if an overflow occurred when writing the entry to the output
     * stream
     */
    PBuffer flushBuffer();

    /**
     * Flushes out entire table of bitmap entries
     *
     * @param start index into bitmap table from which to start flushing
     * entries
     *
     * @return true if all entries successfully written to output stream
     */
    bool flushTable(uint start);

    /**
     * Flushes a single entry in the bitmap table and resets the LbmEntry
     * associated with the table entry
     *
     * @param keycode index of the bitmap entry to be flushed
     *
     * @return true if entries was successfully written to output stream
     */
    bool flushEntry(uint keycode);

public:
    explicit LbmGeneratorExecStream();
    virtual void prepare(LbmGeneratorExecStreamParams const &params);
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
    virtual void getResourceRequirements(
        ExecStreamResourceQuantity &minQuantity,
        ExecStreamResourceQuantity &optQuantity);
    virtual void setResourceAllocation(ExecStreamResourceQuantity &quantity);
    virtual void closeImpl();
};

FENNEL_END_NAMESPACE

#endif

// End LbmGeneratorExecStream.h
