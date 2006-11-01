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

#ifndef Fennel_LbmSplicerExecStream_Included
#define Fennel_LbmSplicerExecStream_Included

#include "fennel/btree/BTreeWriter.h"
#include "fennel/exec/ConduitExecStream.h"
#include "fennel/exec/DynamicParam.h"
#include "fennel/ftrs/BTreeExecStream.h"
#include "fennel/tuple/TupleAccessor.h"
#include "fennel/tuple/TupleData.h"
#include "fennel/lucidera/bitmap/LbmEntry.h"
#include <boost/scoped_array.hpp>

FENNEL_BEGIN_NAMESPACE

struct LbmSplicerExecStreamParams :
    public BTreeExecStreamParams, public ConduitExecStreamParams
{
    DynamicParamId dynParamId;

    bool ignoreDuplicates;
};

class LbmSplicerExecStream : public BTreeExecStream, ConduitExecStream
{
    /**
     * Parameter id of dynamic parameter containing final row count
     */
    DynamicParamId dynParamId;
    
    /**
     * If true, ignore inputs containing rid values that are duplicates.  Only
     * works when the inputs are singletons.
     */
    bool ignoreDuplicates;

    /**
     * Maximum size of an LbmEntry
     */
    uint maxEntrySize;

    /**
     * Buffer for LbmEntry
     */
    boost::scoped_array<FixedBuffer> bitmapBuffer;

    /**
     * Buffer for merges on LbmEntry
     */
    boost::scoped_array<FixedBuffer> mergeBuffer;

    /**
     * Current bitmap entry under construction
     */
    SharedLbmEntry pCurrentEntry;

    /**
     * true if current entry is under construction
     */
    bool currEntry;

    /**
     * True if current bitmap entry under construction refers to an already
     * existing entry in the bitmap index
     */
    bool currExistingEntry;

    /**
     * Start rid of the current existing entry in the bitmap index.
     */
    LcsRid currBTreeStartRid;

    /**
     * True if table that the bitmap is being constructed on was empty to
     * begin with
     */
    bool emptyTable;

    /**
     * Writes btree index corresponding to bitmap
     */
    SharedBTreeWriter bTreeWriter;

    /**
     * True if output has been produced
     */
    bool isDone;

    /**
     * Input tuple
     */
    TupleData inputTuple;

    /**
     * Output tuple
     */
    TupleData outputTuple;

    /**
     * Output accessor
     */
    TupleAccessor *outputTupleAccessor;

    /**
     * Buffer holding the outputTuple to provide to the consumers
     */
    boost::scoped_array<FixedBuffer> outputTupleBuffer;

    /**
     * Number of rows loaded into bitmap index
     */
    RecordNum numRowsLoaded;

    /**
     * Tuple data for reading bitmaps from btree index
     */
    TupleData bTreeTupleData;
    TupleData tempBTreeTupleData;

    /**
     * Tuple descriptor representing bitmap tuple
     */
    TupleDescriptor bitmapTupleDesc;

    /**
     * Number of keys in the bitmap index, excluding the starting rid
     */
    uint nIdxKeys;

    /**
     * True if splicer needs to compute the row count result rather than
     * reading it from a dynamic parameter
     */
    bool computeRowCount;

    /**
     * Determines whether a bitmap entry already exists in the bitmap index,
     * based on the index key values.  If multiple entries have the same key
     * value, locates the last entry with that key value, i.e., the one with
     * the largest starting rid value.  Avoids index lookup if table was empty
     * at the start of the load.  Sets current bitmap entry either to the
     * existing btree entry (if it exists).  Otherwise, sets it to the bitmap
     * entry passed in.
     *
     * @param bitmapEntry tupledata corresponding to bitmap entry being checked
     *
     * @return true if entry already exists in bitmap index; always return
     * false if table was empty at the start of the load
     */
    bool existingEntry(TupleData const &bitmapEntry);

    /**
     * Searches the btree, looking for a bitmap entry
     *
     * @param bitmapEntry entry being searched
     * @param bTreeTupleData tuple data where the btree record will be
     * returned
     *
     * @return true if entry found in btree
     */
    bool findBTreeEntry(
        TupleData const &bitmapEntry, TupleData &bTreeTupleData);

    /**
     * Determines if there exists a better entry in the btree, corresponding
     * to the bitmap entry passed in, as compared to whatever is the current
     * bitmap entry.  If there is, the current entry is written to the btree,
     * and the current entry is set to the btree entry found.
     */
    void findBetterEntry(TupleData const &bitmapEntry);

    /**
     * Splices a bitmap entry to the current entry under construction.  If the
     * combined size of the two entries exceeds the max size of a bitmap entry,
     * the current entry will be inserted into the btree and the new entry
     * becomes the current entry under construction.
     *
     * @param bitmapEntry tupledata corresponding to the entry to be spliced
     */
    void spliceEntry(TupleData &bitmapEntry);

    /**
     * Inserts current bitmap entry under construction into the bitmap index
     */
    void insertBitmapEntry();

    /**
     * Creates a new current bitmap entry for construction
     *
     * @param bitmapEntry tupledata corresponding to initial value for bitmap
     * entry
     */
    void createNewBitmapEntry(TupleData const &bitmapEntry);

public:
    virtual void prepare(LbmSplicerExecStreamParams const &params);
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
    virtual void getResourceRequirements(
        ExecStreamResourceQuantity &minQuantity,
        ExecStreamResourceQuantity &optQuantity);
    virtual void closeImpl();
    virtual ExecStreamBufProvision getOutputBufProvision() const;
};

FENNEL_END_NAMESPACE

#endif

// End LbmSplicerExecStream.h
