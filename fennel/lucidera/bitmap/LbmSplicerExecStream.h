/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2006-2007 LucidEra, Inc.
// Copyright (C) 2006-2007 The Eigenbase Project
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
#include "fennel/ftrs/BTreeExecStream.h"
#include "fennel/exec/DiffluenceExecStream.h"
#include "fennel/exec/DynamicParam.h"
#include "fennel/tuple/TupleAccessor.h"
#include "fennel/tuple/TupleData.h"
#include "fennel/tuple/TupleDataWithBuffer.h"
#include "fennel/lucidera/bitmap/LbmEntry.h"
#include "fennel/lucidera/bitmap/LbmRidReader.h"
#include <boost/scoped_array.hpp>

FENNEL_BEGIN_NAMESPACE

struct LbmSplicerExecStreamParams : public DiffluenceExecStreamParams
{
    std::vector<BTreeParams> bTreeParams;

    DynamicParamId insertRowCountParamId;

    DynamicParamId writeRowCountParamId;
};

/**
 * LbmSplicerExecStream takes as input a stream of bitmap entries.  If
 * possible, it splices the entries into larger entries before writing them
 * into a btree.  The bitmap entries from the input can correspond to new
 * entries, or they may need to be spliced into existing entries in the btree.
 *
 * <p>As output, LbmSplicerExecStream writes out a count corresponding to the
 * number of new row values inserted into the btree.  This value is either
 * directly computed by LbmSplicerExecStream (provided the bitmap entry inputs
 * all correspond to singleton rids), or it is passed into LbmSplicerExecStream
 * via a dynamic parameter.
 *
 * <p>Optionally, LbmSplicerExecStream may also write out the row count into
 * a dynamic parameter that is read downstream by another exec stream.
 *
 * <p>If the index being updated is a primary key or a unique key, then
 * multiple entries mapping to the same key value are rejected. When a new
 * key value is encountered, undeleted entries are counted for that key value.
 * If an entry already exists, then all inputs with the key value are
 * rejected. Otherwise, if no entry exists, a single input RID is accepted as
 * the valid insert (or update). (We deviate from the SQL standard.) In order
 * to count existing entries, we read from the deletion index. Violations are
 * written to a second output. Unique keys differ from primary keys in that
 * multiple entries are allowed for null keys.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
class LbmSplicerExecStream : public DiffluenceExecStream
{
    /**
     * Scratch accessor
     */
    SegmentAccessor scratchAccessor;

    /**
     * Descriptor corresponding to the btree that splicer writes to
     */
    BTreeDescriptor writeBTreeDesc;

    /**
     * Descriptor corresponding to the deletion index btree
     */
    BTreeDescriptor deletionBTreeDesc;

    /**
     * Parameter id of dynamic parameter containing final insert row count
     */
    DynamicParamId insertRowCountParamId;
    
    /**
     * Parameter id of the dynamic parameter used to write the row count
     * affected by this stream that will be read downstream
     */
    DynamicParamId writeRowCountParamId;

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
     * True if we need to determine emptyTable. We can't do this in init()
     * because upstream ExecStream's may insert into the index being updated.
     * An example of early update is when the deletion phase of a merge
     * statement appends the deletion index. The same index may be later
     * is appended for merge violations. 
     */
    bool emptyTableUnknown;

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
     * Whether btree writer position was moved for unique constraint
     * validation. If currExistingEntry is true, then the btree writer is
     * expected to stay positioned at the existing entry. However, constraint
     * validation may run side searches and can reposition the btree writer.
     */
    bool bTreeWriterMoved;

    /**
     * True if output has been produced
     */
    bool isDone;

    /**
     * Input tuple
     */
    TupleData inputTuple;

    /**
     * Input tuple corresponding to singleton entries.  Used only in the
     * case where the rowcount needs to be computed by splicer.
     */
    TupleData singletonTuple;

    /**
     * Output tuple containing rowcount
     */
    TupleData outputTuple;

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
     * Whether the index being updated is a unique key
     */
    bool uniqueKey;

    /**
     * Whether an input tuple is currently being validated
     */
    bool currValidation;

    /**
     * Whether any tuple has been validated yet
     */
    bool firstValidation;

    /**
     * The current unique key value being validated
     */
    TupleDataWithBuffer currUniqueKey;

    /**
     * Number of rows for the current key value (after taking into account
     * the deletion index)
     */
    uint nKeyRows;

    /**
     * Reads rids from the deletion index
     */
    LbmDeletionIndexReader deletionReader;

    /**
     * Current tuple for deletion index rid reader
     */
    TupleData deletionTuple;

    /**
     * Reads rids from an input tuple
     */
    LbmTupleRidReader inputRidReader;

    /**
     * True if no RID has been accepted as the update/insert for the
     * current key value
     */
    bool nullUpsertRid;

    /**
     * If a RID has been accepted as the update/insert for the current
     * key value, this contains the value of the accepted RID
     */
    LcsRid upsertRid;

    /**
     * Accessor for the violation output stream
     */
    SharedExecStreamBufAccessor violationAccessor;

    /**
     * Violation data tuple
     */
    TupleData violationTuple;

    /**
     * Error message for constraint violations
     */
    std::string errorMsg;

    /**
     * TupleDescriptor for error records
     */
    TupleDescriptor errorDesc;

    /**
     * TupleData used to build error records
     */
    TupleData errorTuple;

    /**
     * Determines, and remembers whether the bitmap being updated is empty.
     * Should only be called when the bitmap is actually ready for access.
     */
    bool isEmpty();

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
     * Searches the btree, looking for the btree record matching the index
     * keys and startRid of a specified bitmap entry
     *
     * @param bitmapEntry entry for which we are trying to match
     * @param bTreeTupleData tuple data where the btree record will be
     * returned if a matching entry is found; if a non-match is found, the
     * tuple data contains the greatest lower bound btree entry found
     *
     * @return true if a matching entry is found in the btree; false otherwise;
     * (in this case, bTreeWriter is positioned at the location of the greatest
     * lower bound btree entry corresponding to the bitmap entry)
     */
    bool findMatchingBTreeEntry(
        TupleData const &bitmapEntry,
        TupleData &bTreeTupleData);

    /**
     * Searches the btree, looking for the first btree record which overlaps
     * with a given bitmap entry (or the insertion point for a new one if no
     * existing match exists).
     *
     * @param bitmapEntry entry for which to find an overlap match
     * @param bTreeTupleData tuple data where the btree record will be returned
     * if a matching entry is found (otherwise invalid data references
     * are returned here after search)
     *
     * @return true if entry found in btree; false if no entry found
     * (in this case, bTreeWriter is positioned to correct location
     * for inserting new entry)
     */
    bool findBTreeEntry(
        TupleData const &bitmapEntry,
        TupleData &bTreeTupleData);

    /**
     * Determines whether a rid intersects the rid range spanned by a bitmap
     * entry.  If the bitmap is a singleton, then the byte occupied by the
     * singleton rid is used in determining intersection.
     *
     * @param rid the rid
     * @param bitmapTupleData tupleData representing a bitmap entry
     * @param firstByte if true, only consider overlap in the first byte of
     * the bitmap
     *
     * @return true if the rid overlaps the bitmap entry
     */
    bool ridOverlaps(LcsRid rid, TupleData &bitmapTupleData, bool firstByte);

    /**
     * Determines if there exists a better entry in the btree, corresponding
     * to the bitmap entry passed in, as compared to whatever is the current
     * bitmap entry.  If there is, the current entry is written to the btree,
     * and the current entry is set to the btree entry found.
     *
     * @param bitmapEntry tupleData corresponding to the entry to be spliced
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

    /**
     * Efficient update or insert of tuples into unique indexes
     *
     * @param bitmapEntry tupledata corresponding to the entry to be upserted
     */
    void upsertSingleton(TupleData const &bitmapEntry);

    /**
     * Reads input tuples and filters out Rids that cause key violations.
     * Key violations are posted to an ErrorTarget for logging while Rids
     * are output to a violation stream for deletion. This method completes
     * when a valid tuple has been generated. Sets the inputTuple field.
     *
     * @return EXECRC_YIELD on success, or other status codes for input
     * stream underflow, end of input stream, or violation stream overflow
     */
    ExecStreamResult getValidatedTuple();

    /**
     * Determines whether a tuple key must be unique. A tuple key must be
     * unique if the index has a uniqueness constraint and the tuple key
     * does not contain any nulls.
     *
     * @param tuple input tuple to be checked for uniqueness requirments
     *
     * @return true if the tuple key must be unique
     */
    bool uniqueRequired(const TupleData &tuple);

    /**
     * Counts the number of rows in the index with a particular key value, 
     * prior to modification. This count factors in the deletion index.
     *
     * @param tuple tupledata containing the key value to search the index for
     *
     * @return number of rows with a key value, less the deleted rows
     */
    uint countKeyRows(const TupleData &tuple);

    /**
     * For key values that can only have one valid rid, remembers the value
     * of a single rid that was chosen to be for insert or update.
     *
     * @param rid a rid to be inserted into the index
     */
    inline void setUpsertRid(LcsRid rid);

    /**
     * For key values that can only have one valid rid, gets a pointer to a
     * rid to be inserted into the index.
     *
     * @return a pointer to a rid to be inserted, or NULL if none was set
     */
    inline const LcsRid *getUpsertRidPtr() const;

    /**
     * Generates an error record and posts it to an ErrorTarget
     *
     * @param input the input tuple causing the error
     *
     * @param violation the violation tuple created for the error
     */
    void postViolation(const TupleData &input, const TupleData &violation);

public:
    virtual void prepare(LbmSplicerExecStreamParams const &params);
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
    virtual void getResourceRequirements(
        ExecStreamResourceQuantity &minQuantity,
        ExecStreamResourceQuantity &optQuantity);
    virtual void closeImpl();
};

/**************************************************************
  Definitions of inline methods for class LbmSplicerExecStream
***************************************************************/

inline void LbmSplicerExecStream::setUpsertRid(LcsRid rid)
{
    nullUpsertRid = false;
    upsertRid = rid;
}

inline const LcsRid *LbmSplicerExecStream::getUpsertRidPtr() const
{
    return (nullUpsertRid ? NULL : &upsertRid);
}

FENNEL_END_NAMESPACE

#endif

// End LbmSplicerExecStream.h
