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

#ifndef Fennel_LbmBitOpExecStream_Included
#define Fennel_LbmBitOpExecStream_Included

#include "fennel/tuple/TupleData.h"
#include "fennel/exec/ConfluenceExecStream.h"
#include "fennel/exec/DynamicParam.h"
#include "fennel/lucidera/bitmap/LbmSegmentReader.h"
#include "fennel/lucidera/bitmap/LbmSegmentWriter.h"

FENNEL_BEGIN_NAMESPACE

/**
 * LbmBitOpExecStreamParams defines parameters for instantiating a
 * LbmBitOpExecStream
 */
struct LbmBitOpExecStreamParams : public ConfluenceExecStreamParams
{
    /**
     * Parameter id representing the dynamic parameter used to limit the
     * number of rows producers for this stream should produce on a single
     * execute
     */
    DynamicParamId rowLimitParamId;

    /**
     * Parameter id representing the dynamic parameter used to set the
     * starting rid value for bitmap entries to be produced by this stream's
     * producers
     */
    DynamicParamId startRidParamId;
};

/**
 * LbmBitOpExecStream is a base class for implementing bit operation
 * execution streams that read from N input streams. The first input
 * stream and/or the output stream may be prefixed by key fields.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
class LbmBitOpExecStream : public ConfluenceExecStream
{
protected:
    /**
     * Parameter id representing the dynamic parameter used to limit the
     * number of rows producers for this stream should produce on a single
     * execute
     */
    DynamicParamId rowLimitParamId;

    /**
     * Parameter id representing the dynamic parameter used to set the
     * starting rid value for bitmap entries to be produced by this stream's
     * producers
     */
    DynamicParamId startRidParamId;

    /**
     * Tuple datum used to store dynamic paramter for rowLimit
     */
    TupleDatum rowLimitDatum;

    /**
     * Tuple datum used to store dynamic parameter for startRid
     */
    TupleDatum startRidDatum;

    /**
     * Number of rows that can be produced
     */
    RecordNum rowLimit;

    /**
     * Desired starting rid value for bitmap entries
     */
    LcsRid startRid;

    /**
     * One segment reader for each input stream
     */
    boost::scoped_array<LbmSegmentReader> segmentReaders;

    /**
     * Number of input streams
     */
    uint nInputs;

    /**
     * Current input stream being processed
     */
    uint iInput;

    /**
     * Tuple data for each input stream
     */
    boost::scoped_array<TupleData> bitmapSegTuples;

    /**
     * Segment writer
     */
    LbmSegmentWriter segmentWriter;

    /**
     * Buffer for writing output bitmap segment
     */
    boost::scoped_array<FixedBuffer> outputBuf;

    /**
     * Temporary buffer for bit operation
     */
    boost::scoped_array<FixedBuffer> byteSegBuf;

    /**
     * Pointer to byteSegBuf
     */
    PBuffer pByteSegBuf;

    /**
     * Amount of space available in buffer for bitmaps
     */
    uint bitmapBufSize;

    /**
     * Output tuple data containing AND'd bitmap segments
     */
    TupleData outputTuple;

    /**
     * True if a tuple needs to be written to the output stream
     */
    bool producePending;

    /**
     * Current rid value to be added to the bitmap segment
     */
    LcsRid addRid;

    /**
     * Current byte segment to be added
     */
    PBuffer addByteSeg;

    /**
     * Current length of byte segment to be added
     */
    uint addLen;

    /**
     * Number of non-bitmap fields preceding the bitmap fields
     */
    int nFields;

    /**
     * Produces output tuple that previously failed due to buffer overflow.
     * Writes out the remaining segments that could not fit in the previous
     * buffer.  Determines if input has reached EOS.
     *
     * @param iInput input to read to determine if EOS reached
     *
     * @return EXECRC_YIELD if successful
     */
    ExecStreamResult producePendingOutput(uint iInput);

    /**
     * Reads a byte segment from a specific input stream
     *
     * @param iInput input to read
     * @param currRid startRid of the segment read
     * @param currByteSeg byte segment read; points to the beginning of segment
     * that is stored backwards
     * @param currLen length of the byte segment read
     *
     * @return EXECRC_YIELD if read was successful
     */
    ExecStreamResult readInput(
        uint iInput, LcsRid &currRid, PBuffer &currByteSeg, uint &currLen);

    /**
     * Flushes the segment writer if it has any data. The data is
     * transferred to a bitmap tuple and the segment writer is reset.
     * Finally, the tuple is produced to the output stream. If the tuple
     * cannot be written, then it becomes a pending tuple.
     *
     * @return false if buffer overflow occured while producing the tuple
     */
    bool flush();

    /**
     * Adds the processed segments to the segment under construction.  If
     * the segment fills up, writes it to the output buffer and continues
     * constructing the rest of the segment.  Leading, trailing, and
     * intermediate zeros in the segment are removed.
     *
     * @return false if buffer overflow occurred writing out a segment
     */
    bool addSegments();

    /**
     * Produces a tuple to the output stream, based on a bitmap.
     *
     * @return false if buffer overflow occured while producing the tuple
     */
    virtual bool produceTuple(TupleData bitmapTuple);

public:
    virtual void prepare(LbmBitOpExecStreamParams const &params);
    virtual void open(bool restart);
    virtual void closeImpl();
};

FENNEL_END_NAMESPACE

#endif

// End LbmBitOpExecStream.h
