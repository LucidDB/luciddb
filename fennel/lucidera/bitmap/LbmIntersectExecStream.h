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

#ifndef Fennel_LbmIntersectExecStream_Included
#define Fennel_LbmIntersectExecStream_Included

#include "fennel/common/CommonPreamble.h"
#include "fennel/tuple/TupleData.h"
#include "fennel/exec/ConfluenceExecStream.h"
#include "fennel/exec/DynamicParam.h"
#include "fennel/lucidera/bitmap/LbmSegmentReader.h"
#include "fennel/lucidera/bitmap/LbmSegmentWriter.h"

FENNEL_BEGIN_NAMESPACE

/**
 * LbmIntersectExecStreamParams defines parameters for instantiating a
 * LbmIntersectExecStream
 */
struct LbmIntersectExecStreamParams : public ConfluenceExecStreamParams
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
 * LbmIntersectExecStream is the execution stream used to perform
 * intersection on two or more bitmap stream inputs
 *
 * @author Zelaine Fong
 * @version $Id$
 */
class LbmIntersectExecStream : public ConfluenceExecStream
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

    /**
     * True if dynamic parameters have been created
     */
    bool dynParamsCreated;

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
     * Amount of space available in buffer for bitmaps
     */
    uint bitmapBufSize;

    /**
     * Temporary buffer for AND'ing together byte segments
     */
    boost::scoped_array<FixedBuffer> byteSegBuf;

    /**
     * Pointer to byteSegBuf
     */
    PBuffer pByteSegBuf;

    /**
     * Current input stream being processed
     */
    uint iInput;

    /**
     * Number of inputs with overlapping rid values.  Must be equal to
     * nInputs for an intersection to take place.
     */
    uint nMatches;

    /**
     * Minimum length of overlapping bitmap segments found thus far
     */
    uint minLen;

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
     * Perform intersect operation on all segments
     *
     * @param len length of intersecting segments
     *
     * @return false if buffer overflow occurred writing out a segment
     */
    bool intersectSegments(uint len);

    /**
     * Add the intersected segments to the segment under construction.  If
     * the segment fills up, write it to the output buffer and continue
     * constructing the rest of the segment.  Leading, trailing, and
     * intermediate zeros in the segment are removed.
     *
     * @return false if buffer overflow occurred writing out a segment
     */
    bool addSegments();

public:
    explicit LbmIntersectExecStream();
    virtual void prepare(LbmIntersectExecStreamParams const &params);
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
    virtual void closeImpl();
};

FENNEL_END_NAMESPACE

#endif

// End LbmIntersectExecStream.h
