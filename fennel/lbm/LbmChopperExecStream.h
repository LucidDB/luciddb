/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2010-2010 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
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

#ifndef Fennel_LbmChopperExecStream_Included
#define Fennel_LbmChopperExecStream_Included

#include "fennel/tuple/TupleData.h"
#include "fennel/exec/ConfluenceExecStream.h"
#include "fennel/exec/DynamicParam.h"
#include "fennel/lbm/LbmByteSegment.h"
#include "fennel/lbm/LbmSeqSegmentReader.h"
#include "fennel/lbm/LbmSegmentWriter.h"

FENNEL_BEGIN_NAMESPACE

/**
 * LbmChopperExecStreamParams defines parameters for instantiating a
 * LbmChopperExecStream
 */
struct LbmChopperExecStreamParams : public ConfluenceExecStreamParams
{
    /**
     * Parameter id for the dynamic parameter used to limit the
     * number of rids that should appear in an output tuple. Can be
     * set by consumers of this stream.
     */
    DynamicParamId ridLimitParamId;
};

/**
 * LbmChopperExecStream splits up tuples whose decompressed
 * representations are too large to fit into the memory of associated
 * streams. There is an opportunity for optimization here, by combining
 * chopped segments. But we do not take this option yet.
 *
 * Transfers a tuple's data to an LbmSegmentWriter, one segment at a time.
 * If a segment should not be written, outputs a tuple and starts a new
 * one.
 *
 * @author John Pham
 * @version $Id$
 */
class FENNEL_LBM_EXPORT LbmChopperExecStream
    : public ConfluenceExecStream
{
    enum LbmChopperState {
        LBM_STATE_NONE = 0,
        LBM_STATE_READ,
        LBM_STATE_WRITE,
        LBM_STATE_PRODUCE,
        LBM_STATE_DONE
    };

    DynamicParamId ridLimitParamId;

    /**
     * Number of rids that should appear in input tuples
     */
    RecordNum ridLimit;

    /**
     * Reads input tuples
     */
    LbmSeqSegmentReader segmentReader;

    /**
     * Input tuple data
     */
    TupleData inputTuple;

    /**
     * Segment currently being read
     */
    LbmByteSegment inputSegment;

    /**
     * Segment writer
     */
    LbmSegmentWriter segmentWriter;

    /**
     * Lock on writer scratch page
     */
    SegPageLock writerPageLock;

    /**
     * Usable page size
     */
    uint pageSize;

    /**
     * State of the chopper
     */
    LbmChopperState state;

    /**
     * True if a segment needs to be written to the workspace
     */
    bool writePending;

    /**
     * True if a tuple needs to be written to the output stream
     */
    bool producePending;

    /**
     * Srid of the tuple currently being written
     */
    LcsRid currentSrid;

    /**
     * Last row id of the tuple currently being written
     */
    LcsRid currentEndRid;

    /**
     * Reads a byte segment.  If the previous byte segment was
     * not written, then the previous segment is returned.
     */
    ExecStreamResult readSegment();

    /**
     * Attempts to add the next segment to the writer. Returns false if
     * the segment is part of another tuple, or would cause the current
     * write tuple to be to break the read limit.
     */
    bool writeSegment();

    /**
     * Produces an output tuple.
     */
    bool produceTuple();

public:
    explicit LbmChopperExecStream();
    virtual void prepare(LbmChopperExecStreamParams const &params);
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
    virtual void getResourceRequirements(
        ExecStreamResourceQuantity &minQuantity,
        ExecStreamResourceQuantity &optQuantity);
    virtual void closeImpl();
};

FENNEL_END_NAMESPACE

#endif

// End LbmChopperExecStream.h
