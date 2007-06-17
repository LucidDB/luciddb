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

#ifndef Fennel_LbmUnionExecStream_Included
#define Fennel_LbmUnionExecStream_Included

#include "fennel/tuple/TupleData.h"
#include "fennel/exec/ConfluenceExecStream.h"
#include "fennel/exec/DynamicParam.h"
#include "fennel/lucidera/bitmap/LbmSeqSegmentReader.h"
#include "fennel/lucidera/bitmap/LbmSegmentWriter.h"
#include "fennel/lucidera/bitmap/LbmUnionWorkspace.h"

FENNEL_BEGIN_NAMESPACE

/**
 * LbmUnionExecStreamParams defines parameters for instantiating a
 * LbmUnionExecStream
 */
struct LbmUnionExecStreamParams : public ConfluenceExecStreamParams
{
    /**
     * Maximum rid of the table being processed
     */
    LcsRid maxRid;

    /**
     * Parameter id for the dynamic parameter used to limit the 
     * number of rids that should appear in an input tuple. Producers
     * for this stream should respect the limitation.
     *
     * This parameter is set by the union stream during open.
     */
    DynamicParamId ridLimitParamId;

    /**
     * Parameter id for the dynamic parameter used to request filtering
     * of rids output. This dynamic parameter may be set by consumers.
     *
     * This parameter is optional and is read during execute. If not
     * used, this parameter should be set to zero.
     */
    DynamicParamId startRidParamId;

    /**
     * Parameter id for the dynamic parameter used to limit the
     * number of segments this stream should produce on a single
     * execute. May be set by consumers.
     *
     * This parameter is optional and is read during execute. If not
     * used, this parameter should be set to zero.
     */
    DynamicParamId segmentLimitParamId;
};

/**
 * LbmUnionExecStream is the execution stream used to perform
 * a union on a stream of overlapping bitmap tuple
 *
 * @author John Pham
 * @version $Id$
 */
class LbmUnionExecStream : public ConfluenceExecStream
{
    // see LbmUnionExecStreamParams
    LcsRid maxRid;
    DynamicParamId ridLimitParamId;
    DynamicParamId startRidParamId;
    DynamicParamId segmentLimitParamId;

    /**
     * Tuple datum used to store dynamic paramter for ridLimit
     */
    TupleDatum ridLimitDatum;

    /**
     * Number of rids that should appear in input tuples
     */
    RecordNum ridLimit;

    /**
     * Usable page size
     */
    uint pageSize;

    /**
     * Number of pages reserved for the workspace
     */
    uint nWorkspacePages;

    /**
     * Reads input tuples
     */
    LbmSeqSegmentReader segmentReader;

    /**
     * Workspace for merging segments
     */
    LbmUnionWorkspace workspace;

    /**
     * Segment writer
     */
    LbmSegmentWriter segmentWriter;

    /**
     * Scratch accessor for allocating memory for output buffer
     */
    SegmentAccessor scratchAccessor;

    /**
     * Lock on workspace pages
     */
    SegPageLock workspacePageLock;

    /**
     * Lock on writer scratch page
     */
    SegPageLock writerPageLock;

    /**
     * Input tuple data
     */
    TupleData inputTuple;

    /**
     * Segment currently being read
     */
    LbmByteSegment inputSegment;

    /**
     * Scratch area to use for reversing output segments
     */
    PBuffer reverseArea;

    uint reverseAreaSize;

    /**
     * Output tuple data containing OR'd bitmap segments
     */
    TupleData outputTuple;

    /**
     * True if a segment needs to be written to the workspace
     */
    bool writePending;

    /**
     * True if a tuple needs to be written to the output stream
     */
    bool producePending;

    /**
     * True if all input has been processed
     */
    bool isDone;

    /**
     * Start rid requested by a consumer
     */
    LcsRid requestedSrid;

    /**
     * Number of segments remaining before hitting the production limit
     * set by a consumer
     */
    uint segmentsRemaining;

    /**
     * Compute the optimum number of pages for the union, based on the
     * maximum number of rids in the table
     */
    uint computeOptWorkspacePages(LcsRid maxRid);

    /**
     * Returns the maximum tuple size the workspace can handle and
     * still produce segments of reasonable size
     */
    uint computeRidLimit(uint nWorkspacePages);

    /**
     * Whether stream has a parameter for consumer start rid
     */
    bool isConsumerSridSet();

    /**
     * Whether stream has a segment limit
     */
    bool isSegmentLimitSet();

    /**
     * Reads a byte segment.  If the previous byte segment was 
     * not written, then the previous segment is returned.
     */
    ExecStreamResult readSegment();

    /**
     * Attempts to write a segment to the workspace. First eagerly
     * flushes contents of workspace. If contents cannot be flushed,
     * returns false. Once contents have been flushed, write the 
     * segment to the workspace. This should always succeed due to
     * the limit on tuple size.
     */
    bool writeSegment();

    /**
     * Transfers any remaining data from workspace to writer.  Transfers
     * as much as possible, then yields.
     */
    void transferLast();

    /**
     * Transfers data from workspace to writer. Returns false if unable to 
     * transfer all completed workspace contents. For example, returns false 
     * if the writer is unable to accept more data or if yielding due to  
     * the limitation on number of segments.
     */
    bool transfer();

    /**
     * Produces an output tuple.
     */
    bool produceTuple();

public:
    virtual void prepare(LbmUnionExecStreamParams const &params);
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
    virtual void getResourceRequirements(
        ExecStreamResourceQuantity &minQuantity,
        ExecStreamResourceQuantity &optQuantity,
        ExecStreamResourceSettingType &optType);
    virtual void setResourceAllocation(ExecStreamResourceQuantity &quantity);
    virtual void closeImpl();
};

FENNEL_END_NAMESPACE

#endif

// End LbmUnionExecStream.h
