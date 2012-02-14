/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
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
