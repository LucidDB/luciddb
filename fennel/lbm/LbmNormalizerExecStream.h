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

#ifndef Fennel_LbmNormalizerExecStream_Included
#define Fennel_LbmNormalizerExecStream_Included

#include "fennel/lbm/LbmByteSegment.h"
#include "fennel/lbm/LbmSeqSegmentReader.h"

#include "fennel/exec/ConduitExecStream.h"
#include "fennel/tuple/TupleAccessor.h"
#include "fennel/tuple/TupleData.h"

FENNEL_BEGIN_NAMESPACE

/**
 * This structure defines parameters for instantiating an
 * LbmNormalizerExecStream
 */
struct LbmNormalizerExecStreamParams : public ConduitExecStreamParams
{
    TupleProjection keyProj;
};

/**
 * The bitmap normalizer stream expands bitmap data to tuples. It can be
 * used to select data directly from a bitmap index. Bitmap data comes in
 * the form:<br>
 * [key1, key2, ..., keyn, srid, bitmap directory, bitmap data]<br>
 * <br>
 * It is converted to the form: <br>
 * [key1, key2, ..., keyn] (repeated for each bit)<br>
 * <br>
 * Rather than returning all keys, it is possible to return a projection
 * of the keys. Rather than tossing the RID (row id) it is possible to
 * return it as a column.
 *
 * @author John Pham
 * @version $Id$
 */
class FENNEL_LBM_EXPORT LbmNormalizerExecStream
    : public ConduitExecStream
{
    /**
     * Input descriptor, accessor, and data
     */
    TupleDescriptor keyBitmapDesc;
    TupleAccessor keyBitmapAccessor;
    TupleData keyBitmapData;

    /**
     * Projection of key columns from input stream
     */
    TupleProjection keyProj;
    TupleDescriptor keyDesc;
    TupleData keyData;

    /**
     * Reads input tuples
     */
    LbmSeqSegmentReader segmentReader;

    /**
     * Whether a tuple is ready to be produced
     */
    bool producePending;

    /**
     * Number of pending tuples in segment
     */
    uint nTuplesPending;

    /**
     * Last segment read
     */
    LbmByteSegment segment;

    /**
     * Reads a segment, returning EXECRC_YIELD on success
     */
    ExecStreamResult readSegment();

    /**
     * Produces a pending tuple to the output stream
     */
    bool produceTuple();

public:
    virtual void prepare(LbmNormalizerExecStreamParams const &params);
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
};

FENNEL_END_NAMESPACE

#endif

// End LbmNormalizerExecStream.h
