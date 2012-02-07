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

#ifndef Fennel_LbmTupleReader_Included
#define Fennel_LbmTupleReader_Included

#include "fennel/exec/ExecStreamDefs.h"
#include "fennel/tuple/TupleData.h"
#include "fennel/lbm/LbmSegment.h"

FENNEL_BEGIN_NAMESPACE

typedef TupleData *PTupleData;

/**
 * LbmTupleReader is an interface for reading bitmap tuples
 *
 * @author John Pham
 * @version $Id$
 */
class FENNEL_LBM_EXPORT LbmTupleReader
{
public:
    virtual ~LbmTupleReader();

    /**
     * Reads an input tuple. The tuple read remains valid until the next
     * call to this method.
     *
     * @return EXECRC_YIELD if read was successful, EXECRC_EOS if there
     * was no more data to be read, or EXECRC_BUF_UNDERFLOW if an input
     * stream buffer was exhausted
     */
    virtual ExecStreamResult read(PTupleData &pTupleData) = 0;
};

/**
 * LbmStreamTupleReader is a base class for reading bitmap tuples
 * from an input stream
 */
class FENNEL_LBM_EXPORT LbmStreamTupleReader
    : public LbmTupleReader
{
    /**
     * Input stream accessor
     */
    SharedExecStreamBufAccessor pInAccessor;

    /**
     * Pointer to a tupledata containing the input bitmap tuple
     */
    PTupleData pInputTuple;

public:
    /**
     * Initializes reader to start reading bitmap tuples from a specified
     * input stream
     *
     * @param pInAccessorInit input stream accessor
     *
     * @param bitmapSegTupleInit tuple data for reading tuples
     */
    void init(
        SharedExecStreamBufAccessor &pInAccessorInit,
        TupleData &bitmapSegTupleInit);

    // implement LbmTupleReader
    ExecStreamResult read(PTupleData &pTupleData);
};

/**
 * LbmSingleTupleReader is a class satisyfing the bitmap tuple reader
 * interface for a single input tuple
 */
class FENNEL_LBM_EXPORT LbmSingleTupleReader
    : public LbmTupleReader
{
    /**
     * Whether the segment reader has a tuple to return
     */
    bool hasTuple;

    /**
     * Pointer to a tuple data containing the input bitmap tuple
     */
    PTupleData pInputTuple;

public:
    /**
     * Initializes reader to return a specified tuple
     *
     * @param bitmapSegTupleInit tuple data for reading tuples
     */
    void init(TupleData &bitmapSegTupleInit);

    // implement LbmTupleReader
    ExecStreamResult read(PTupleData &pTupleData);
};

FENNEL_END_NAMESPACE

#endif

// End LbmTupleReader.h
