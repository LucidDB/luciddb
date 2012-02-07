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

#include "fennel/common/CommonPreamble.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/lbm/LbmNormalizerExecStream.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void LbmNormalizerExecStream::prepare(
    LbmNormalizerExecStreamParams const &params)
{
    ConduitExecStream::prepare(params);

    keyBitmapDesc = pInAccessor->getTupleDesc();
    keyBitmapAccessor.compute(keyBitmapDesc);
    keyBitmapData.compute(keyBitmapDesc);

    // temporarily fake a key projection
    keyProj = params.keyProj;
    keyDesc.projectFrom(keyBitmapDesc, keyProj);
    keyData.compute(keyDesc);
}

void LbmNormalizerExecStream::open(bool restart)
{
    ConduitExecStream::open(restart);
    segmentReader.init(pInAccessor, keyBitmapData);
    producePending = false;
}

ExecStreamResult LbmNormalizerExecStream::execute(
    ExecStreamQuantum const &quantum)
{
    ExecStreamResult rc;

    for (uint i = 0; i < quantum.nTuplesMax; i++) {
        while (! producePending) {
            rc = readSegment();
            if (rc == EXECRC_EOS) {
                pOutAccessor->markEOS();
                return rc;
            } else if (rc != EXECRC_YIELD) {
                return rc;
            }
            assert(producePending);
        }
        if (! produceTuple()) {
            return EXECRC_BUF_OVERFLOW;
        }
    }
    return EXECRC_QUANTUM_EXPIRED;
}

ExecStreamResult LbmNormalizerExecStream::readSegment()
{
    assert(!producePending);

    ExecStreamResult rc = segmentReader.readSegmentAndAdvance(
        segment.byteNum, segment.byteSeg, segment.len);
    if (rc == EXECRC_YIELD) {
        producePending = true;
        nTuplesPending = segment.countBits();
        assert(nTuplesPending > 0);
    }
    return rc;
}

bool LbmNormalizerExecStream::produceTuple()
{
    assert(producePending);

    // manually project output keys from the current tuple
    if (segmentReader.getTupleChange()) {
        for (uint i = 0; i < keyProj.size(); i++) {
            keyData[i] = keyBitmapData[keyProj[i]];
        }
        segmentReader.resetChangeListener();
    }

    if (pOutAccessor->produceTuple(keyData)) {
        nTuplesPending--;
        if (nTuplesPending == 0) {
            producePending = false;
        }
        return true;
    }
    return false;
}

FENNEL_END_CPPFILE("$Id$");

// End LbmNormalizerExecStream.cpp
