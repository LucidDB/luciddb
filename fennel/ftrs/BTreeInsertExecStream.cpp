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
#include "fennel/ftrs/BTreeInsertExecStream.h"
#include "fennel/btree/BTreeBuilder.h"
#include "fennel/btree/BTreeWriter.h"
#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/exec/ExecStreamBufAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void BTreeInsertExecStream::prepare(BTreeInsertExecStreamParams const &params)
{
    BTreeExecStream::prepare(params);
    ConduitExecStream::prepare(params);
    distinctness = params.distinctness;
    monotonic = params.monotonic;

    dynamicBTree = (rootPageIdParamId > DynamicParamId(0));
    truncateOnRestart = false;
    assert(treeDescriptor.tupleDescriptor == pInAccessor->getTupleDesc());
}

void BTreeInsertExecStream::getResourceRequirements(
    ExecStreamResourceQuantity &minQuantity,
    ExecStreamResourceQuantity &optQuantity)
{
    BTreeExecStream::getResourceRequirements(minQuantity, optQuantity);

    // max number of pages locked during tree update (REVIEW),
    // including BTreeWriter's private scratch page
    minQuantity.nCachePages += 5;

    // TODO:  use opt to govern prefetch and come up with a good formula
    optQuantity = minQuantity;
}

void BTreeInsertExecStream::open(bool restart)
{
    BTreeExecStream::open(restart);
    ConduitExecStream::open(restart);

    if (dynamicBTree) {
        buildTree(restart);
    }

    if (restart) {
        return;
    }

    if (rootPageIdParamId > DynamicParamId(0)) {
        StandardTypeDescriptorFactory stdTypeFactory;
        TupleAttributeDescriptor attrDesc =
            TupleAttributeDescriptor(
                stdTypeFactory.newDataType(STANDARD_TYPE_UINT_64));
        pDynamicParamManager->createParam(rootPageIdParamId, attrDesc);
        TupleDatum rootPageIdDatum;
        rootPageIdDatum.pData = (PConstBuffer) &(treeDescriptor.rootPageId);
        rootPageIdDatum.cbData = sizeof(PageId);
        pDynamicParamManager->writeParam(rootPageIdParamId, rootPageIdDatum);
    }

    // NOTE: do this last so that rootPageId is available
    pWriter = newWriter(monotonic);
}

void BTreeInsertExecStream::buildTree(bool restart)
{
    if (restart) {
        if (truncateOnRestart) {
            truncateTree(false);
        }
    } else {
        BTreeBuilder builder(
            treeDescriptor,
            treeDescriptor.segmentAccessor.pSegment);
        builder.createEmptyRoot();
        treeDescriptor.rootPageId = builder.getRootPageId();
    }
}

void BTreeInsertExecStream::truncateTree(bool rootless)
{
    if (treeDescriptor.rootPageId == NULL_PAGE_ID) {
        // nothing to do
        assert(rootless);
        return;
    }
    BTreeBuilder builder(
        treeDescriptor,
        treeDescriptor.segmentAccessor.pSegment);
    builder.truncate(rootless);
    if (rootless) {
        treeDescriptor.rootPageId = NULL_PAGE_ID;
    }
}

ExecStreamResult BTreeInsertExecStream::execute(
    ExecStreamQuantum const &quantum)
{
    ExecStreamResult rc = precheckConduitBuffers();
    if (rc != EXECRC_YIELD) {
        if (rc == EXECRC_EOS) {
            // Terminate search so other streams can read from the btree
            pWriter->endSearch();
            pOutAccessor->markEOS();
        }
        return rc;
    }

    uint nTuples = 0;

    for (;;) {
        PConstBuffer pTupleBuf = pInAccessor->getConsumptionStart();
        uint cb = pWriter->insertTupleFromBuffer(pTupleBuf, distinctness);
        pInAccessor->consumeData(pTupleBuf + cb);
        ++nTuples;
        if (nTuples > quantum.nTuplesMax) {
            return EXECRC_QUANTUM_EXPIRED;
        }
        if (!pInAccessor->demandData()) {
            return EXECRC_BUF_UNDERFLOW;
        }
    }
}

void BTreeInsertExecStream::closeImpl()
{
    BTreeExecStream::closeImpl();
    pWriter.reset();
    pBTreeAccessBase.reset();
    ConduitExecStream::closeImpl();
    if (dynamicBTree) {
        truncateTree(true);
    }
}

FENNEL_END_CPPFILE("$Id$");

// End BTreeInsertExecStream.cpp
