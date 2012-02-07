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
#include "fennel/ftrs/BTreeExecStream.h"
#include "fennel/btree/BTreeWriter.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void BTreeExecStream::prepare(BTreeExecStreamParams const &params)
{
    SingleOutputExecStream::prepare(params);

    copyParamsToDescriptor(treeDescriptor, params, params.pCacheAccessor);
    scratchAccessor = params.scratchAccessor;
    pRootMap = params.pRootMap;
    rootPageIdParamId = params.rootPageIdParamId;
}

void BTreeExecStream::open(bool restart)
{
    SingleOutputExecStream::open(restart);
    if (restart) {
        endSearch();
    }
    if (!restart) {
        if (pRootMap) {
            treeDescriptor.rootPageId = pRootMap->getRoot(
                treeDescriptor.pageOwnerId);
            if (pBTreeAccessBase) {
                pBTreeAccessBase->setRootPageId(treeDescriptor.rootPageId);
            }
        }
    }
}

void BTreeExecStream::closeImpl()
{
    endSearch();
    if (pRootMap && pBTreeAccessBase) {
        treeDescriptor.rootPageId = NULL_PAGE_ID;
        pBTreeAccessBase->setRootPageId(NULL_PAGE_ID);
    }
    SingleOutputExecStream::closeImpl();
}

SharedBTreeReader BTreeExecStream::newReader()
{
    SharedBTreeReader pReader = SharedBTreeReader(
        new BTreeReader(treeDescriptor));
    pBTreeAccessBase = pBTreeReader = pReader;
    return pReader;
}

SharedBTreeWriter BTreeExecStream::newWriter(bool monotonic)
{
    SharedBTreeWriter pWriter = SharedBTreeWriter(
        new BTreeWriter(treeDescriptor, scratchAccessor, monotonic));
    pBTreeAccessBase = pBTreeReader = pWriter;
    return pWriter;
}

SharedBTreeWriter BTreeExecStream::newWriter(
    BTreeExecStreamParams const &params)
{
    BTreeDescriptor treeDescriptor;
    copyParamsToDescriptor(treeDescriptor, params, params.pCacheAccessor);
    return SharedBTreeWriter(
        new BTreeWriter(
            treeDescriptor, params.scratchAccessor));
}

void BTreeExecStream::copyParamsToDescriptor(
    BTreeDescriptor &treeDescriptor,
    BTreeParams const &params,
    SharedCacheAccessor const &pCacheAccessor)
{
    treeDescriptor.segmentAccessor.pSegment = params.pSegment;
    treeDescriptor.segmentAccessor.pCacheAccessor = pCacheAccessor;
    treeDescriptor.tupleDescriptor = params.tupleDesc;
    treeDescriptor.keyProjection = params.keyProj;
    treeDescriptor.rootPageId = params.rootPageId;
    treeDescriptor.segmentId = params.segmentId;
    treeDescriptor.pageOwnerId = params.pageOwnerId;
}

void BTreeExecStream::endSearch()
{
    if (pBTreeReader && pBTreeReader->isSingular() == false) {
        pBTreeReader->endSearch();
    }
}

BTreeExecStreamParams::BTreeExecStreamParams()
{
    pRootMap = NULL;
}

BTreeOwnerRootMap::~BTreeOwnerRootMap()
{
}

FENNEL_END_CPPFILE("$Id$");

// End BTreeExecStream.cpp
