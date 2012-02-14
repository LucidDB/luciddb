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
#include "fennel/ftrs/FtrsTableWriterFactory.h"
#include "fennel/common/ByteInputStream.h"
#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/segment/SegmentMap.h"

FENNEL_BEGIN_CPPFILE("$Id$");

FtrsTableWriterFactory::FtrsTableWriterFactory(
    SharedSegmentMap pSegmentMapInit,
    SharedCacheAccessor pCacheAccessorInit,
    StoredTypeDescriptorFactory const &typeFactoryInit,
    SegmentAccessor scratchAccessorInit)
    : pSegmentMap(pSegmentMapInit),
      pCacheAccessor(pCacheAccessorInit),
      typeFactory(typeFactoryInit)
{
    scratchAccessor = scratchAccessorInit;
}

SharedFtrsTableWriter FtrsTableWriterFactory::newTableWriter(
    FtrsTableWriterParams const &params)
{
    // check pool first
    for (uint i = 0; i < pool.size(); ++i) {
        SharedFtrsTableWriter pPooledWriter = pool[i];
        if (pPooledWriter->getTableId() != params.tableId) {
            continue;
        }
        if (pPooledWriter->updateProj != params.updateProj) {
            continue;
        }
        // TODO:  assert that other parameters match?
        return pPooledWriter;
    }
    SharedFtrsTableWriter pNewWriter(new FtrsTableWriter(params));
    pool.push_back(pNewWriter);
    return pNewWriter;
}

SharedLogicalTxnParticipant FtrsTableWriterFactory::loadParticipant(
    LogicalTxnClassId classId,
    ByteInputStream &logStream)
{
    assert(classId == getParticipantClassId());

    TupleDescriptor clusteredTupleDesc;
    clusteredTupleDesc.readPersistent(logStream, typeFactory);

    uint nIndexes;
    logStream.readValue(nIndexes);

    FtrsTableWriterParams params;
    params.indexParams.resize(nIndexes);

    for (uint i = 0; i < nIndexes; ++i) {
        loadIndex(clusteredTupleDesc, params.indexParams[i], logStream);
    }

    params.updateProj.readPersistent(logStream);

    return SharedLogicalTxnParticipant(
        new FtrsTableWriter(params));
}

void FtrsTableWriterFactory::loadIndex(
    TupleDescriptor const &clusteredTupleDesc,
    FtrsTableIndexWriterParams &params,
    ByteInputStream &logStream)
{
    logStream.readValue(params.segmentId);
    logStream.readValue(params.pageOwnerId);
    logStream.readValue(params.rootPageId);
    logStream.readValue(params.distinctness);
    logStream.readValue(params.updateInPlace);
    params.inputProj.readPersistent(logStream);
    params.keyProj.readPersistent(logStream);
    if (params.inputProj.empty()) {
        params.tupleDesc = clusteredTupleDesc;
    } else {
        params.tupleDesc.projectFrom(clusteredTupleDesc, params.inputProj);
    }
    params.pCacheAccessor = pCacheAccessor;
    SharedSegment pSegment;
    params.pSegment = pSegmentMap->getSegmentById(params.segmentId, pSegment);
    params.scratchAccessor = scratchAccessor;
}

LogicalTxnClassId FtrsTableWriterFactory::getParticipantClassId()
{
    return LogicalTxnClassId(0xaa6576b8efadbcdcLL);
}

FENNEL_END_CPPFILE("$Id$");

// End FtrsTableWriterFactory.cpp
