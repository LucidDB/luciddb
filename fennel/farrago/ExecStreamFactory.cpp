/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 1999-2004 John V. Sichi.
// Copyright (C) 2003-2004 Disruptive Tech
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

#include "fennel/common/CommonPreamble.h"
#include "fennel/farrago/ExecStreamFactory.h"
#include "fennel/farrago/ExecutionStreamFactory.h"
#include "fennel/farrago/JavaSourceExecStream.h"
#include "fennel/farrago/CmdInterpreter.h"
#include "fennel/ftrs/BTreeScanExecStream.h"
#include "fennel/ftrs/BTreeSearchExecStream.h"
#include "fennel/ftrs/BTreeSearchUniqueExecStream.h"
#include "fennel/ftrs/FtrsTableWriterExecStream.h"
#include "fennel/ftrs/BTreeSortExecStream.h"
#include "fennel/exec/SegBufferExecStream.h"
#include "fennel/exec/CopyExecStream.h"
#include "fennel/exec/ScratchBufferExecStream.h"
#include "fennel/ftrs/FtrsTableWriterFactory.h"
#include "fennel/exec/CartesianJoinExecStream.h"
#include "fennel/exec/MockProducerExecStream.h"
#include "fennel/db/Database.h"
#include "fennel/db/CheckpointThread.h"
#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/tuple/TupleAccessor.h"
#include "fennel/cache/QuotaCacheAccessor.h"
#include "fennel/segment/SegmentFactory.h"

FENNEL_BEGIN_CPPFILE(
        "$Id$");

ExecStreamFactory::ExecStreamFactory(
    SharedDatabase pDatabaseInit,
    SharedFtrsTableWriterFactory pTableWriterFactoryInit,
    CmdInterpreter::StreamGraphHandle *pStreamGraphHandleInit)
{
    pDatabase = pDatabaseInit;
    pTableWriterFactory = pTableWriterFactoryInit;
    pStreamGraphHandle = pStreamGraphHandleInit;
}

void *ExecStreamFactory::getLeafPtr() 
{
    return static_cast<FemVisitor *>(this);
}

const char *ExecStreamFactory::getLeafTypeName()
{
    return "FemVisitor";
}

SharedDatabase ExecStreamFactory::getDatabase()
{
    return pDatabase;
}

void ExecStreamFactory::setScratchAccessor(
    SegmentAccessor &scratchAccessorInit)
{
    scratchAccessor = scratchAccessorInit;
}

void ExecStreamFactory::addSubFactory(
    SharedExecStreamSubFactory pSubFactory)
{
    subFactories.push_back(pSubFactory);
}

ExecStreamEmbryo const &ExecStreamFactory::visitStream(
    ProxyExecutionStreamDef &streamDef)
{
    bool created = false;
    
    // first give sub-factories a shot
    std::vector<SharedExecStreamSubFactory>::iterator ppSubFactory;
    for (ppSubFactory = subFactories.begin();
         ppSubFactory != subFactories.end(); ++ppSubFactory)
    {
        ExecStreamSubFactory &subFactory = **ppSubFactory;
        created = subFactory.createStream(
            *this,
            streamDef,
            embryo);
        if (created) {
            break;
        }
    }

    if (!created) {
        // dispatch based on polymorphic stream type
        invokeVisit(streamDef);
    }
    embryo.getStream()->setName(streamDef.getName());
    return embryo;
}

void ExecStreamFactory::invokeVisit(
    ProxyExecutionStreamDef &streamDef)
{
    FemVisitor::visitTbl.accept(*this,streamDef);
}

const ExecStreamEmbryo &
ExecStreamFactory::newConsumerToProducerProvisionAdapter(
    std::string &name,
    ExecStreamParams const &params)
{
    ScratchBufferExecStreamParams adapterParams;
    copyAdapterParams(adapterParams, params);
    createQuotaAccessors(adapterParams);
    embryo.init(
        new ScratchBufferExecStream(),
        adapterParams);
    embryo.getStream()->setName(name);
    return embryo;
}

const ExecStreamEmbryo &
ExecStreamFactory::newProducerToConsumerProvisionAdapter(
    std::string &name,
    ExecStreamParams const &params)
{
    CopyExecStreamParams adapterParams;
    copyAdapterParams(adapterParams, params);
    createQuotaAccessors(adapterParams);
    embryo.init(
        new CopyExecStream(),
        adapterParams);
    embryo.getStream()->setName(name);
    return embryo;
}

void ExecStreamFactory::copyAdapterParams(
    ExecStreamParams &adapterParams,
    ExecStreamParams const &params)
{
    adapterParams.pCacheAccessor = params.pCacheAccessor;
    adapterParams.scratchAccessor = params.scratchAccessor;
    adapterParams.enforceQuotas = params.enforceQuotas;
}

// NOTE:  if you are adding a new stream implementation, be careful to follow
// the pattern set by the existing methods:
// (1) declare params on stack
// (2) assign values to params
// (3) call embryo.init(new YourVerySpecialExecStream(), params)

void ExecStreamFactory::visit(ProxyIndexScanDef &streamDef)
{
    BTreeScanExecStreamParams params;
    readBTreeReadStreamParams(params, streamDef);
    embryo.init(
        new BTreeScanExecStream(),
        params);
}

void ExecStreamFactory::visit(ProxyIndexSearchDef &streamDef)
{
    BTreeSearchExecStreamParams params;
    readBTreeReadStreamParams(params, streamDef);
    params.outerJoin = streamDef.isOuterJoin();
    if (streamDef.getInputKeyProj()) {
        CmdInterpreter::readTupleProjection(
            params.inputKeyProj,
            streamDef.getInputKeyProj());
    }
    if (streamDef.getInputJoinProj()) {
        CmdInterpreter::readTupleProjection(
            params.inputJoinProj,
            streamDef.getInputJoinProj());
    }
    embryo.init(
        streamDef.isUniqueKey()
        ? new BTreeSearchUniqueExecStream() : new BTreeSearchExecStream(),
        params);
}

void ExecStreamFactory::visit(ProxyJavaTupleStreamDef &streamDef)
{
    JavaSourceExecStreamParams params;
    readTupleStreamParams(params, streamDef);
    params.pStreamGraphHandle = pStreamGraphHandle;
    params.javaTupleStreamId = streamDef.getStreamId();
    embryo.init(new JavaSourceExecStream(), params);
}

void ExecStreamFactory::visit(ProxyTableInserterDef &streamDef)
{
    FtrsTableWriterExecStreamParams params;
    params.actionType = FtrsTableWriter::ACTION_INSERT;
    readTableWriterStreamParams(params, streamDef);
    embryo.init(new FtrsTableWriterExecStream(), params);
}

void ExecStreamFactory::visit(ProxyTableDeleterDef &streamDef)
{
    FtrsTableWriterExecStreamParams params;
    params.actionType = FtrsTableWriter::ACTION_DELETE;
    readTableWriterStreamParams(params, streamDef);
    embryo.init(new FtrsTableWriterExecStream(), params);
}

void ExecStreamFactory::visit(ProxyTableUpdaterDef &streamDef)
{
    FtrsTableWriterExecStreamParams params;
    params.actionType = FtrsTableWriter::ACTION_UPDATE;
    SharedProxyTupleProjection pUpdateProj = streamDef.getUpdateProj();
    CmdInterpreter::readTupleProjection(
        params.updateProj,
        pUpdateProj);
    readTableWriterStreamParams(params, streamDef);
    embryo.init(new FtrsTableWriterExecStream(), params);
}

void ExecStreamFactory::visit(ProxySortingStreamDef &streamDef)
{
    BTreeSortExecStreamParams params;
    readTupleStreamParams(params,streamDef);
    params.distinctness = streamDef.getDistinctness();
    params.pSegment = pDatabase->getTempSegment();
    params.rootPageId = NULL_PAGE_ID;
    params.segmentId = Database::TEMP_SEGMENT_ID;
    params.pageOwnerId = ANON_PAGE_OWNER_ID;
    params.pRootMap = NULL;
    CmdInterpreter::readTupleProjection(
        params.keyProj,
        streamDef.getKeyProj());
    params.tupleDesc = params.outputTupleDesc;
    embryo.init(new BTreeSortExecStream(), params);
}

void ExecStreamFactory::visit(ProxyIndexLoaderDef &streamDef)
{
    // TODO
#if 0    
    BTreeLoader *pStream = new BTreeLoader();

    BTreeLoaderParams *pParams = new BTreeLoaderParams();
    readBTreeStreamParams(*pParams,streamDef);
    pParams->distinctness = streamDef.getDistinctness();
    pParams->pTempSegment = pDatabase->getTempSegment();

    parts.setParts(pStream,pParams);
#endif
    permAssert(false);
}

void ExecStreamFactory::visit(ProxyCartesianProductStreamDef &streamDef)
{
    CartesianJoinExecStreamParams params;
    readTupleStreamParams(params, streamDef);
    embryo.init(new CartesianJoinExecStream(), params);
}

void ExecStreamFactory::visit(ProxyMockTupleStreamDef &streamDef)
{
    MockProducerExecStreamParams params;
    readTupleStreamParams(params, streamDef);
    params.nRows = streamDef.getRowCount();
    embryo.init(new MockProducerExecStream(), params);
}

void ExecStreamFactory::visit(ProxyBufferingTupleStreamDef &streamDef)
{
    SegBufferExecStreamParams params;
    readTupleStreamParams(params, streamDef);
    params.multipass = streamDef.isMultipass();
    if (!streamDef.isInMemory()) {
        params.scratchAccessor.pSegment = pDatabase->getTempSegment();
        params.scratchAccessor.pCacheAccessor = params.pCacheAccessor;
    }
    embryo.init(new SegBufferExecStream(), params);
}

void ExecStreamFactory::readExecStreamParams(
    ExecStreamParams &params,
    ProxyExecutionStreamDef &streamDef)
{
    createQuotaAccessors(params);
}

void ExecStreamFactory::readTupleStreamParams(
    SingleOutputExecStreamParams &params,
    ProxyTupleStreamDef &streamDef)
{
    readExecStreamParams(params,streamDef);
    assert(streamDef.getOutputDesc());
    CmdInterpreter::readTupleDescriptor(
        params.outputTupleDesc,
        *(streamDef.getOutputDesc()),
        pDatabase->getTypeFactory());
}

void ExecStreamFactory::createQuotaAccessors(
    ExecStreamParams &params)
{
    params.pCacheAccessor = pDatabase->getCache();
    params.scratchAccessor = scratchAccessor;
    if (shouldEnforceCacheQuotas()) {
        params.enforceQuotas = true;
        // All cache access should be wrapped by quota checks.  Actual
        // quotas will be set per-execution.
        uint quota = 0;
        SharedQuotaCacheAccessor pQuotaAccessor(
            new QuotaCacheAccessor(
                SharedQuotaCacheAccessor(),
                params.pCacheAccessor,
                quota));
        params.pCacheAccessor = pQuotaAccessor;

        // scratch access has to go through a separate CacheAccessor, but
        // delegates quota checking to pQuotaAccessor
        params.scratchAccessor.pCacheAccessor.reset(
            new QuotaCacheAccessor(
                pQuotaAccessor,
                params.scratchAccessor.pCacheAccessor,
                quota));
    } else {
        params.enforceQuotas = false;
    }
}

void ExecStreamFactory::readTableWriterStreamParams(
    FtrsTableWriterExecStreamParams &params,
    ProxyTableWriterDef &streamDef)
{
    readTupleStreamParams(params, streamDef);
    params.pTableWriterFactory = pTableWriterFactory;
    params.tableId = ANON_PAGE_OWNER_ID;
    params.pActionMutex = &(pDatabase->getCheckpointThread()->getActionMutex());
    
    SharedProxyIndexWriterDef pIndexWriterDef = streamDef.getIndexWriter();
    for (; pIndexWriterDef; ++pIndexWriterDef) {
        FtrsTableIndexWriterParams indexParams;
        // all index writers share some common attributes
        indexParams.pCacheAccessor = params.pCacheAccessor;
        indexParams.scratchAccessor = params.scratchAccessor;
        readIndexWriterParams(indexParams, *pIndexWriterDef);
        SharedProxyTupleProjection pInputProj =
            pIndexWriterDef->getInputProj();
        if (pInputProj) {
            CmdInterpreter::readTupleProjection(
                indexParams.inputProj,
                pInputProj);
        } else {
            // this is the clustered index; use it as a table ID
            params.tableId = indexParams.pageOwnerId;
        }
        params.indexParams.push_back(indexParams);
    }
    assert(params.tableId != ANON_PAGE_OWNER_ID);
}

void ExecStreamFactory::readBTreeStreamParams(
    BTreeExecStreamParams &params,
    ProxyIndexAccessorDef &streamDef)
{
    assert(params.pCacheAccessor);
    
    params.segmentId = SegmentId(streamDef.getSegmentId());
    params.pageOwnerId = PageOwnerId(streamDef.getIndexId());
    params.pSegment = pDatabase->getSegmentById(params.segmentId);
    
    CmdInterpreter::readTupleDescriptor(
        params.tupleDesc,
        *(streamDef.getTupleDesc()),
        pDatabase->getTypeFactory());
    
    CmdInterpreter::readTupleProjection(
        params.keyProj,
        streamDef.getKeyProj());

    if (streamDef.getRootPageId() != -1) {
        params.rootPageId = PageId(streamDef.getRootPageId());
        params.pRootMap = NULL;
    } else {
        params.rootPageId = NULL_PAGE_ID;
        params.pRootMap = pStreamGraphHandle;
    }
}

void ExecStreamFactory::readBTreeReadStreamParams(
    BTreeReadExecStreamParams &params,
    ProxyIndexScanDef &streamDef)
{
    readTupleStreamParams(params, streamDef);
    readBTreeStreamParams(params, streamDef);
    CmdInterpreter::readTupleProjection(
        params.outputProj,
        streamDef.getOutputProj());
}

void ExecStreamFactory::readIndexWriterParams(
    FtrsTableIndexWriterParams &params,
    ProxyIndexWriterDef &indexWriterDef)
{
    readBTreeStreamParams(params, indexWriterDef);
    params.distinctness = indexWriterDef.getDistinctness();
    params.updateInPlace = indexWriterDef.isUpdateInPlace();
}

bool ExecStreamFactory::shouldEnforceCacheQuotas()
{
    TraceLevel traceLevel =
        pDatabase->getTraceTarget().getSourceTraceLevel("xo.quota");
#ifdef DEBUG
    return traceLevel <= TRACE_OFF;
#else
    return traceLevel <= TRACE_FINE;
#endif
}

ExecStreamSubFactory::~ExecStreamSubFactory()
{
}

FENNEL_END_CPPFILE("$Id$");

// End ExecStreamFactory.cpp
