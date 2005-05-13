/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2003-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 1999-2005 John V. Sichi
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

#include "fennel/common/CommonPreamble.h"
#include "fennel/farrago/ExecutionStreamFactory.h"
#include "fennel/farrago/JavaTupleStream.h"
#include "fennel/farrago/JavaSinkExecStream.h"
#include "fennel/farrago/CmdInterpreter.h"
#include "fennel/xo/BTreeScan.h"
#include "fennel/xo/BTreeSearch.h"
#include "fennel/xo/BTreeSearchUnique.h"
#include "fennel/xo/TableWriterStream.h"
#include "fennel/xo/BTreeLoader.h"
#include "fennel/xo/SortingStream.h"
#include "fennel/xo/BufferingTupleStream.h"
#include "fennel/xo/TracingTupleStream.h"
#include "fennel/xo/ProducerToConsumerProvisionAdapter.h"
#include "fennel/xo/ConsumerToProducerProvisionAdapter.h"
#include "fennel/xo/TableWriterFactory.h"
#include "fennel/xo/CartesianProductStream.h"
#include "fennel/xo/MockTupleStream.h"
#include "fennel/db/Database.h"
#include "fennel/db/CheckpointThread.h"
#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/tuple/TupleAccessor.h"
#include "fennel/cache/QuotaCacheAccessor.h"
#include "fennel/segment/SegmentFactory.h"

FENNEL_BEGIN_CPPFILE(
        "$Id$");

ExecutionStreamFactory::ExecutionStreamFactory(
    SharedDatabase pDatabaseInit,
    SharedTableWriterFactory pTableWriterFactoryInit,
    CmdInterpreter::StreamGraphHandle *pStreamGraphHandleInit)
{
    pDatabase = pDatabaseInit;
    pTableWriterFactory = pTableWriterFactoryInit;
    pStreamGraphHandle = pStreamGraphHandleInit;
}

SharedDatabase ExecutionStreamFactory::getDatabase()
{
    return pDatabase;
}

void ExecutionStreamFactory::setScratchAccessor(
    SegmentAccessor &scratchAccessorInit)
{
    scratchAccessor = scratchAccessorInit;
}

void ExecutionStreamFactory::addSubFactory(
    SharedExecutionStreamSubFactory pSubFactory)
{
    subFactories.push_back(pSubFactory);
}

const ExecutionStreamParts &ExecutionStreamFactory::visitStream(
    ProxyExecutionStreamDef &streamDef)
{
    bool created = false;
    
    // first give sub-factories a shot
    std::vector<SharedExecutionStreamSubFactory>::iterator ppSubFactory;
    for (ppSubFactory = subFactories.begin();
         ppSubFactory != subFactories.end(); ++ppSubFactory)
    {
        ExecutionStreamSubFactory &subFactory = **ppSubFactory;
        created = subFactory.createStream(
            *this,
            streamDef,
            parts);
        if (created) {
            break;
        }
    }

    if (!created) {
        // dispatch based on polymorphic stream type
        invokeVisit(streamDef);
    }
    parts.getStream()->setName(streamDef.getName());
    return parts;
}

void ExecutionStreamFactory::invokeVisit(
    ProxyExecutionStreamDef &streamDef)
{
    FemVisitor::visitTbl.accept(*this,streamDef);
}

const ExecutionStreamParts &
ExecutionStreamFactory::newTracingStream(
    std::string &name,
    ExecutionStreamParts &originalParts)
{
    assert(originalParts.getTraceType() 
           == ExecutionStreamParts::TRACE_TYPE_TUPLE_STREAM);
    TracingTupleStream *pTracingStream = new TracingTupleStream();
    TupleStreamParams *pParams = new TupleStreamParams();
    pParams->outputTupleDesc = originalParts.getParams().outputTupleDesc;
    createQuotaAccessors(*pParams);
    parts.setParts(pTracingStream,pParams);
    parts.getStream()->setName(name);
    return parts;
}

const ExecutionStreamParts &
ExecutionStreamFactory::newConsumerToProducerProvisionAdapter(
    std::string &name,
    ExecutionStreamParams &params)
{
    ConsumerToProducerProvisionAdapter *pAdapter = 
        new ConsumerToProducerProvisionAdapter();
    TupleStreamParams *pParams = new TupleStreamParams();
    pParams->outputTupleDesc = params.outputTupleDesc;
    createQuotaAccessors(*pParams);
    parts.setParts(pAdapter,pParams);
    parts.getStream()->setName(name);
    return parts;
}

const ExecutionStreamParts &
ExecutionStreamFactory::newProducerToConsumerProvisionAdapter(
    std::string &name,
    ExecutionStreamParams &params)
{
    ProducerToConsumerProvisionAdapter *pAdapter = 
        new ProducerToConsumerProvisionAdapter();
    TupleStreamParams *pParams = new TupleStreamParams();
    pParams->outputTupleDesc = params.outputTupleDesc;
    createQuotaAccessors(*pParams);
    parts.setParts(pAdapter,pParams);
    parts.getStream()->setName(name);
    return parts;
}

// NOTE:  if you are adding a new stream implementation, be careful to follow
// the pattern set by the existing methods:
// (1) allocate new stream object
// (2) allocate new parameters object
// (3) read stream-specific parameters
// (4) set parts
// TODO: do we have a possible memory leak here?
// REVIEW jvs 3-April-2004:  Yes, there's a leak in case of exception.
// Need to fix that in some uniform fashion.

void ExecutionStreamFactory::visit(ProxyIndexScanDef &streamDef)
{
    BTreeScan *pStream = new BTreeScan();
    BTreeScanParams *pParams = new BTreeScanParams();
    readBTreeReadTupleStreamParams(*pParams,streamDef);

    parts.setParts(pStream,pParams);
}

void ExecutionStreamFactory::visit(ProxyIndexSearchDef &streamDef)
{
    BTreeSearch *pStream =
        streamDef.isUniqueKey() ? new BTreeSearchUnique() : new BTreeSearch();
 
    BTreeSearchParams *pParams = new BTreeSearchParams();
    readBTreeReadTupleStreamParams(*pParams,streamDef);
    pParams->outerJoin = streamDef.isOuterJoin();
    if (streamDef.getInputKeyProj()) {
        CmdInterpreter::readTupleProjection(
            pParams->inputKeyProj,
            streamDef.getInputKeyProj());
    }
    if (streamDef.getInputJoinProj()) {
        CmdInterpreter::readTupleProjection(
            pParams->inputJoinProj,
            streamDef.getInputJoinProj());
    }

    parts.setParts(pStream,pParams);
}

void ExecutionStreamFactory::visit(ProxyJavaTupleStreamDef &streamDef)
{
    JavaTupleStream *pStream = new JavaTupleStream();
    
    JavaTupleStreamParams *pParams = new JavaTupleStreamParams();
    readTupleStreamParams(*pParams,streamDef);
    pParams->pStreamGraphHandle = pStreamGraphHandle;
    pParams->javaTupleStreamId = streamDef.getStreamId();

    parts.setParts(pStream,pParams);
}

void ExecutionStreamFactory::visit(ProxyTableInserterDef &streamDef)
{
    TableWriterStream *pStream = new TableWriterStream();

    TableWriterStreamParams *pParams = new TableWriterStreamParams();
    pParams->actionType = TableWriter::ACTION_INSERT;
    readTableWriterStreamParams(*pParams,streamDef);

    parts.setParts(pStream,pParams);
}

void ExecutionStreamFactory::visit(ProxyTableDeleterDef &streamDef)
{
    TableWriterStream *pStream = new TableWriterStream();

    TableWriterStreamParams *pParams = new TableWriterStreamParams();
    pParams->actionType = TableWriter::ACTION_DELETE;
    readTableWriterStreamParams(*pParams,streamDef);

    parts.setParts(pStream,pParams);
}

void ExecutionStreamFactory::visit(ProxyTableUpdaterDef &streamDef)
{
    TableWriterStream *pStream = new TableWriterStream();

    TableWriterStreamParams *pParams = new TableWriterStreamParams();
    pParams->actionType = TableWriter::ACTION_UPDATE;
    SharedProxyTupleProjection pUpdateProj =
        streamDef.getUpdateProj();
    CmdInterpreter::readTupleProjection(
        pParams->updateProj,
        pUpdateProj);
    readTableWriterStreamParams(*pParams,streamDef);

    parts.setParts(pStream,pParams);
}

void ExecutionStreamFactory::visit(ProxySortingStreamDef &streamDef)
{
    SortingStream *pStream = new SortingStream();
    SortingStreamParams *pParams = new SortingStreamParams();
    readTupleStreamParams(*pParams,streamDef);
    pParams->distinctness = streamDef.getDistinctness();
    pParams->pSegment = pDatabase->getTempSegment();
    pParams->rootPageId = NULL_PAGE_ID;
    pParams->segmentId = Database::TEMP_SEGMENT_ID;
    pParams->pageOwnerId = ANON_PAGE_OWNER_ID;
    pParams->pRootMap = NULL;
    CmdInterpreter::readTupleProjection(
        pParams->keyProj,
        streamDef.getKeyProj());
    parts.setParts(pStream,pParams);
}

void ExecutionStreamFactory::visit(ProxyIndexLoaderDef &streamDef)
{
    BTreeLoader *pStream = new BTreeLoader();

    BTreeLoaderParams *pParams = new BTreeLoaderParams();
    readBTreeStreamParams(*pParams,streamDef);
    pParams->distinctness = streamDef.getDistinctness();
    pParams->pTempSegment = pDatabase->getTempSegment();

    parts.setParts(pStream,pParams);
}

void ExecutionStreamFactory::visit(ProxyCartesianProductStreamDef &streamDef)
{
    CartesianProductStream *pStream = new CartesianProductStream();

    CartesianProductStreamParams *pParams = new CartesianProductStreamParams();
    readTupleStreamParams(*pParams,streamDef);

    parts.setParts(pStream,pParams);
}

void ExecutionStreamFactory::visit(ProxyMockTupleStreamDef &streamDef)
{
    MockTupleStream *pStream = new MockTupleStream();

    MockTupleStreamParams *pParams = new MockTupleStreamParams();
    readTupleStreamParams(*pParams,streamDef);
    pParams->nRows = streamDef.getRowCount();

    parts.setParts(pStream,pParams);
}

void ExecutionStreamFactory::visit(ProxyBufferingTupleStreamDef &streamDef)
{
    BufferingTupleStream *pStream = new BufferingTupleStream();

    BufferingTupleStreamParams *pParams = new BufferingTupleStreamParams();
    readTupleStreamParams(*pParams,streamDef);
    pParams->multipass = streamDef.isMultipass();
    if (!streamDef.isInMemory()) {
        pParams->scratchAccessor.pSegment = pDatabase->getTempSegment();
        pParams->scratchAccessor.pCacheAccessor = pParams->pCacheAccessor;
    }
    
    parts.setParts(pStream,pParams);
}

void ExecutionStreamFactory::readExecutionStreamParams(
    ExecutionStreamParams &params,
    ProxyExecutionStreamDef &streamDef)
{
    assert(streamDef.getOutputDesc());
    CmdInterpreter::readTupleDescriptor(
        params.outputTupleDesc,
        *(streamDef.getOutputDesc()),
        pDatabase->getTypeFactory());
}

void ExecutionStreamFactory::readTupleStreamParams(
    TupleStreamParams &params,
    ProxyTupleStreamDef &streamDef)
{
    readExecutionStreamParams(params,streamDef);
    createQuotaAccessors(params);
}

void ExecutionStreamFactory::createQuotaAccessors(
    ExecutionStreamParams &params)
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

void ExecutionStreamFactory::readTableWriterStreamParams(
    TableWriterStreamParams &params,
    ProxyTableWriterDef &streamDef)
{
    readTupleStreamParams(params,streamDef);
    params.pTableWriterFactory = pTableWriterFactory;
    params.tableId = ANON_PAGE_OWNER_ID;
    params.pActionMutex = &(pDatabase->getCheckpointThread()->getActionMutex());
    
    SharedProxyIndexWriterDef pIndexWriterDef =
        streamDef.getIndexWriter();
    for (; pIndexWriterDef; ++pIndexWriterDef) {
        TableIndexWriterParams indexParams;
        // all index writers share some common attributes
        indexParams.pCacheAccessor = params.pCacheAccessor;
        indexParams.scratchAccessor = params.scratchAccessor;
        readIndexWriterParams(indexParams,*pIndexWriterDef);
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

void ExecutionStreamFactory::readBTreeStreamParams(
    BTreeStreamParams &params,
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

void ExecutionStreamFactory::readBTreeReadTupleStreamParams(
    BTreeReadTupleStreamParams &params,
    ProxyIndexScanDef &streamDef)
{
    readTupleStreamParams(params,streamDef);
    readBTreeStreamParams(params,streamDef);
    CmdInterpreter::readTupleProjection(
        params.outputProj,
        streamDef.getOutputProj());
}

void ExecutionStreamFactory::readIndexWriterParams(
    TableIndexWriterParams &params,
    ProxyIndexWriterDef &indexWriterDef)
{
    readBTreeStreamParams(params,indexWriterDef);
    params.distinctness = indexWriterDef.getDistinctness();
    params.updateInPlace = indexWriterDef.isUpdateInPlace();
}

bool ExecutionStreamFactory::shouldEnforceCacheQuotas()
{
    TraceLevel traceLevel =
        pDatabase->getTraceTarget().getSourceTraceLevel("xo.quota");
#ifdef DEBUG
    return traceLevel <= TRACE_OFF;
#else
    return traceLevel <= TRACE_FINE;
#endif
}

FENNEL_END_CPPFILE("$Id$");

// End ExecutionStreamFactory.cpp
