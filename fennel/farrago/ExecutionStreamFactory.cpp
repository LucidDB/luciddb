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
#include "fennel/farrago/ExecutionStreamFactory.h"
#include "fennel/farrago/JavaTupleStream.h"
#include "fennel/farrago/CmdInterpreter.h"
#include "fennel/farrago/TupleStreamBuilder.h"
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
#include "fennel/xo/CalcTupleStream.h"
#include "fennel/db/Database.h"
#include "fennel/db/CheckpointThread.h"
#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/tuple/TupleAccessor.h"
#include "fennel/cache/QuotaCacheAccessor.h"
#include "fennel/segment/SegmentFactory.h"

FENNEL_BEGIN_CPPFILE(
        "$Id$");

ExecutionStream *ExecutionStreamFactors::getStream() const
{
    return pStream;
}

ExecutionStreamParams &ExecutionStreamFactors::getParams() const
{
    return *sharedParams;
}

void ExecutionStreamFactors::prepareStream()
{
    function();
}

ExecutionStreamFactory::ExecutionStreamFactory(
    SharedDatabase pDatabaseInit,
    SharedTableWriterFactory pTableWriterFactoryInit,
    CmdInterpreter::StreamGraphHandle *pStreamGraphHandleInit)
{
    pDatabase = pDatabaseInit;
    pTableWriterFactory = pTableWriterFactoryInit;
    pStreamGraphHandle = pStreamGraphHandleInit;
}

void ExecutionStreamFactory::setScratchAccessor(
    SegmentAccessor &scratchAccessorInit)
{
    scratchAccessor = scratchAccessorInit;
}

const ExecutionStreamFactors &ExecutionStreamFactory::visitStream(
    ProxyExecutionStreamDef &streamDef)
{
    // dispatch based on polymorphic stream type
    FemVisitor::visitTbl.visit(*this,streamDef);
    factors.getStream()->name = streamDef.getName();
    return factors;
}

const ExecutionStreamFactors &
ExecutionStreamFactory::newTracingStream(
    TraceTarget &traceTarget,
    std::string &name,
    ExecutionStreamParams &params)
{
    TracingTupleStream *pTracingStream =
        new TracingTupleStream(traceTarget,name);
    TupleStreamParams *pParams = new TupleStreamParams();
    pParams->pCacheAccessor = params.pCacheAccessor;
    pParams->scratchAccessor = params.scratchAccessor;
    factors.setFactors(pTracingStream,pParams);
    factors.getStream()->name = name;
    return factors;
}

const ExecutionStreamFactors &
ExecutionStreamFactory::newConsumerToProducerProvisionAdapter(
    std::string &name,
    ExecutionStreamParams &params)
{
    ConsumerToProducerProvisionAdapter *pAdapter = 
        new ConsumerToProducerProvisionAdapter();
    TupleStreamParams *pParams = new TupleStreamParams();
    pParams->pCacheAccessor = params.pCacheAccessor;
    pParams->scratchAccessor = params.scratchAccessor;
    factors.setFactors(pAdapter,pParams);
    factors.getStream()->name = name;
    return factors;
}

const ExecutionStreamFactors &
ExecutionStreamFactory::newProducerToConsumerProvisionAdapter(
    std::string &name,
    ExecutionStreamParams &params)
{
    ProducerToConsumerProvisionAdapter *pAdapter = 
        new ProducerToConsumerProvisionAdapter();
    TupleStreamParams *pParams = new TupleStreamParams();
    pParams->pCacheAccessor = params.pCacheAccessor;
    pParams->scratchAccessor = params.scratchAccessor;
    factors.setFactors(pAdapter,pParams);
    factors.getStream()->name = name;
    return factors;
}

// NOTE:  if you are adding a new stream implementation, be careful to follow
// the pattern set by the existing methods:
// (1) allocate new stream object
// (2) allocate new parameters object
// (3) read stream-specific parameters
// (4) set factors
// TODO: do we have a possible memory leak here?
// REVIEW jvs 3-April-2004:  Yes, there's a leak in case of exception.
// Need to fix that in some uniform fashion.

void ExecutionStreamFactory::visit(ProxyIndexScanDef &streamDef)
{
    BTreeScan *pStream = new BTreeScan();
    BTreeScanParams *pParams = new BTreeScanParams();
    readBTreeReadTupleStreamParams(*pParams,streamDef);

    factors.setFactors(pStream,pParams);
}

void ExecutionStreamFactory::visit(ProxyIndexSearchDef &streamDef)
{
    BTreeSearch *pStream =
        streamDef.isUniqueKey() ? new BTreeSearchUnique() : new BTreeSearch();
 
    BTreeSearchParams *pParams = new BTreeSearchParams();
    readBTreeReadTupleStreamParams(*pParams,streamDef);
    pParams->outerJoin = streamDef.isOuterJoin();
    if (streamDef.getInputKeyProj()) {
        readTupleProjection(
            pParams->inputKeyProj,
            streamDef.getInputKeyProj());
    }
    if (streamDef.getInputJoinProj()) {
        readTupleProjection(
            pParams->inputJoinProj,
            streamDef.getInputJoinProj());
    }

    factors.setFactors(pStream,pParams);
}

void ExecutionStreamFactory::visit(ProxyJavaTupleStreamDef &streamDef)
{
    JavaTupleStream *pStream = new JavaTupleStream();
    
    JavaTupleStreamParams *pParams = new JavaTupleStreamParams();
    readTupleStreamParams(*pParams,streamDef);
    pParams->pStreamGraphHandle = pStreamGraphHandle;
    pParams->javaTupleStreamId = streamDef.getStreamId();
    readTupleDescriptor(
        pParams->tupleDesc,
        *(streamDef.getTupleDesc()),
        pDatabase->getTypeFactory());

    factors.setFactors(pStream,pParams);
}

void ExecutionStreamFactory::visit(ProxyTableInserterDef &streamDef)
{
    TableWriterStream *pStream = new TableWriterStream();

    TableWriterStreamParams *pParams = new TableWriterStreamParams();
    pParams->actionType = TableWriter::ACTION_INSERT;
    readTableWriterStreamParams(*pParams,streamDef);

    factors.setFactors(pStream,pParams);
}

void ExecutionStreamFactory::visit(ProxyTableDeleterDef &streamDef)
{
    TableWriterStream *pStream = new TableWriterStream();

    TableWriterStreamParams *pParams = new TableWriterStreamParams();
    pParams->actionType = TableWriter::ACTION_DELETE;
    readTableWriterStreamParams(*pParams,streamDef);

    factors.setFactors(pStream,pParams);
}

void ExecutionStreamFactory::visit(ProxyTableUpdaterDef &streamDef)
{
    TableWriterStream *pStream = new TableWriterStream();

    TableWriterStreamParams *pParams = new TableWriterStreamParams();
    pParams->actionType = TableWriter::ACTION_UPDATE;
    SharedProxyTupleProjection pUpdateProj =
        streamDef.getUpdateProj();
    readTupleProjection(
        pParams->updateProj,
        pUpdateProj);
    readTableWriterStreamParams(*pParams,streamDef);

    factors.setFactors(pStream,pParams);
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
    readTupleProjection(
        pParams->keyProj,
        streamDef.getKeyProj());
    
    factors.setFactors(pStream,pParams);
}

void ExecutionStreamFactory::visit(ProxyIndexLoaderDef &streamDef)
{
    BTreeLoader *pStream = new BTreeLoader();

    BTreeLoaderParams *pParams = new BTreeLoaderParams();
    readBTreeStreamParams(*pParams,streamDef);
    pParams->distinctness = streamDef.getDistinctness();
    pParams->pTempSegment = pDatabase->getTempSegment();

    factors.setFactors(pStream,pParams);
}

void ExecutionStreamFactory::visit(ProxyCartesianProductStreamDef &streamDef)
{
    CartesianProductStream *pStream = new CartesianProductStream();

    CartesianProductStreamParams *pParams = new CartesianProductStreamParams();
    readTupleStreamParams(*pParams,streamDef);

    factors.setFactors(pStream,pParams);
}

void ExecutionStreamFactory::visit(ProxyCalcTupleStreamDef &streamDef)
{
    CalcTupleStream *pStream = new CalcTupleStream();

    CalcTupleStreamParams *pParams = new CalcTupleStreamParams();
    readTupleStreamParams(*pParams,streamDef);
    pParams->program = streamDef.getProgram();
    pParams->isFilter = streamDef.isFilter();

    factors.setFactors(pStream,pParams);
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
    
    factors.setFactors(pStream,pParams);
}

void ExecutionStreamFactory::readTupleStreamParams(
    TupleStreamParams &params,
    ProxyTupleStreamDef &streamDef)
{
    assert(streamDef.getCachePageQuota() >= streamDef.getCachePageMin());
    assert(streamDef.getCachePageQuota() <= streamDef.getCachePageMax());
    if (streamDef.getCachePageQuota()) {
        params.pCacheAccessor = pDatabase->getCache();
        params.scratchAccessor = scratchAccessor;
        if (shouldEnforceCacheQuotas()) {
            // all cache access should be wrapped by quota checks
            uint quota = streamDef.getCachePageQuota();
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
        }
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
            readTupleProjection(
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
    
    readTupleDescriptor(
        params.tupleDesc,
        *(streamDef.getTupleDesc()),
        pDatabase->getTypeFactory());
    
    readTupleProjection(
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
    readTupleProjection(
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

void ExecutionStreamFactory::readTupleDescriptor(
    TupleDescriptor &tupleDesc,
    ProxyTupleDescriptor &javaTupleDesc,
    StoredTypeDescriptorFactory const &typeFactory)
{
    tupleDesc.clear();
    SharedProxyTupleAttrDescriptor pAttr = javaTupleDesc.getAttrDescriptor();
    for (; pAttr; ++pAttr) {
        StoredTypeDescriptor const &typeDescriptor = 
            typeFactory.newDataType(pAttr->getTypeOrdinal());
        tupleDesc.push_back(
            TupleAttributeDescriptor(
                typeDescriptor,pAttr->isNullable(),pAttr->getByteLength()));
    }
}

void ExecutionStreamFactory::readTupleProjection(
    TupleProjection &tupleProj,
    SharedProxyTupleProjection pJavaTupleProj)
{
    tupleProj.clear();
    SharedProxyTupleAttrProjection pAttr = pJavaTupleProj->getAttrProjection();
    for (; pAttr; ++pAttr) {
        tupleProj.push_back(pAttr->getAttributeIndex());
    }
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
