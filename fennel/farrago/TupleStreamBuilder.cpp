/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 1999-2004 John V. Sichi.
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
#include "fennel/farrago/TupleStreamBuilder.h"
#include "fennel/farrago/JavaTupleStream.h"
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
#include "fennel/db/Database.h"
#include "fennel/db/CheckpointThread.h"
#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/tuple/TupleAccessor.h"
#include "fennel/cache/QuotaCacheAccessor.h"
#include "fennel/segment/SegmentFactory.h"

FENNEL_BEGIN_CPPFILE("$Id$");

TupleStreamBuilder::TupleStreamBuilder(
    SharedDatabase pDatabaseInit,
    SharedTableWriterFactory pTableWriterFactoryInit,
    SharedTupleStreamGraph pGraphInit,
    CmdInterpreter::StreamHandle *pStreamHandleInit)
{
    pDatabase = pDatabaseInit;
    pTableWriterFactory = pTableWriterFactoryInit;
    pGraph = pGraphInit;
    pStreamHandle = pStreamHandleInit;
}

void TupleStreamBuilder::buildStreamGraph(ProxyExecutionStreamDef &streamDef)
{
    scratchAccessor =
        pDatabase->getSegmentFactory()->newScratchSegment(
            pDatabase->getCache());
    pGraph->setScratchSegment(scratchAccessor.pSegment);

    // perform a recursive traversal, requesting that the topmost stream
    // provide a buffer (since it has no consumer)
    visitStream(streamDef,TupleStream::PRODUCER_PROVISION);
    
    // the top-level stream visit should have left pChildStream set
    assert(pChildStream);
    // but we don't actually need it, since the graph remembers all the streams
    // just built
    pChildStream.reset();
}

void TupleStreamBuilder::visitStream(
    ProxyExecutionStreamDef &streamDef,
    TupleStream::BufferProvision requiredDataflow)
{
    TraceTarget &traceTarget = pDatabase->getTraceTarget();
    std::string name = streamDef.getName();

    // add the XO prefix
    name = "xo." + name;

    // dispatch based on polymorphic stream type
    FemVisitor::visitTbl.visit(*this,streamDef);
    
    if (traceTarget.getSourceTraceLevel(name) <= TRACE_FINE) {
        // interpose a tracing stream
        addTracingStream(name);
    }

    addAdapterFor(requiredDataflow);
}

void TupleStreamBuilder::buildStreamInputs(
    TupleStream *pNewStream,ProxyExecutionStreamDef &streamDef)
{
    // One of the visit methods is in the middle of building a new stream.
    // First, add a reference to it to the graph.
    SharedTupleStream pStream(pNewStream);
    pGraph->addStream(pStream);

    // Next, recursively visit each input, building depth-first
    SharedProxyExecutionStreamDef pInput = streamDef.getInput();
    for (; pInput; ++pInput) {
        visitStream(
            *pInput,
            pNewStream->getInputBufferRequirement());

        // visit should have left input subtree set in pChildStream
        assert(pChildStream);
        // record dataflow between input and pNewStream
        pGraph->addDataflow(
            pChildStream->getStreamId(),
            pStream->getStreamId());
        // forget input now that we're done with it; the graph remembers it
        pChildStream.reset();
    }
    // return this stream to parent once current visit completes
    pChildStream = pStream;
}

void TupleStreamBuilder::addAdapter(TupleStream &adapter)
{
    SharedTupleStream pSharedAdapter(&adapter);
    pGraph->addStream(pSharedAdapter);
    pGraph->addDataflow(
        pChildStream->getStreamId(),
        pSharedAdapter->getStreamId());
    pChildStream = pSharedAdapter;
    adapter.prepare(childParams);
}

void TupleStreamBuilder::addAdapterFor(
    TupleStream::BufferProvision requiredDataflow)
{
    TupleStream::BufferProvision availableDataflow =
        pChildStream->getResultBufferProvision();
    assert(availableDataflow != TupleStream::NO_PROVISION);
    switch (requiredDataflow) {
    case TupleStream::CONSUMER_PROVISION:
        if (availableDataflow == TupleStream::PRODUCER_PROVISION) {
            addAdapter(*new ProducerToConsumerProvisionAdapter());
        }
        break;
    case TupleStream::PRODUCER_PROVISION:
        if (availableDataflow == TupleStream::CONSUMER_PROVISION) {
            addAdapter(*new ConsumerToProducerProvisionAdapter());
        }
        break;
    case TupleStream::PRODUCER_OR_CONSUMER_PROVISION:
        // we can accept anything, so no adapter required
        break;
    default:
        assert(false);
        break;
    }
}

void TupleStreamBuilder::addTracingStream(std::string name)
{
    TraceTarget &traceTarget = pDatabase->getTraceTarget();
    TracingTupleStream *pTracingStream =
        new TracingTupleStream(traceTarget,name);
    SharedTupleStream pSharedTracingStream(pTracingStream);
    pGraph->addStream(pSharedTracingStream);

    // TracingStream may have different buffer provisioning requirements from
    // real stream, so may need adapters above and below.
    addAdapterFor(pTracingStream->getInputBufferRequirement());

    pGraph->addDataflow(
        pChildStream->getStreamId(),
        pSharedTracingStream->getStreamId());
    pChildStream = pSharedTracingStream;
    pTracingStream->prepare(childParams);
}

// NOTE:  if you are adding a new stream implementation, be careful to follow
// the pattern set by the existing methods:
// (1) allocate new stream object
// (2) immediately call buildStreamInputs
// (3) read stream-specific parameters
// (4) call prepare on the stream

void TupleStreamBuilder::visit(ProxyIndexScanDef &streamDef)
{
    BTreeScan *pStream = new BTreeScan();
    buildStreamInputs(pStream,streamDef);

    BTreeScanParams params;
    readBTreeReadTupleStreamParams(params,streamDef);
    pStream->prepare(params);
}

void TupleStreamBuilder::visit(ProxyIndexSearchDef &streamDef)
{
    BTreeSearch *pStream =
        streamDef.isUniqueKey() ? new BTreeSearchUnique() : new BTreeSearch();
    buildStreamInputs(pStream,streamDef);

    BTreeSearchParams params;
    readBTreeReadTupleStreamParams(params,streamDef);
    params.outerJoin = streamDef.isOuterJoin();
    if (streamDef.getInputKeyProj()) {
        readTupleProjection(
            params.inputKeyProj,
            streamDef.getInputKeyProj());
    }
    if (streamDef.getInputJoinProj()) {
        readTupleProjection(
            params.inputJoinProj,
            streamDef.getInputJoinProj());
    }
    pStream->prepare(params);
}

void TupleStreamBuilder::visit(ProxyJavaTupleStreamDef &streamDef)
{
    JavaTupleStream *pStream = new JavaTupleStream();
    buildStreamInputs(pStream,streamDef);
    
    JavaTupleStreamParams params;
    readTupleStreamParams(params,streamDef);
    params.pStreamHandle = pStreamHandle;
    params.javaTupleStreamId = streamDef.getStreamId();
    readTupleDescriptor(
        params.tupleDesc,
        *(streamDef.getTupleDesc()),
        pDatabase->getTypeFactory());
    pStream->prepare(params);
}

void TupleStreamBuilder::visit(ProxyTableInserterDef &streamDef)
{
    TableWriterStream *pStream = new TableWriterStream();
    buildStreamInputs(pStream,streamDef);

    TableWriterStreamParams params;
    params.actionType = TableWriter::ACTION_INSERT;
    readTableWriterStreamParams(params,streamDef);
    pStream->prepare(params);
}

void TupleStreamBuilder::visit(ProxyTableDeleterDef &streamDef)
{
    TableWriterStream *pStream = new TableWriterStream();
    buildStreamInputs(pStream,streamDef);

    TableWriterStreamParams params;
    params.actionType = TableWriter::ACTION_DELETE;
    readTableWriterStreamParams(params,streamDef);
    pStream->prepare(params);
}

void TupleStreamBuilder::visit(ProxyTableUpdaterDef &streamDef)
{
    TableWriterStream *pStream = new TableWriterStream();
    buildStreamInputs(pStream,streamDef);

    TableWriterStreamParams params;
    params.actionType = TableWriter::ACTION_UPDATE;
    SharedProxyTupleProjection pUpdateProj =
        streamDef.getUpdateProj();
    readTupleProjection(
        params.updateProj,
        pUpdateProj);
    readTableWriterStreamParams(params,streamDef);
    pStream->prepare(params);
}

void TupleStreamBuilder::visit(ProxySortingStreamDef &streamDef)
{
    SortingStream *pStream = new SortingStream();
    buildStreamInputs(pStream,streamDef);

    SortingStreamParams params;
    readTupleStreamParams(params,streamDef);
    params.distinctness = parseDistinctness(streamDef.getDistinctness());
    params.pSegment = pDatabase->getTempSegment();
    params.rootPageId = NULL_PAGE_ID;
    params.segmentId = Database::TEMP_SEGMENT_ID;
    params.pageOwnerId = ANON_PAGE_OWNER_ID;
    params.pRootMap = NULL;
    readTupleProjection(
        params.keyProj,
        streamDef.getKeyProj());
    
    pStream->prepare(params);
}

void TupleStreamBuilder::visit(ProxyIndexLoaderDef &streamDef)
{
    BTreeLoader *pStream = new BTreeLoader();
    buildStreamInputs(pStream,streamDef);

    BTreeLoaderParams params;
    readBTreeStreamParams(params,streamDef);
    params.distinctness = parseDistinctness(streamDef.getDistinctness());
    params.pTempSegment = pDatabase->getTempSegment();
    pStream->prepare(params);
}

void TupleStreamBuilder::visit(ProxyCartesianProductStreamDef &streamDef)
{
    CartesianProductStream *pStream = new CartesianProductStream();
    buildStreamInputs(pStream,streamDef);

    CartesianProductStreamParams params;
    readTupleStreamParams(params,streamDef);
    pStream->prepare(params);
}

void TupleStreamBuilder::visit(ProxyBufferingTupleStreamDef &streamDef)
{
    BufferingTupleStream *pStream = new BufferingTupleStream();
    buildStreamInputs(pStream,streamDef);

    BufferingTupleStreamParams params;
    readTupleStreamParams(params,streamDef);
    params.multipass = streamDef.isMultipass();
    if (!streamDef.isInMemory()) {
        params.scratchAccessor.pSegment = pDatabase->getTempSegment();
        params.scratchAccessor.pCacheAccessor = params.pCacheAccessor;
    }
    
    pStream->prepare(params);
}

void TupleStreamBuilder::readTupleStreamParams(
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
    childParams = params;
}

void TupleStreamBuilder::readTableWriterStreamParams(
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

void TupleStreamBuilder::readBTreeStreamParams(
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
        params.pRootMap = pStreamHandle;
    }
}

void TupleStreamBuilder::readBTreeReadTupleStreamParams(
    BTreeReadTupleStreamParams &params,
    ProxyIndexScanDef &streamDef)
{
    readTupleStreamParams(params,streamDef);
    readBTreeStreamParams(params,streamDef);
    readTupleProjection(
        params.outputProj,
        streamDef.getOutputProj());
}

void TupleStreamBuilder::readIndexWriterParams(
    TableIndexWriterParams &params,
    ProxyIndexWriterDef &indexWriterDef)
{
    readBTreeStreamParams(params,indexWriterDef);
    params.distinctness = parseDistinctness(
        indexWriterDef.getDistinctness());
    params.updateInPlace = indexWriterDef.isUpdateInPlace();
}

void TupleStreamBuilder::readTupleDescriptor(
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

void TupleStreamBuilder::readTupleProjection(
    TupleProjection &tupleProj,
    SharedProxyTupleProjection pJavaTupleProj)
{
    tupleProj.clear();
    SharedProxyTupleAttrProjection pAttr = pJavaTupleProj->getAttrProjection();
    for (; pAttr; ++pAttr) {
        tupleProj.push_back(pAttr->getAttributeIndex());
    }
}

// TODO:  support enumerations in ProxyGen so this isn't required
Distinctness TupleStreamBuilder::parseDistinctness(std::string s)
{
    if (s == "DUP_ALLOW") {
        return DUP_ALLOW;
    } else if (s == "DUP_DISCARD") {
        return DUP_DISCARD;
    } else if (s == "DUP_FAIL") {
        return DUP_FAIL;
    }
    assert(false);
    throw;
}

bool TupleStreamBuilder::shouldEnforceCacheQuotas()
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

// End TupleStreamBuilder.cpp
