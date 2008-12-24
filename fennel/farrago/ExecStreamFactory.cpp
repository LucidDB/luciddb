/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2003-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 1999-2007 John V. Sichi
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
#include "fennel/farrago/ExecStreamFactory.h"
#include "fennel/farrago/JavaSinkExecStream.h"
#include "fennel/farrago/JavaTransformExecStream.h"
#include "fennel/farrago/CmdInterpreter.h"
#include "fennel/ftrs/BTreePrefetchSearchExecStream.h"
#include "fennel/ftrs/BTreeScanExecStream.h"
#include "fennel/ftrs/BTreeSearchExecStream.h"
#include "fennel/ftrs/BTreeSearchUniqueExecStream.h"
#include "fennel/ftrs/FtrsTableWriterExecStream.h"
#include "fennel/ftrs/BTreeSortExecStream.h"
#include "fennel/exec/MergeExecStream.h"
#include "fennel/exec/SegBufferExecStream.h"
#include "fennel/exec/SplitterExecStream.h"
#include "fennel/exec/BarrierExecStream.h"
#include "fennel/exec/ValuesExecStream.h"
#include "fennel/exec/ExecStreamGraphEmbryo.h"
#include "fennel/ftrs/FtrsTableWriterFactory.h"
#include "fennel/exec/CartesianJoinExecStream.h"
#include "fennel/exec/SortedAggExecStream.h"
#include "fennel/exec/MockProducerExecStream.h"
#include "fennel/exec/ReshapeExecStream.h"
#include "fennel/exec/NestedLoopJoinExecStream.h"
#include "fennel/exec/BernoulliSamplingExecStream.h"
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
    pGraphEmbryo = NULL;
}

SharedDatabase ExecStreamFactory::getDatabase()
{
    return pDatabase;
}

void ExecStreamFactory::setGraphEmbryo(
    ExecStreamGraphEmbryo &graphEmbryo)
{
    pGraphEmbryo = &graphEmbryo;
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

// NOTE:  if you are adding a new stream implementation, be careful to follow
// the pattern set by the existing methods:
// (1) declare params on stack
// (2) assign values to params
// (3) call embryo.init(new YourVerySpecialExecStream(), params)

void ExecStreamFactory::visit(ProxyBarrierStreamDef &streamDef)
{
    BarrierExecStreamParams params;
    readTupleStreamParams(params, streamDef);
    params.returnMode = streamDef.getReturnMode();
    readBarrierDynamicParams(params, streamDef);
    embryo.init(new BarrierExecStream(), params);
}

void ExecStreamFactory::readBarrierDynamicParams(
    BarrierExecStreamParams &params,
    ProxyBarrierStreamDef &streamDef)
{
    SharedProxyDynamicParameter dynamicParam = streamDef.getDynamicParameter();
    for ( ; dynamicParam; ++dynamicParam) {
        DynamicParamId p = (DynamicParamId) dynamicParam->getParameterId();
        params.parameterIds.push_back(p);
    }
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

void ExecStreamFactory::visit(ProxyCartesianProductStreamDef &streamDef)
{
    CartesianJoinExecStreamParams params;
    readTupleStreamParams(params, streamDef);
    params.leftOuter = streamDef.isLeftOuter();
    embryo.init(new CartesianJoinExecStream(), params);
}

void ExecStreamFactory::visit(ProxyIndexLoaderDef &streamDef)
{
    BTreeInsertExecStreamParams params;
    readTupleStreamParams(params, streamDef);
    readBTreeStreamParams(params, streamDef);
    params.distinctness = streamDef.getDistinctness();
    params.monotonic = streamDef.isMonotonic();
    embryo.init(new BTreeInsertExecStream(), params);
}

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
    assert(!(streamDef.isUniqueKey() && streamDef.isPrefetch()));
    if (streamDef.isPrefetch()) {
        BTreePrefetchSearchExecStreamParams params;
        initBTreePrefetchSearchParams(params, streamDef);
        embryo.init(
            new BTreePrefetchSearchExecStream(),
            params);
    } else {
        BTreeSearchExecStreamParams params;
        readBTreeSearchStreamParams(params, streamDef);
        embryo.init(
            streamDef.isUniqueKey()
            ? new BTreeSearchUniqueExecStream() : new BTreeSearchExecStream(),
            params);
    }
}

void ExecStreamFactory::initBTreePrefetchSearchParams(
    BTreePrefetchSearchExecStreamParams &params,
    ProxyIndexSearchDef &streamDef)
{
    readBTreeSearchStreamParams(params, streamDef);
    // Need a private scratch segment because scratch pages are
    // deallocated when the stream is closed.
    createPrivateScratchSegment(params);
}

void ExecStreamFactory::visit(ProxyJavaSinkStreamDef &streamDef)
{
    JavaSinkExecStreamParams params;
    readExecStreamParams(params, streamDef);
    params.pStreamGraphHandle = pStreamGraphHandle;
    params.javaFennelPipeTupleIterId = streamDef.getStreamId();
    embryo.init(new JavaSinkExecStream(), params);
}

void ExecStreamFactory::visit(ProxyJavaTransformStreamDef &streamDef)
{
    JavaTransformExecStreamParams params;

    readExecStreamParams(params, streamDef);

    readTupleDescriptor(params.outputTupleDesc, streamDef.getOutputDesc());

    params.pStreamGraphHandle = pStreamGraphHandle;
    params.javaClassName = streamDef.getJavaClassName();
    embryo.init(new JavaTransformExecStream(), params);
}

void ExecStreamFactory::visit(ProxyMergeStreamDef &streamDef)
{
    MergeExecStreamParams params;
    readTupleStreamParams(params, streamDef);
    if (!streamDef.isSequential()) {
        params.isParallel = true;
    }
    // prePullInputs parameter isn't actually supported yet
    assert(!streamDef.isPrePullInputs());
    embryo.init(new MergeExecStream(), params);
}

void ExecStreamFactory::visit(ProxyMockTupleStreamDef &streamDef)
{
    MockProducerExecStreamParams params;
    readTupleStreamParams(params, streamDef);
    params.nRows = streamDef.getRowCount();
    embryo.init(new MockProducerExecStream(), params);
}

void ExecStreamFactory::visit(ProxyTableDeleterDef &streamDef)
{
    FtrsTableWriterExecStreamParams params;
    params.actionType = FtrsTableWriter::ACTION_DELETE;
    readTableWriterStreamParams(params, streamDef);
    embryo.init(new FtrsTableWriterExecStream(), params);
}

void ExecStreamFactory::visit(ProxyTableInserterDef &streamDef)
{
    FtrsTableWriterExecStreamParams params;
    params.actionType = FtrsTableWriter::ACTION_INSERT;
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

void ExecStreamFactory::visit(ProxySortedAggStreamDef &streamDef)
{
    SortedAggExecStreamParams params;
    readAggStreamParams(params, streamDef);
    embryo.init(new SortedAggExecStream(), params);
}

void ExecStreamFactory::visit(ProxySortingStreamDef &streamDef)
{
    BTreeSortExecStreamParams params;
    readTupleStreamParams(params,streamDef);
    params.distinctness = streamDef.getDistinctness();
    params.monotonic = false;
    params.pSegment = pDatabase->getTempSegment();
    params.rootPageId = NULL_PAGE_ID;
    params.segmentId = Database::TEMP_SEGMENT_ID;
    params.pageOwnerId = ANON_PAGE_OWNER_ID;
    params.pRootMap = NULL;
    params.rootPageIdParamId = DynamicParamId(0);
    CmdInterpreter::readTupleProjection(
        params.keyProj,
        streamDef.getKeyProj());
    // TODO jvs 3-Dec-2006:  pass along streamDef.getDescendingProj() once
    // btree can deal with it
    params.tupleDesc = params.outputTupleDesc;
    embryo.init(new BTreeSortExecStream(), params);
}

void ExecStreamFactory::visit(ProxySplitterStreamDef &streamDef)
{
    SplitterExecStreamParams params;
    readExecStreamParams(params, streamDef);
    readTupleDescriptor(params.outputTupleDesc, streamDef.getOutputDesc());
    embryo.init(new SplitterExecStream(), params);
}


void ExecStreamFactory::visit(ProxyValuesStreamDef &streamDef)
{
    ValuesExecStreamParams params;
    readTupleStreamParams(params, streamDef);

    // Get the Java String object so that we can pass it to the decoder.
    jobject tupleBytesBase64 = streamDef.pEnv->CallObjectMethod(
        streamDef.jObject, ProxyValuesStreamDef::meth_getTupleBytesBase64);

    // Call back into Java again to perform the decode.
    jbyteArray jbytes = (jbyteArray) streamDef.pEnv->CallStaticObjectMethod(
        JniUtil::classRhBase64,
        JniUtil::methBase64Decode,
        tupleBytesBase64);

    // Copy the bytes from Java to our tuple buffer.
    params.bufSize = streamDef.pEnv->GetArrayLength(jbytes);
    params.pTupleBuffer.reset(new FixedBuffer[params.bufSize]);
    streamDef.pEnv->GetByteArrayRegion(
        jbytes, 0, params.bufSize,
        reinterpret_cast<jbyte *>(params.pTupleBuffer.get()));
    
    embryo.init(new ValuesExecStream(), params);
}

void ExecStreamFactory::visit(ProxyReshapeStreamDef &streamDef)
{
    ReshapeExecStreamParams params;
    readTupleStreamParams(params, streamDef);

    params.compOp = streamDef.getCompareOp();
    if (params.compOp != COMP_NOOP) {
        // Get the Java String object so that we can pass it to the decoder.
        jobject tupleBytesBase64 = streamDef.pEnv->CallObjectMethod(
            streamDef.jObject,
            ProxyReshapeStreamDef::meth_getTupleCompareBytesBase64);

        // Call back into Java again to perform the decode.
        jbyteArray jbytes = (jbyteArray) streamDef.pEnv->CallStaticObjectMethod(
            JniUtil::classRhBase64,
            JniUtil::methBase64Decode,
            tupleBytesBase64);

        // Copy the bytes from Java to our tuple buffer.
        int bufSize = streamDef.pEnv->GetArrayLength(jbytes);
        params.pCompTupleBuffer.reset(new FixedBuffer[bufSize]);
        streamDef.pEnv->GetByteArrayRegion(
            jbytes, 0, bufSize,
            reinterpret_cast<jbyte *>(params.pCompTupleBuffer.get()));

        CmdInterpreter::readTupleProjection(
            params.inputCompareProj, streamDef.getInputCompareProjection());
    }

    CmdInterpreter::readTupleProjection(
        params.outputProj, streamDef.getOutputProjection());

    SharedProxyReshapeParameter dynamicParam = streamDef.getReshapeParameter();
    for ( ; dynamicParam; ++dynamicParam) {
        int offset = dynamicParam->getCompareOffset();
        ReshapeParameter reshapeParam(
            DynamicParamId(dynamicParam->getDynamicParamId()),
            (offset < 0) ? MAXU : uint(offset),
            dynamicParam->isOutputParam());
        params.dynamicParameters.push_back(reshapeParam);
    }

    embryo.init(new ReshapeExecStream(), params);
}

void ExecStreamFactory::visit(ProxyNestedLoopJoinStreamDef &streamDef)
{
    NestedLoopJoinExecStreamParams params;
    readTupleStreamParams(params, streamDef);
    params.leftOuter = streamDef.isLeftOuter();

    SharedProxyCorrelation dynamicParam = streamDef.getLeftJoinKey();
    for ( ; dynamicParam; ++dynamicParam) {
        NestedLoopJoinKey joinKey(
            DynamicParamId(dynamicParam->getId()),
            dynamicParam->getOffset());
        params.leftJoinKeys.push_back(joinKey);
    }

    embryo.init(new NestedLoopJoinExecStream(), params);
}

void ExecStreamFactory::visit(ProxyBernoulliSamplingStreamDef &streamDef)
{
    BernoulliSamplingExecStreamParams params;
    readTupleStreamParams(params, streamDef);

    params.samplingRate = streamDef.getSamplingRate();
    params.isRepeatable = streamDef.isRepeatable();
    params.repeatableSeed = streamDef.getRepeatableSeed();

    embryo.init(new BernoulliSamplingExecStream(), params);
}


void ExecStreamFactory::readExecStreamParams(
    ExecStreamParams &params,
    ProxyExecutionStreamDef &streamDef)
{
    createQuotaAccessors(params);
}

void ExecStreamFactory::readTupleDescriptor(
    TupleDescriptor& desc,
    SharedProxyTupleDescriptor def)
{
    assert(def);
    CmdInterpreter::readTupleDescriptor(desc, *def, pDatabase->getTypeFactory());
}


void ExecStreamFactory::readTupleStreamParams(
    SingleOutputExecStreamParams &params,
    ProxyTupleStreamDef &streamDef)
{
    readExecStreamParams(params,streamDef);
    readTupleDescriptor(params.outputTupleDesc, streamDef.getOutputDesc());
}

void ExecStreamFactory::createPrivateScratchSegment(ExecStreamParams &params)
{
    // Make sure global scratch segment was already set up.
    assert(params.pCacheAccessor);
    
    params.scratchAccessor =
        pDatabase->getSegmentFactory()->newScratchSegment(
            pDatabase->getCache());
    SharedQuotaCacheAccessor pSuperQuotaAccessor =
        boost::dynamic_pointer_cast<QuotaCacheAccessor>(
            params.pCacheAccessor);
    params.scratchAccessor.pCacheAccessor.reset(
        new QuotaCacheAccessor(
            pSuperQuotaAccessor,
            params.scratchAccessor.pCacheAccessor,
            UINT_MAX));
}

void ExecStreamFactory::createQuotaAccessors(
    ExecStreamParams &params)
{
    assert(pGraphEmbryo);
    pGraphEmbryo->initStreamParams(params);
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
    readBTreeParams(params, streamDef);
}

void ExecStreamFactory::readBTreeParams(
    BTreeParams &params,
    ProxyIndexAccessorDef &streamDef)
{
    params.rootPageIdParamId =
        readDynamicParamId(streamDef.getRootPageIdParamId());
    if (params.rootPageIdParamId > DynamicParamId(0) &&
        streamDef.getRootPageId() == -1)
    {
        // In the case where the btree is dynamically created during
        // runtime, the btree will be created in the temp segment
        params.segmentId = Database::TEMP_SEGMENT_ID;
        params.pageOwnerId = ANON_PAGE_OWNER_ID;
        params.pSegment = pDatabase->getTempSegment();
        params.rootPageId = NULL_PAGE_ID;
        params.pRootMap = NULL;
    } else {
        params.segmentId = SegmentId(streamDef.getSegmentId());
        params.pageOwnerId = PageOwnerId(streamDef.getIndexId());
        assert(VALID_PAGE_OWNER_ID(params.pageOwnerId));
        // Set the btree to read from the appropriate segment, depending
        // on whether or not the reader needs to see uncommitted data
        // created upstream in the stream graph.
        if (streamDef.isReadOnlyCommittedData()) {
            params.pSegment =
                pDatabase->getSegmentById(
                    params.segmentId,
                    pStreamGraphHandle->pReadCommittedSegment);
        } else {
            params.pSegment =
                pDatabase->getSegmentById(
                    params.segmentId,
                    pStreamGraphHandle->pSegment);
        }
        if (streamDef.getRootPageId() != -1) {
            params.rootPageId = PageId(streamDef.getRootPageId());
            params.pRootMap = NULL;
        } else {
            params.rootPageId = NULL_PAGE_ID;
            if (params.rootPageIdParamId == DynamicParamId(0)) {
                params.pRootMap = pStreamGraphHandle;
            }
        }
    }
    readTupleDescriptor(params.tupleDesc, streamDef.getTupleDesc());
    CmdInterpreter::readTupleProjection(
        params.keyProj,
        streamDef.getKeyProj());

}

DynamicParamId ExecStreamFactory::readDynamicParamId(const int val)
{
    // NOTE: zero is a special code for no parameter id
    uint (id) = (val < 0) ? 0 : (uint) val;
    return (DynamicParamId) id;
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

void ExecStreamFactory::readBTreeSearchStreamParams(
    BTreeSearchExecStreamParams &params,
    ProxyIndexSearchDef &streamDef)
{
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
    if (streamDef.getInputDirectiveProj()) {
        CmdInterpreter::readTupleProjection(
            params.inputDirectiveProj,
            streamDef.getInputDirectiveProj());
    }

    SharedProxyCorrelation dynamicParam = streamDef.getSearchKeyParameter();
    for ( ; dynamicParam; ++dynamicParam) {
        BTreeSearchKeyParameter searchKeyParam(
            DynamicParamId(dynamicParam->getId()),
            dynamicParam->getOffset());
        params.searchKeyParams.push_back(searchKeyParam);
    }
}

void ExecStreamFactory::readAggStreamParams(
    SortedAggExecStreamParams &params,
    ProxyAggStreamDef &streamDef)
{
    readTupleStreamParams(params,streamDef);
    SharedProxyAggInvocation pAggInvocation = streamDef.getAggInvocation();
    for (; pAggInvocation; ++pAggInvocation) {
        AggInvocation aggInvocation;
        aggInvocation.aggFunction = pAggInvocation->getFunction();
        aggInvocation.iInputAttr =
            pAggInvocation->getInputAttributeIndex();
        params.aggInvocations.push_back(aggInvocation);
    }
    params.groupByKeyCount = streamDef.getGroupingPrefixSize();
}

ExecStreamSubFactory::~ExecStreamSubFactory()
{
}

FENNEL_END_CPPFILE("$Id$");

// End ExecStreamFactory.cpp
