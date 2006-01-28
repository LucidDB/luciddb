/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2003-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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
#include "fennel/farrago/ExecStreamFactory.h"
#include "fennel/farrago/JavaPullSourceExecStream.h"
#include "fennel/farrago/JavaSinkExecStream.h"
#include "fennel/farrago/CmdInterpreter.h"
#include "fennel/ftrs/BTreeScanExecStream.h"
#include "fennel/ftrs/BTreeSearchExecStream.h"
#include "fennel/ftrs/BTreeSearchUniqueExecStream.h"
#include "fennel/ftrs/FtrsTableWriterExecStream.h"
#include "fennel/ftrs/BTreeSortExecStream.h"
#include "fennel/exec/MergeExecStream.h"
#include "fennel/exec/SegBufferExecStream.h"
#include "fennel/exec/SplitterExecStream.h"
#include "fennel/exec/BarrierExecStream.h"
#include "fennel/exec/ExecStreamGraphEmbryo.h"
#include "fennel/ftrs/FtrsTableWriterFactory.h"
#include "fennel/exec/CartesianJoinExecStream.h"
#include "fennel/exec/SortedAggExecStream.h"
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
    if (streamDef.getInputDirectiveProj()) {
        CmdInterpreter::readTupleProjection(
            params.inputDirectiveProj,
            streamDef.getInputDirectiveProj());
    }
    embryo.init(
        streamDef.isUniqueKey()
        ? new BTreeSearchUniqueExecStream() : new BTreeSearchExecStream(),
        params);
}

void ExecStreamFactory::visit(ProxyJavaTupleStreamDef &streamDef)
{
    JavaPullSourceExecStreamParams params;
    readTupleStreamParams(params, streamDef);
    params.pStreamGraphHandle = pStreamGraphHandle;
    params.javaTupleStreamId = streamDef.getStreamId();
    embryo.init(new JavaPullSourceExecStream(), params);
}

void ExecStreamFactory::visit(ProxyJavaSinkStreamDef &streamDef)
{
    JavaSinkExecStreamParams params;
    readExecStreamParams(params, streamDef);
    params.pStreamGraphHandle = pStreamGraphHandle;
    params.javaFennelPipeIterId = streamDef.getStreamId();
    embryo.init(new JavaSinkExecStream(), params);
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

void ExecStreamFactory::visit(ProxyAggStreamDef &streamDef)
{
    SortedAggExecStreamParams params;
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
    embryo.init(new SortedAggExecStream(), params);
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
    params.leftOuter = streamDef.isLeftOuter();
    embryo.init(new CartesianJoinExecStream(), params);
}

void ExecStreamFactory::visit(ProxyMergeStreamDef &streamDef)
{
    MergeExecStreamParams params;
    readTupleStreamParams(params, streamDef);
    // MergeExecStream doesn't support anything but sequential yet
    assert(streamDef.isSequential());
    embryo.init(new MergeExecStream(), params);
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

void ExecStreamFactory::visit(ProxySplitterStreamDef &streamDef)
{
    SplitterExecStreamParams params;
    readExecStreamParams(params, streamDef);
    embryo.init(new SplitterExecStream(), params);
}

void ExecStreamFactory::visit(ProxyBarrierStreamDef &streamDef)
{
    BarrierExecStreamParams params;
    readTupleStreamParams(params, streamDef);
    embryo.init(new BarrierExecStream(), params);
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
    
    params.segmentId = SegmentId(streamDef.getSegmentId());
    params.pageOwnerId = PageOwnerId(streamDef.getIndexId());
    params.pSegment = pDatabase->getSegmentById(params.segmentId);
    readTupleDescriptor(params.tupleDesc, streamDef.getTupleDesc());
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

ExecStreamSubFactory::~ExecStreamSubFactory()
{
}

FENNEL_END_CPPFILE("$Id$");

// End ExecStreamFactory.cpp
