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
#include "fennel/farrago/CmdInterpreter.h"
#include "fennel/farrago/ExecutionStreamFactory.h"
#include "fennel/farrago/JavaTraceTarget.h"

#include "fennel/xo/TupleStreamGraph.h"
#include "fennel/farrago/TupleStreamBuilder.h"
#include "fennel/cache/CacheParams.h"
#include "fennel/common/ConfigMap.h"
#include "fennel/btree/BTreeBuilder.h"
#include "fennel/db/Database.h"
#include "fennel/db/CheckpointThread.h"
#include "fennel/txn/LogicalTxn.h"
#include "fennel/txn/LogicalTxnLog.h"
#include "fennel/xo/TableWriterFactory.h"
#include "fennel/tuple/StoredTypeDescriptorFactory.h"
#include "fennel/segment/SegmentFactory.h"

FENNEL_BEGIN_CPPFILE("$Id$");

int64_t CmdInterpreter::executeCommand(
    ProxyCmd &cmd)
{
    resultHandle = 0;
    // dispatch based on polymorphic command type
    FemVisitor::visitTbl.visit(*this,cmd);
    return resultHandle;
}

CmdInterpreter::DbHandle *CmdInterpreter::getDbHandle(
    SharedProxyDbHandle pHandle)
{
    return reinterpret_cast<DbHandle *>(pHandle->getLongHandle());
}

CmdInterpreter::TxnHandle *CmdInterpreter::getTxnHandle(
    SharedProxyTxnHandle pHandle)
{
    return reinterpret_cast<TxnHandle *>(pHandle->getLongHandle());
}

CmdInterpreter::StreamHandle *CmdInterpreter::getStreamHandle(
    SharedProxyStreamHandle pHandle)
{
    return reinterpret_cast<StreamHandle *>(pHandle->getLongHandle());
}

SavepointId CmdInterpreter::getSavepointId(SharedProxySvptHandle pHandle)
{
    return SavepointId(pHandle->getLongHandle());
}

void CmdInterpreter::setDbHandle(
    SharedProxyDbHandle,DbHandle *pHandle)
{
    resultHandle = reinterpret_cast<int64_t>(pHandle);
}

void CmdInterpreter::setTxnHandle(
    SharedProxyTxnHandle,TxnHandle *pHandle)
{
    resultHandle = reinterpret_cast<int64_t>(pHandle);
}

void CmdInterpreter::setStreamHandle(
    SharedProxyStreamHandle,StreamHandle *pHandle)
{
    resultHandle = reinterpret_cast<int64_t>(pHandle);
}

void CmdInterpreter::setSvptHandle(
    SharedProxySvptHandle,SavepointId svptId)
{
    resultHandle = opaqueToInt(svptId);
}

CmdInterpreter::StreamHandle &CmdInterpreter::getStreamHandleFromObj(
    JniEnvRef pEnv,jobject jHandle)
{
    ProxyStreamHandle streamHandle;
    streamHandle.init(pEnv,jHandle);
    return *reinterpret_cast<StreamHandle *>(streamHandle.getLongHandle());
}

CmdInterpreter::TxnHandle &CmdInterpreter::getTxnHandleFromObj(
    JniEnvRef pEnv,jobject jHandle)
{
    ProxyTxnHandle txnHandle;
    txnHandle.init(pEnv,jHandle);
    return *reinterpret_cast<TxnHandle *>(txnHandle.getLongHandle());
}

void CmdInterpreter::visit(ProxyCmdOpenDatabase &cmd)
{
    ConfigMap configMap;

    SharedProxyDatabaseParam pParam = cmd.getParams();
    for (; pParam; ++pParam) {
        configMap.setStringParam(pParam->getName(),pParam->getValue());
    }

    CacheParams cacheParams;
    cacheParams.readConfig(configMap);
    SharedCache pCache = Cache::newCache(cacheParams);

    DeviceMode openMode = cmd.isCreateDatabase()
        ? DeviceMode::createNew
        : DeviceMode::load;
    
    jobject javaTrace = getObjectFromHandle(cmd.getJavaTraceHandle());

    DbHandle *pDbHandle = new DbHandle();
    ++JniUtil::handleCount;
    pDbHandle->pTraceTarget.reset(new JavaTraceTarget(javaTrace));
    SharedDatabase pDb(
        new Database(
            pCache,
            configMap,
            openMode,
            pDbHandle->pTraceTarget.get()));

    pDbHandle->pDb = pDb;

    pDbHandle->statsTimer.addSource(pDb);
    pDbHandle->statsTimer.start();

    if (pDb->isRecoveryRequired()) {
        SegmentAccessor scratchAccessor =
            pDb->getSegmentFactory()->newScratchSegment(pDb->getCache());
        TableWriterFactory recoveryFactory(
            pDb,
            pDb->getCache(),
            pDb->getTypeFactory(),
            scratchAccessor);
        pDb->recover(recoveryFactory);
    }
    setDbHandle(cmd.getResultHandle(),pDbHandle);
}
    
void CmdInterpreter::visit(ProxyCmdCloseDatabase &cmd)
{
    DbHandle *pDbHandle = getDbHandle(cmd.getDbHandle());

    pDbHandle->statsTimer.stop();
    
    // close database before trace
    pDbHandle->pDb.reset();
    deleteAndNullify(pDbHandle);
    --JniUtil::handleCount;
}
    
void CmdInterpreter::visit(ProxyCmdCheckpoint &cmd)
{
    DbHandle *pDbHandle = getDbHandle(cmd.getDbHandle());

    pDbHandle->pDb->requestCheckpoint(
        cmd.isFuzzy() ? CHECKPOINT_FLUSH_FUZZY : CHECKPOINT_FLUSH_ALL,
        cmd.isAsync());
}
    
void CmdInterpreter::getBTreeForIndexCmd(
    ProxyIndexCmd &cmd,PageId rootPageId,BTreeDescriptor &treeDescriptor)
{
    SharedDatabase pDatabase = getDbHandle(cmd.getDbHandle())->pDb;
    
    ExecutionStreamFactory::readTupleDescriptor(
        treeDescriptor.tupleDescriptor,
        *(cmd.getTupleDesc()),pDatabase->getTypeFactory());
    
    ExecutionStreamFactory::readTupleProjection(
        treeDescriptor.keyProjection,cmd.getKeyProj());

    treeDescriptor.pageOwnerId = PageOwnerId(cmd.getIndexId());
    treeDescriptor.segmentId = SegmentId(cmd.getSegmentId());
    treeDescriptor.segmentAccessor.pSegment =
        pDatabase->getSegmentById(treeDescriptor.segmentId);
    treeDescriptor.segmentAccessor.pCacheAccessor = pDatabase->getCache();
    treeDescriptor.rootPageId = rootPageId;
}

void CmdInterpreter::visit(ProxyCmdCreateIndex &cmd)
{
    // block checkpoints
    SharedDatabase pDb = getDbHandle(cmd.getDbHandle())->pDb;
    SXMutexSharedGuard actionMutexGuard(
        pDb->getCheckpointThread()->getActionMutex());
    
    BTreeDescriptor treeDescriptor;
    getBTreeForIndexCmd(cmd,NULL_PAGE_ID,treeDescriptor);
    BTreeBuilder builder(treeDescriptor);
    builder.createEmptyRoot();
    resultHandle = opaqueToInt(builder.getRootPageId());
}

void CmdInterpreter::visit(ProxyCmdTruncateIndex &cmd)
{
    // block checkpoints
    SharedDatabase pDb = getDbHandle(cmd.getDbHandle())->pDb;
    SXMutexSharedGuard actionMutexGuard(
        pDb->getCheckpointThread()->getActionMutex());
    
    BTreeDescriptor treeDescriptor;
    getBTreeForIndexCmd(cmd,PageId(cmd.getRootPageId()),treeDescriptor);
    BTreeBuilder builder(treeDescriptor);
    builder.truncate(false);
}

void CmdInterpreter::visit(ProxyCmdDropIndex &cmd)
{
    // block checkpoints
    SharedDatabase pDb = getDbHandle(cmd.getDbHandle())->pDb;
    SXMutexSharedGuard actionMutexGuard(
        pDb->getCheckpointThread()->getActionMutex());
    
    BTreeDescriptor treeDescriptor;
    getBTreeForIndexCmd(cmd,PageId(cmd.getRootPageId()),treeDescriptor);
    BTreeBuilder builder(treeDescriptor);
    builder.truncate(true);
}

void CmdInterpreter::visit(ProxyCmdBeginTxn &cmd)
{
    // block checkpoints
    SharedDatabase pDb = getDbHandle(cmd.getDbHandle())->pDb;
    SXMutexSharedGuard actionMutexGuard(
        pDb->getCheckpointThread()->getActionMutex());
    
    TxnHandle *pTxnHandle = new TxnHandle();
    ++JniUtil::handleCount;
    pTxnHandle->pDb = pDb;
    // TODO:  CacheAccessor factory
    pTxnHandle->pTxn = pDb->getTxnLog()->newLogicalTxn(pDb->getCache());
    
    // NOTE:  use a null scratchAccessor; individual TupleStreamGraphs
    // will have their own
    SegmentAccessor scratchAccessor;
    
    pTxnHandle->pTableWriterFactory = SharedTableWriterFactory(
        new TableWriterFactory(
            pDb,
            pDb->getCache(),
            pDb->getTypeFactory(),
            scratchAccessor));
    setTxnHandle(cmd.getResultHandle(),pTxnHandle);
}

void CmdInterpreter::visit(ProxyCmdSavepoint &cmd)
{
    TxnHandle *pTxnHandle = getTxnHandle(cmd.getTxnHandle());
    
    // block checkpoints
    SXMutexSharedGuard actionMutexGuard(
        pTxnHandle->pDb->getCheckpointThread()->getActionMutex());
    
    setSvptHandle(
        cmd.getResultHandle(),
        pTxnHandle->pTxn->createSavepoint());
}

void CmdInterpreter::visit(ProxyCmdCommit &cmd)
{
    TxnHandle *pTxnHandle = getTxnHandle(cmd.getTxnHandle());

    // block checkpoints
    SXMutexSharedGuard actionMutexGuard(
        pTxnHandle->pDb->getCheckpointThread()->getActionMutex());
    
    if (cmd.getSvptHandle()) {
        SavepointId svptId = getSavepointId(cmd.getSvptHandle());
        pTxnHandle->pTxn->commitSavepoint(svptId);
    } else {
        pTxnHandle->pTxn->commit();
        deleteAndNullify(pTxnHandle);
        --JniUtil::handleCount;
    }
}

void CmdInterpreter::visit(ProxyCmdRollback &cmd)
{
    TxnHandle *pTxnHandle = getTxnHandle(cmd.getTxnHandle());

    // block checkpoints
    SXMutexSharedGuard actionMutexGuard(
        pTxnHandle->pDb->getCheckpointThread()->getActionMutex());
    
    if (cmd.getSvptHandle()) {
        SavepointId svptId = getSavepointId(cmd.getSvptHandle());
        pTxnHandle->pTxn->rollback(&svptId);
    } else {
        pTxnHandle->pTxn->rollback();
        deleteAndNullify(pTxnHandle);
        --JniUtil::handleCount;
    }
}

void CmdInterpreter::visit(ProxyCmdPrepareExecutionStreamGraph &cmd)
{
    TxnHandle *pTxnHandle = getTxnHandle(cmd.getTxnHandle());
    SharedTupleStreamGraph pGraph =
        TupleStreamGraph::newTupleStreamGraph();
    pGraph->setTxn(pTxnHandle->pTxn);
    // TODO:  fix leak if excn thrown
    StreamHandle *pStreamHandle = new StreamHandle();
    ++JniUtil::handleCount;
    // TODO: Perhaps stream factory should be singleton
    ExecutionStreamFactory streamFactory(
        pTxnHandle->pDb,
        pTxnHandle->pTableWriterFactory,
        pStreamHandle);
    TupleStreamBuilder streamBuilder(
        pTxnHandle->pDb,
        streamFactory,
        pGraph);
    // TODO jvs 12-Feb-2004: Temporarily, we assume the first stream is the
    // root of a tree.  This assumption will go away once TupleStreamBuilder
    // can handle an arbitrary topology (and then it will take the
    // entire collection, not just one stream).
    streamBuilder.buildStreamGraph(*(cmd.getStreamDefs()));
    pGraph->prepare();
    pStreamHandle->pTupleStreamGraph = pGraph;
    setStreamHandle(cmd.getResultHandle(),pStreamHandle);
}

PageId CmdInterpreter::StreamHandle::getRoot(PageOwnerId pageOwnerId)
{
    JniEnvAutoRef pEnv;
    jlong x = opaqueToInt(pageOwnerId);
    x = pEnv->CallLongMethod(
        javaRuntimeContext,JniUtil::methGetIndexRoot,x);
    return PageId(x);
}

FENNEL_END_CPPFILE("$Id$");

// End CmdInterpreter.cpp
