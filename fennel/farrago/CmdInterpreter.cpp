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
#include "fennel/farrago/CmdInterpreter.h"
#include "fennel/farrago/JavaTraceTarget.h"
#include "fennel/farrago/ExecStreamBuilder.h"
#include "fennel/cache/CacheParams.h"
#include "fennel/common/ConfigMap.h"
#include "fennel/common/FennelExcn.h"
#include "fennel/btree/BTreeBuilder.h"
#include "fennel/db/Database.h"
#include "fennel/db/CheckpointThread.h"
#include "fennel/txn/LogicalTxn.h"
#include "fennel/txn/LogicalTxnLog.h"
#include "fennel/tuple/StoredTypeDescriptorFactory.h"
#include "fennel/segment/SegmentFactory.h"
#include "fennel/exec/DfsTreeExecStreamScheduler.h"
#include "fennel/exec/ExecStreamGraph.h"
#include "fennel/farrago/ExecStreamFactory.h"
#include "fennel/ftrs/FtrsTableWriterFactory.h"

// DEPRECATED
#include "fennel/xo/TableWriterFactory.h"
#include "fennel/xo/TupleStreamGraph.h"
#include "fennel/farrago/ExecutionStreamFactory.h"
#include "fennel/farrago/ExecutionStreamBuilder.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void *CmdInterpreter::getLeafPtr()
{
    return static_cast<FemVisitor *>(this);
}

const char *CmdInterpreter::getLeafTypeName()
{
    return "FemVisitor";
}

int64_t CmdInterpreter::executeCommand(
    ProxyCmd &cmd)
{
    resultHandle = 0;
    // dispatch based on polymorphic command type
    FemVisitor::visitTbl.accept(*this,cmd);
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

CmdInterpreter::StreamGraphHandle *CmdInterpreter::getStreamGraphHandle(
    SharedProxyStreamGraphHandle pHandle)
{
    return reinterpret_cast<StreamGraphHandle *>(pHandle->getLongHandle());
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

void CmdInterpreter::setStreamGraphHandle(
    SharedProxyStreamGraphHandle,StreamGraphHandle *pHandle)
{
    resultHandle = reinterpret_cast<int64_t>(pHandle);
}

void CmdInterpreter::setStreamHandle(
    SharedProxyStreamHandle,ExecutionStream *pStream)
{
    resultHandle = reinterpret_cast<int64_t>(pStream);
}

void CmdInterpreter::setExecStreamHandle(
    SharedProxyStreamHandle,ExecStream *pStream)
{
    resultHandle = reinterpret_cast<int64_t>(pStream);
}

void CmdInterpreter::setSvptHandle(
    SharedProxySvptHandle,SavepointId svptId)
{
    resultHandle = opaqueToInt(svptId);
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
    
    jobject javaTrace = getObjectFromLong(cmd.getJavaTraceHandle());

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
        // NOTE jvs 10-Aug-2004 -- the if (false) branch below is the real
        // recovery code.  It's currently disabled because MDR recovery isn't
        // working yet.  So for now, once we detect a crash we fail fast.
        if (false) {
            SegmentAccessor scratchAccessor =
                pDb->getSegmentFactory()->newScratchSegment(pDb->getCache());
            FtrsTableWriterFactory recoveryFactory(
                pDb,
                pDb->getCache(),
                pDb->getTypeFactory(),
                scratchAccessor);
            pDb->recover(recoveryFactory);
        } else {
            deleteDbHandle(pDbHandle);
            // NOTE jvs 10-Aug-2004 -- this message is intentionally NOT
            // internationalized because it's supposed to be temporary.
            throw FennelExcn(
                "Database crash detected.  "
                "To repair system, you must restore the catalog from backup.");
        }
    }
    setDbHandle(cmd.getResultHandle(),pDbHandle);
}
    
void CmdInterpreter::visit(ProxyCmdCloseDatabase &cmd)
{
    DbHandle *pDbHandle = getDbHandle(cmd.getDbHandle());
    deleteDbHandle(pDbHandle);
}

void CmdInterpreter::deleteDbHandle(DbHandle *pDbHandle)
{
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
    
    readTupleDescriptor(
        treeDescriptor.tupleDescriptor,
        *(cmd.getTupleDesc()),pDatabase->getTypeFactory());
    
    CmdInterpreter::readTupleProjection(
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
    
    // NOTE:  use a null scratchAccessor; individual ExecStreamGraphs
    // will have their own
    SegmentAccessor scratchAccessor;
    
    // DEPRECATED
    pTxnHandle->pTableWriterFactory = SharedTableWriterFactory(
        new TableWriterFactory(
            pDb,
            pDb->getCache(),
            pDb->getTypeFactory(),
            scratchAccessor));
    
    pTxnHandle->pFtrsTableWriterFactory = SharedFtrsTableWriterFactory(
        new FtrsTableWriterFactory(
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

void CmdInterpreter::visit(ProxyCmdCreateExecutionStreamGraph &cmd)
{
    TxnHandle *pTxnHandle = getTxnHandle(cmd.getTxnHandle());
    if (JniUtil::isUsingOldScheduler()) {
        // DEPRECATED
        SharedTupleStreamGraph pGraph =
            TupleStreamGraph::newTupleStreamGraph();
        pGraph->setTxn(pTxnHandle->pTxn);
        TupleStreamGraphHandle *pStreamGraphHandle =
            new TupleStreamGraphHandle();
        ++JniUtil::handleCount;
        pStreamGraphHandle->pTxnHandle = pTxnHandle;
        pStreamGraphHandle->setTupleStreamGraph(pGraph);
        pStreamGraphHandle->pStreamFactory.reset(
            new ExecutionStreamFactory(
                pTxnHandle->pDb,
                pTxnHandle->pTableWriterFactory,
                pStreamGraphHandle));
        setStreamGraphHandle(cmd.getResultHandle(),pStreamGraphHandle);
    } else {
        SharedExecStreamGraph pGraph =
            ExecStreamGraph::newExecStreamGraph();
        pGraph->setTxn(pTxnHandle->pTxn);
        StreamGraphHandle *pStreamGraphHandle =
            new StreamGraphHandle();
        ++JniUtil::handleCount;
        pStreamGraphHandle->pTxnHandle = pTxnHandle;
        pStreamGraphHandle->pExecStreamGraph = pGraph;
        pStreamGraphHandle->pExecStreamFactory.reset(
            new ExecStreamFactory(
                pTxnHandle->pDb,
                pTxnHandle->pFtrsTableWriterFactory,
                pStreamGraphHandle));
        setStreamGraphHandle(cmd.getResultHandle(),pStreamGraphHandle);
    }
}

void CmdInterpreter::visit(ProxyCmdPrepareExecutionStreamGraph &cmd)
{
    StreamGraphHandle *pStreamGraphHandle = getStreamGraphHandle(
        cmd.getStreamGraphHandle());
    TxnHandle *pTxnHandle = pStreamGraphHandle->pTxnHandle;
    if (JniUtil::isUsingOldScheduler()) {
        // DEPRECATED
        ExecutionStreamBuilder streamBuilder(
            pTxnHandle->pDb,
            *(pStreamGraphHandle->pStreamFactory),
            pStreamGraphHandle->getGraph());
        streamBuilder.buildStreamGraph(cmd);
        pStreamGraphHandle->pStreamFactory.reset();
    } else {
        // NOTE:  sequence is important here
        SharedExecStreamScheduler pScheduler(
            new DfsTreeExecStreamScheduler(
                &(pTxnHandle->pDb->getTraceTarget()),
                "xo.scheduler"));
        ExecStreamBuilder streamBuilder(
            pTxnHandle->pDb,
            pScheduler,
            *(pStreamGraphHandle->pExecStreamFactory),
            pStreamGraphHandle->pExecStreamGraph);
        streamBuilder.buildStreamGraph(cmd);
        pStreamGraphHandle->pExecStreamFactory.reset();
        pScheduler->addGraph(pStreamGraphHandle->pExecStreamGraph);
        pScheduler->start();
        pStreamGraphHandle->pScheduler = pScheduler;
    }
}

void CmdInterpreter::visit(ProxyCmdCreateStreamHandle &cmd)
{
    StreamGraphHandle *pStreamGraphHandle = getStreamGraphHandle(
        cmd.getStreamGraphHandle());
    if (JniUtil::isUsingOldScheduler()) {
        // DEPRECATED
        SharedExecutionStream pStream =
            pStreamGraphHandle->getGraph()->findLastStream(
                cmd.getStreamName());
        setStreamHandle(
            cmd.getResultHandle(),
            pStream.get());
    } else {
        SharedExecStream pStream =
            pStreamGraphHandle->pExecStreamGraph->findLastStream(
                cmd.getStreamName());
        setExecStreamHandle(
            cmd.getResultHandle(),
            pStream.get());
    }
}

PageId CmdInterpreter::StreamGraphHandle::getRoot(PageOwnerId pageOwnerId)
{
    JniEnvAutoRef pEnv;
    jlong x = opaqueToInt(pageOwnerId);
    x = pEnv->CallLongMethod(
        javaRuntimeContext,JniUtil::methGetIndexRoot,x);
    return PageId(x);
}

void CmdInterpreter::TupleStreamGraphHandle::setTupleStreamGraph(
    SharedTupleStreamGraph pGraphIn)
{
    pGraph = pGraphIn;
}

SharedExecutionStreamGraph CmdInterpreter::StreamGraphHandle::getGraph()
{
    return SharedExecutionStreamGraph();
}

SharedExecutionStreamGraph CmdInterpreter::TupleStreamGraphHandle::getGraph()
{
    return pGraph;
}

SharedTupleStreamGraph 
CmdInterpreter::TupleStreamGraphHandle::getTupleStreamGraph()
{
    return pGraph;
}

void CmdInterpreter::readTupleDescriptor(
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

void CmdInterpreter::readTupleProjection(
    TupleProjection &tupleProj,
    SharedProxyTupleProjection pJavaTupleProj)
{
    tupleProj.clear();
    SharedProxyTupleAttrProjection pAttr = pJavaTupleProj->getAttrProjection();
    for (; pAttr; ++pAttr) {
        tupleProj.push_back(pAttr->getAttributeIndex());
    }
}

FENNEL_END_CPPFILE("$Id$");

// End CmdInterpreter.cpp
