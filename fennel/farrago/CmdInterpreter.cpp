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
#include "fennel/farrago/CmdInterpreter.h"
#include "fennel/farrago/JavaTraceTarget.h"
#include "fennel/exec/ExecStreamGraphEmbryo.h"
#include "fennel/farrago/ExecStreamBuilder.h"
#include "fennel/cache/CacheParams.h"
#include "fennel/common/ConfigMap.h"
#include "fennel/common/FennelExcn.h"
#include "fennel/common/Backtrace.h"
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

FENNEL_BEGIN_CPPFILE("$Id$");

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

CmdInterpreter::DbHandle* CmdInterpreter::newDbHandle()
{
    return new DbHandle();
}

CmdInterpreter::TxnHandle* CmdInterpreter::newTxnHandle()
{
    return new TxnHandle();
}

CmdInterpreter::DbHandle::~DbHandle()
{
    statsTimer.stop();
    
    // close database before trace
    pDb->close();
    JniUtil::decrementHandleCount(DBHANDLE_TRACE_TYPE_STR, this);

    JniUtil::shutdown();
}
    
CmdInterpreter::TxnHandle::~TxnHandle()
{
    JniUtil::decrementHandleCount(TXNHANDLE_TRACE_TYPE_STR, this);
}
    
CmdInterpreter::StreamGraphHandle::~StreamGraphHandle()
{
    if (javaRuntimeContext) {
        JniEnvAutoRef pEnv;
        pEnv->DeleteGlobalRef(javaRuntimeContext);
    }
    JniUtil::decrementHandleCount(STREAMGRAPHHANDLE_TRACE_TYPE_STR, this);
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

    JniUtilParams jniUtilParams;
    jniUtilParams.readConfig(configMap);
    JniUtil::configure(jniUtilParams);

    DeviceMode openMode = cmd.isCreateDatabase()
        ? DeviceMode::createNew
        : DeviceMode::load;
    
    std::auto_ptr<DbHandle> pDbHandle(newDbHandle());
    JniUtil::incrementHandleCount(DBHANDLE_TRACE_TYPE_STR, pDbHandle.get());

    pDbHandle->pTraceTarget.reset(new JavaTraceTarget());
    // on a fatal error, echo the backtrace to the log file:
    AutoBacktrace::setTraceTarget(pDbHandle->pTraceTarget);

    SharedDatabase pDb = Database::newDatabase(
        pCache,
        configMap,
        openMode,
        pDbHandle->pTraceTarget);

    pDbHandle->pDb = pDb;

    pDbHandle->statsTimer.addSource(pDb);
    pDbHandle->statsTimer.start();

    if (pDb->isRecoveryRequired()) {
        SegmentAccessor scratchAccessor =
            pDb->getSegmentFactory()->newScratchSegment(pDb->getCache());
        FtrsTableWriterFactory recoveryFactory(
            pDb,
            pDb->getCache(),
            pDb->getTypeFactory(),
            scratchAccessor);
        pDb->recover(recoveryFactory);
    }
    setDbHandle(cmd.getResultHandle(),pDbHandle.release());
}
    
void CmdInterpreter::visit(ProxyCmdCloseDatabase &cmd)
{
    DbHandle *pDbHandle = getDbHandle(cmd.getDbHandle());
    AutoBacktrace::setTraceTarget();
    deleteAndNullify(pDbHandle);
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
    // block checkpoints during this method
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
    dropOrTruncateIndex(cmd, false);
}

void CmdInterpreter::visit(ProxyCmdDropIndex &cmd)
{
    dropOrTruncateIndex(cmd, true);
}

void CmdInterpreter::dropOrTruncateIndex(
    ProxyCmdDropIndex &cmd, bool drop)
{
    // block checkpoints during this method
    SharedDatabase pDb = getDbHandle(cmd.getDbHandle())->pDb;
    SXMutexSharedGuard actionMutexGuard(
        pDb->getCheckpointThread()->getActionMutex());
    
    BTreeDescriptor treeDescriptor;
    getBTreeForIndexCmd(cmd,PageId(cmd.getRootPageId()),treeDescriptor);
    TupleProjection leafPageIdProj;
    if (cmd.getLeafPageIdProj()) {
        CmdInterpreter::readTupleProjection(
            leafPageIdProj, cmd.getLeafPageIdProj());
    }
    BTreeBuilder builder(treeDescriptor);
    builder.truncate(drop, leafPageIdProj.size() ? &leafPageIdProj : NULL);
}

void CmdInterpreter::visit(ProxyCmdBeginTxn &cmd)
{
    // block checkpoints during this method
    SharedDatabase pDb = getDbHandle(cmd.getDbHandle())->pDb;
    SXMutexSharedGuard actionMutexGuard(
        pDb->getCheckpointThread()->getActionMutex());

    bool readOnly = cmd.isReadOnly();

    if (!readOnly && pDb->shouldForceTxns()) {
        // We're equating transactions with checkpoints, so take
        // out an extra lock to block checkpoints for the duration
        // of the transaction.  But we don't need to do this for
        // queries because checkpoints only care about dirty
        // persistent pages.
        pDb->getCheckpointThread()->getActionMutex().waitFor(LOCKMODE_S);
    }

    std::auto_ptr<TxnHandle> pTxnHandle(newTxnHandle());
    JniUtil::incrementHandleCount(TXNHANDLE_TRACE_TYPE_STR, pTxnHandle.get());
    pTxnHandle->pDb = pDb;
    pTxnHandle->readOnly = readOnly;
    // TODO:  CacheAccessor factory
    pTxnHandle->pTxn = pDb->getTxnLog()->newLogicalTxn(pDb->getCache());
    
    // NOTE:  use a null scratchAccessor; individual ExecStreamGraphs
    // will have their own
    SegmentAccessor scratchAccessor;
    
    pTxnHandle->pFtrsTableWriterFactory = SharedFtrsTableWriterFactory(
        new FtrsTableWriterFactory(
            pDb,
            pDb->getCache(),
            pDb->getTypeFactory(),
            scratchAccessor));
    setTxnHandle(cmd.getResultHandle(),pTxnHandle.release());
}

void CmdInterpreter::visit(ProxyCmdSavepoint &cmd)
{
    TxnHandle *pTxnHandle = getTxnHandle(cmd.getTxnHandle());
    
    // block checkpoints during this method
    SXMutexSharedGuard actionMutexGuard(
        pTxnHandle->pDb->getCheckpointThread()->getActionMutex());
    
    setSvptHandle(
        cmd.getResultHandle(),
        pTxnHandle->pTxn->createSavepoint());
}

void CmdInterpreter::visit(ProxyCmdCommit &cmd)
{
    TxnHandle *pTxnHandle = getTxnHandle(cmd.getTxnHandle());
    SharedDatabase pDb = pTxnHandle->pDb;

    // block checkpoints during this method
    SXMutexSharedGuard actionMutexGuard(
        pDb->getCheckpointThread()->getActionMutex());
    
    if (cmd.getSvptHandle()) {
        SavepointId svptId = getSavepointId(cmd.getSvptHandle());
        pTxnHandle->pTxn->commitSavepoint(svptId);
    } else {
        pTxnHandle->pTxn->commit();
        bool readOnly = pTxnHandle->readOnly;
        deleteAndNullify(pTxnHandle);
        if (!readOnly && pDb->shouldForceTxns()) {
            // release the checkpoint lock acquired at BeginTxn
            pDb->getCheckpointThread()->getActionMutex().release(
                LOCKMODE_S);
            actionMutexGuard.unlock();
            // force a checkpoint now to flush all data modified by transaction
            // to disk; wait for it to complete before reporting the
            // transaction as committed
            pDb->requestCheckpoint(CHECKPOINT_FLUSH_ALL, false);
        }
    }
}

void CmdInterpreter::visit(ProxyCmdRollback &cmd)
{
    TxnHandle *pTxnHandle = getTxnHandle(cmd.getTxnHandle());
    SharedDatabase pDb = pTxnHandle->pDb;

    // block checkpoints during this method
    SXMutexSharedGuard actionMutexGuard(
        pDb->getCheckpointThread()->getActionMutex());
    
    if (cmd.getSvptHandle()) {
        SavepointId svptId = getSavepointId(cmd.getSvptHandle());
        pTxnHandle->pTxn->rollback(&svptId);
    } else {
        pTxnHandle->pTxn->rollback();
        bool readOnly = pTxnHandle->readOnly;
        deleteAndNullify(pTxnHandle);
        if (!readOnly && pDb->shouldForceTxns()) {
            // implement rollback by simulating crash recovery,
            // reverting all pages modified by transaction

            // TODO jvs 6-Mar-2006:  this actually discards all
            // log pages from the cache, forcing us to re-read them
            // during recoverPhysical.  Keep them around instead.
            pDb->checkpointImpl(CHECKPOINT_DISCARD);
            
            pDb->recoverPhysical();
            // release the checkpoint lock acquired at BeginTxn
            pDb->getCheckpointThread()->getActionMutex().release(
                LOCKMODE_S);
        }
    }
}

void CmdInterpreter::visit(ProxyCmdCreateExecutionStreamGraph &cmd)
{
    TxnHandle *pTxnHandle = getTxnHandle(cmd.getTxnHandle());
    SharedExecStreamGraph pGraph =
        ExecStreamGraph::newExecStreamGraph();
    pGraph->setTxn(pTxnHandle->pTxn);
    std::auto_ptr<StreamGraphHandle> pStreamGraphHandle(
        new StreamGraphHandle());
    JniUtil::incrementHandleCount(
        STREAMGRAPHHANDLE_TRACE_TYPE_STR, pStreamGraphHandle.get());
    pStreamGraphHandle->javaRuntimeContext = NULL;
    pStreamGraphHandle->pTxnHandle = pTxnHandle;
    pStreamGraphHandle->pExecStreamGraph = pGraph;
    pStreamGraphHandle->pExecStreamFactory.reset(
        new ExecStreamFactory(
            pTxnHandle->pDb,
            pTxnHandle->pFtrsTableWriterFactory,
            pStreamGraphHandle.get()));
    setStreamGraphHandle(
        cmd.getResultHandle(),
        pStreamGraphHandle.release());
}

void CmdInterpreter::visit(ProxyCmdPrepareExecutionStreamGraph &cmd)
{
    StreamGraphHandle *pStreamGraphHandle = getStreamGraphHandle(
        cmd.getStreamGraphHandle());
    TxnHandle *pTxnHandle = pStreamGraphHandle->pTxnHandle;
    // NOTE:  sequence is important here
    SharedExecStreamScheduler pScheduler(
        new DfsTreeExecStreamScheduler(
            pTxnHandle->pDb->getSharedTraceTarget(),
            "xo.scheduler"));
    ExecStreamGraphEmbryo graphEmbryo(
        pStreamGraphHandle->pExecStreamGraph,
        pScheduler,
        pTxnHandle->pDb->getCache(),
        pTxnHandle->pDb->getSegmentFactory());
    pStreamGraphHandle->pExecStreamFactory->setGraphEmbryo(graphEmbryo);
    ExecStreamBuilder streamBuilder(
        graphEmbryo, 
        *(pStreamGraphHandle->pExecStreamFactory));
    streamBuilder.buildStreamGraph(cmd, true);
    pStreamGraphHandle->pExecStreamFactory.reset();
    pStreamGraphHandle->pScheduler = pScheduler;
}

void CmdInterpreter::visit(ProxyCmdCreateStreamHandle &cmd)
{
    StreamGraphHandle *pStreamGraphHandle = getStreamGraphHandle(
        cmd.getStreamGraphHandle());
    SharedExecStream pStream;
    if (cmd.isInput()) {
        pStream =
            pStreamGraphHandle->pExecStreamGraph->findLastStream(
            cmd.getStreamName(), 0);
    }
    else {
        pStream =
            pStreamGraphHandle->pExecStreamGraph->findStream(
            cmd.getStreamName());
    }

    setExecStreamHandle(
        cmd.getResultHandle(),
        pStream.get());
}

PageId CmdInterpreter::StreamGraphHandle::getRoot(PageOwnerId pageOwnerId)
{
    JniEnvAutoRef pEnv;
    jlong x = opaqueToInt(pageOwnerId);
    x = pEnv->CallLongMethod(
        javaRuntimeContext,JniUtil::methGetIndexRoot,x);
    return PageId(x);
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
