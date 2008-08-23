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
#include "fennel/farrago/CmdInterpreter.h"
#include "fennel/farrago/JavaErrorTarget.h"
#include "fennel/farrago/JavaTraceTarget.h"
#include "fennel/exec/ExecStreamGraphEmbryo.h"
#include "fennel/exec/SimpleExecStreamGovernor.h"
#include "fennel/farrago/ExecStreamBuilder.h"
#include "fennel/cache/CacheParams.h"
#include "fennel/common/ConfigMap.h"
#include "fennel/common/FennelExcn.h"
#include "fennel/common/FennelResource.h"
#include "fennel/common/InvalidParamExcn.h"
#include "fennel/common/Backtrace.h"
#include "fennel/btree/BTreeBuilder.h"
#include "fennel/db/Database.h"
#include "fennel/db/CheckpointThread.h"
#include "fennel/txn/LogicalTxn.h"
#include "fennel/txn/LogicalTxnLog.h"
#include "fennel/tuple/StoredTypeDescriptorFactory.h"
#include "fennel/segment/SegmentFactory.h"
#include "fennel/segment/SnapshotRandomAllocationSegment.h"
#include "fennel/exec/ParallelExecStreamScheduler.h"
#include "fennel/exec/DfsTreeExecStreamScheduler.h"
#include "fennel/exec/ExecStreamGraph.h"
#include "fennel/farrago/ExecStreamFactory.h"
#include "fennel/ftrs/FtrsTableWriterFactory.h"
#include "fennel/btree/BTreeVerifier.h"

#include <boost/lexical_cast.hpp>

#include <malloc.h>

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

TxnId CmdInterpreter::getCsn(SharedProxyCsnHandle pHandle)
{
    return TxnId(pHandle->getLongHandle());
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

void CmdInterpreter::setCsnHandle(
    SharedProxyCsnHandle, TxnId csnId)
{
    resultHandle = opaqueToInt(csnId);
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
    if (pDb) {
        pDb->close();
    }
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
    
JavaTraceTarget *CmdInterpreter::newTraceTarget()
{
    return new JavaTraceTarget();
}

SharedErrorTarget CmdInterpreter::newErrorTarget(
    jobject fennelJavaErrorTarget)
{
    SharedErrorTarget errorTarget;
    errorTarget.reset(new JavaErrorTarget(fennelJavaErrorTarget));
    return errorTarget;
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

    JavaTraceTarget *pJavaTraceTarget = newTraceTarget();
    pDbHandle->pTraceTarget.reset(pJavaTraceTarget);
    // on a fatal error, echo the backtrace to the log file:
    AutoBacktrace::setTraceTarget(pDbHandle->pTraceTarget);

    SharedDatabase pDb;
    try {
        pDb = Database::newDatabase(
            pCache,
            configMap,
            openMode,
            pDbHandle->pTraceTarget,
            SharedPseudoUuidGenerator(new JniPseudoUuidGenerator()));
    } catch (...) {
        AutoBacktrace::setTraceTarget();
        throw;
    }

    pDbHandle->pDb = pDb;

    ExecStreamResourceKnobs knobSettings;
    knobSettings.cacheReservePercentage =
        configMap.getIntParam("cacheReservePercentage");
    knobSettings.expectedConcurrentStatements =
        configMap.getIntParam("expectedConcurrentStatements");

    ExecStreamResourceQuantity resourcesAvailable;
    resourcesAvailable.nCachePages = pCache->getMaxLockedPages();

    pDbHandle->pResourceGovernor =
        SharedExecStreamGovernor(
            new SimpleExecStreamGovernor(
                knobSettings, resourcesAvailable,
                pDbHandle->pTraceTarget,
                "xo.resourceGovernor"));

    pDbHandle->statsTimer.setTarget(*pJavaTraceTarget);
    pDbHandle->statsTimer.addSource(pDb);
    pDbHandle->statsTimer.addSource(pDbHandle->pResourceGovernor);
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

    // Cache initialization may have been unable to allocate the requested
    // number of pages -- check for this case and report it in the log.
    if (pCache->getMaxAllocatedPageCount() != cacheParams.nMemPagesMax ||
        pCache->getAllocatedPageCount() != cacheParams.nMemPagesInit)
    {
        FENNEL_DELEGATE_TRACE(
            TRACE_WARNING, 
            pDb,
            "Unable to allocate "
            << cacheParams.nMemPagesInit 
            << " (of "
            << cacheParams.nMemPagesMax
            << " max) cache pages; allocated "
            << pCache->getAllocatedPageCount()
            << " cache pages.");
    }

    setDbHandle(cmd.getResultHandle(),pDbHandle.release());
}
    
void CmdInterpreter::visit(ProxyCmdCloseDatabase &cmd)
{
    DbHandle *pDbHandle = getDbHandle(cmd.getDbHandle());
    pDbHandle->pResourceGovernor.reset();
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

void CmdInterpreter::visit(ProxyCmdSetParam &cmd)
{
    DbHandle *pDbHandle = getDbHandle(cmd.getDbHandle());
    SharedProxyDatabaseParam pParam = cmd.getParam();

    std::string paramName = pParam->getName();

    if (paramName.compare("cachePagesInit") == 0) {
        int pageCount = boost::lexical_cast<int>(pParam->getValue());
        SharedCache pCache = pDbHandle->pDb->getCache();
        if (pageCount <= 0 || pageCount > pCache->getMaxAllocatedPageCount()) {
            throw InvalidParamExcn("1", "'cachePagesMax'");
        }

        bool decreasingPageCount = pageCount < pCache->getAllocatedPageCount();
        if (decreasingPageCount) {
            // Let governor veto a page count decrease
            ExecStreamResourceQuantity available;
            available.nCachePages = pageCount;
            if (!pDbHandle->pResourceGovernor->setResourceAvailability(
                    available, EXEC_RESOURCE_CACHE_PAGES))
            {
                throw InvalidParamExcn(
                    "the number of pages currently assigned (plus reserve)",
                    "'cachePagesMax'");
            }
        }

        pCache->setAllocatedPageCount(pageCount);

        if (!decreasingPageCount) {
            // Notify governor of increased page count
            ExecStreamResourceQuantity available;
            available.nCachePages = pageCount;
            bool result =
                pDbHandle->pResourceGovernor->setResourceAvailability(
                    available, EXEC_RESOURCE_CACHE_PAGES);
            assert(result);
        }
    } else if (paramName.compare("expectedConcurrentStatements") == 0) {
        int nStatements = boost::lexical_cast<int>(pParam->getValue());
        SharedCache pCache = pDbHandle->pDb->getCache();
        // need to set aside at least 5 pages per statement
        if (nStatements <= 0 ||
            nStatements > pCache->getMaxLockedPages()/5)
        {
            throw InvalidParamExcn("1", "'cachePagesInit/5'");
        }
        ExecStreamResourceKnobs knob;
        knob.expectedConcurrentStatements = nStatements;
        pDbHandle->pResourceGovernor->setResourceKnob(
            knob, EXEC_KNOB_EXPECTED_CONCURRENT_STATEMENTS);

    } else if (paramName.compare("cacheReservePercentage") == 0) {
        int percent = boost::lexical_cast<int>(pParam->getValue());
        if (percent <= 0 || percent >= 99) {
            throw InvalidParamExcn("1", "99");
        }
        ExecStreamResourceKnobs knob;
        knob.cacheReservePercentage = percent;
        if (!pDbHandle->pResourceGovernor->setResourceKnob(
            knob, EXEC_KNOB_CACHE_RESERVE_PERCENTAGE))
        {
            throw InvalidParamExcn(
                "1",
                "a percentage that sets aside fewer pages, to allow for pages already assigned");
        }
    }
}
    
void CmdInterpreter::getBTreeForIndexCmd(
    ProxyIndexCmd &cmd,PageId rootPageId,BTreeDescriptor &treeDescriptor)
{
    TxnHandle *pTxnHandle = getTxnHandle(cmd.getTxnHandle());
    
    readTupleDescriptor(
        treeDescriptor.tupleDescriptor,
        *(cmd.getTupleDesc()),pTxnHandle->pDb->getTypeFactory());
    
    CmdInterpreter::readTupleProjection(
        treeDescriptor.keyProjection,cmd.getKeyProj());

    treeDescriptor.pageOwnerId = PageOwnerId(cmd.getIndexId());
    treeDescriptor.segmentId = SegmentId(cmd.getSegmentId());
    treeDescriptor.segmentAccessor.pSegment =
        pTxnHandle->pDb->getSegmentById(
            treeDescriptor.segmentId,
            pTxnHandle->pSnapshotSegment);
    treeDescriptor.segmentAccessor.pCacheAccessor = pTxnHandle->pDb->getCache();
    treeDescriptor.rootPageId = rootPageId;
}

void CmdInterpreter::visit(ProxyCmdCreateIndex &cmd)
{
    // block checkpoints during this method
    TxnHandle *pTxnHandle = getTxnHandle(cmd.getTxnHandle());
    SXMutexSharedGuard actionMutexGuard(
        pTxnHandle->pDb->getCheckpointThread()->getActionMutex());
    
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

void CmdInterpreter::visit(ProxyCmdVerifyIndex &cmd)
{
    // block checkpoints during this method
    TxnHandle *pTxnHandle = getTxnHandle(cmd.getTxnHandle());
    SXMutexSharedGuard actionMutexGuard(
        pTxnHandle->pDb->getCheckpointThread()->getActionMutex());
    
    BTreeDescriptor treeDescriptor;
    getBTreeForIndexCmd(cmd,PageId(cmd.getRootPageId()),treeDescriptor);
    TupleProjection leafPageIdProj;
    if (cmd.getLeafPageIdProj()) {
        CmdInterpreter::readTupleProjection(
            leafPageIdProj, cmd.getLeafPageIdProj());
    }
    bool estimate = cmd.isEstimate();
    bool includeTuples = cmd.isIncludeTuples();
    bool keys = (!estimate);
    bool leaf = ((!estimate) || includeTuples);
    BTreeVerifier verifier(treeDescriptor);
    verifier.verify(true, keys, leaf);
    BTreeStatistics statistics = verifier.getStatistics();
    long pageCount = statistics.nNonLeafNodes + statistics.nLeafNodes;
    if (includeTuples) {
        pageCount += statistics.nTuples;
    }
    cmd.setResultPageCount(pageCount);

    if (keys) {
        cmd.setResultUniqueKeyCount(statistics.nUniqueKeys);
    } else {
        cmd.clearResultUniqueKeyCount();
    }
}

void CmdInterpreter::dropOrTruncateIndex(
    ProxyCmdDropIndex &cmd, bool drop)
{
    // block checkpoints during this method
    TxnHandle *pTxnHandle = getTxnHandle(cmd.getTxnHandle());
    SXMutexSharedGuard actionMutexGuard(
        pTxnHandle->pDb->getCheckpointThread()->getActionMutex());
    
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
    beginTxn(cmd, cmd.isReadOnly(), NULL_TXN_ID);
}

void CmdInterpreter::beginTxn(ProxyBeginTxnCmd &cmd, bool readOnly, TxnId csn)
{
    assert(readOnly || csn == NULL_TXN_ID);

    // block checkpoints during this method
    DbHandle *pDbHandle = getDbHandle(cmd.getDbHandle());
    SharedDatabase pDb = pDbHandle->pDb;

    SXMutexSharedGuard actionMutexGuard(
        pDb->getCheckpointThread()->getActionMutex());
    
    std::auto_ptr<TxnHandle> pTxnHandle(newTxnHandle());
    JniUtil::incrementHandleCount(TXNHANDLE_TRACE_TYPE_STR, pTxnHandle.get());
    pTxnHandle->pDb = pDb;
    pTxnHandle->readOnly = readOnly;
    // TODO:  CacheAccessor factory
    pTxnHandle->pTxn = pDb->getTxnLog()->newLogicalTxn(pDb->getCache());
    pTxnHandle->pResourceGovernor = pDbHandle->pResourceGovernor;
    
    // NOTE:  use a null scratchAccessor; individual ExecStreamGraphs
    // will have their own
    SegmentAccessor scratchAccessor;
    
    pTxnHandle->pFtrsTableWriterFactory = SharedFtrsTableWriterFactory(
        new FtrsTableWriterFactory(
            pDb,
            pDb->getCache(),
            pDb->getTypeFactory(),
            scratchAccessor));

    if (pDb->areSnapshotsEnabled()) {
        if (csn == NULL_TXN_ID) {
            csn = pTxnHandle->pTxn->getTxnId();
        }
        pTxnHandle->pSnapshotSegment =
            pDb->getSegmentFactory()->newSnapshotRandomAllocationSegment(
                pDb->getDataSegment(),
                pDb->getDataSegment(),
                csn);
    } else {
        assert(csn == NULL_TXN_ID);
    }

    setTxnHandle(cmd.getResultHandle(),pTxnHandle.release());
}

void CmdInterpreter::visit(ProxyCmdBeginTxnWithCsn &cmd)
{
    beginTxn(cmd, true, getCsn(cmd.getCsnHandle()));
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
    bool txnBlocksCheckpoint = !pTxnHandle->readOnly && pDb->shouldForceTxns();
    SXMutexSharedGuard actionMutexGuard(
        pDb->getCheckpointThread()->getActionMutex());
    
    if (pDb->areSnapshotsEnabled()) {
        // Commit the current txn, and start a new one so the versioned
        // pages that we're now going to commit will be marked with a txnId
        // corresponding to the time of the commit.  At present, those pages
        // are marked with a txnId corresponding to the start of the txn.
        pTxnHandle->pTxn->commit();
        pTxnHandle->pTxn = pDb->getTxnLog()->newLogicalTxn(pDb->getCache());
        SnapshotRandomAllocationSegment *pSnapshotSegment =
            SegmentFactory::dynamicCast<SnapshotRandomAllocationSegment *>(
                pTxnHandle->pSnapshotSegment);
        TxnId commitTxnId = pTxnHandle->pTxn->getTxnId();
        pSnapshotSegment->commitChanges(commitTxnId);

        // Flush pages associated with the snapshot segment.  Note that we
        // don't need to flush the underlying versioned segment first since
        // the snapshot pages are all new and therefore, are never logged.
        // Pages in the underlying versioned segment will be flushed in the
        // requestCheckpoint call further below.  Also note that the
        // checkpoint is not initiated through the dynamically cast segment
        // to ensure that the command is traced if tracing is turned on.
        if (txnBlocksCheckpoint) {
            pTxnHandle->pSnapshotSegment->checkpoint(CHECKPOINT_FLUSH_ALL);
        }

        pDb->setLastCommittedTxnId(commitTxnId);
    }

    if (cmd.getSvptHandle()) {
        SavepointId svptId = getSavepointId(cmd.getSvptHandle());
        pTxnHandle->pTxn->commitSavepoint(svptId);
    } else {
        pTxnHandle->pTxn->commit();
        deleteAndNullify(pTxnHandle);
        if (txnBlocksCheckpoint) {
            // release the checkpoint lock acquired above
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
    bool txnBlocksCheckpoint = !pTxnHandle->readOnly && pDb->shouldForceTxns();
    SXMutexSharedGuard actionMutexGuard(
        pDb->getCheckpointThread()->getActionMutex());

    if (pDb->areSnapshotsEnabled()) {
        SnapshotRandomAllocationSegment *pSegment =
            SegmentFactory::dynamicCast<SnapshotRandomAllocationSegment *>(
                pTxnHandle->pSnapshotSegment);
        pSegment->rollbackChanges();
    }

    if (cmd.getSvptHandle()) {
        SavepointId svptId = getSavepointId(cmd.getSvptHandle());
        pTxnHandle->pTxn->rollback(&svptId);
    } else {
        pTxnHandle->pTxn->rollback();
        deleteAndNullify(pTxnHandle);
        if (txnBlocksCheckpoint && !pDb->areSnapshotsEnabled()) {
            // Implement rollback by simulating crash recovery,
            // reverting all pages modified by transaction.  No need
            // to do this when snapshots are in use because no permanent
            // pages were modified.
            pDb->recoverOnline();
        }
    }
}

void CmdInterpreter::visit(ProxyCmdGetTxnCsn &cmd)
{
    TxnHandle *pTxnHandle = getTxnHandle(cmd.getTxnHandle());
    SharedDatabase pDb = pTxnHandle->pDb;
    assert(pDb->areSnapshotsEnabled());
    SnapshotRandomAllocationSegment *pSegment =
        SegmentFactory::dynamicCast<SnapshotRandomAllocationSegment *>(
            pTxnHandle->pSnapshotSegment);
    setCsnHandle(cmd.getResultHandle(), pSegment->getSnapshotCsn());
}

void CmdInterpreter::visit(ProxyCmdGetLastCommittedTxnId &cmd)
{
    DbHandle *pDbHandle = getDbHandle(cmd.getDbHandle());
    SharedDatabase pDb = pDbHandle->pDb;
    setCsnHandle(cmd.getResultHandle(), pDb->getLastCommittedTxnId());
}

void CmdInterpreter::visit(ProxyCmdCreateExecutionStreamGraph &cmd)
{
#if 0
    struct mallinfo minfo = mallinfo();
    std::cout << "Number of allocated bytes before stream graph construction = "
        << minfo.uordblks << " bytes" << std::endl;
#endif
    TxnHandle *pTxnHandle = getTxnHandle(cmd.getTxnHandle());
    SharedDatabase pDb = pTxnHandle->pDb;
    SharedExecStreamGraph pGraph =
        ExecStreamGraph::newExecStreamGraph();
    pGraph->setTxn(pTxnHandle->pTxn);
    pGraph->setResourceGovernor(pTxnHandle->pResourceGovernor);
    std::auto_ptr<StreamGraphHandle> pStreamGraphHandle(
        new StreamGraphHandle());
    JniUtil::incrementHandleCount(
        STREAMGRAPHHANDLE_TRACE_TYPE_STR, pStreamGraphHandle.get());
    pStreamGraphHandle->javaRuntimeContext = NULL;
    pStreamGraphHandle->pTxnHandle = pTxnHandle;
    pStreamGraphHandle->pExecStreamGraph = pGraph;
    pStreamGraphHandle->pExecStreamFactory.reset(
        new ExecStreamFactory(
            pDb,
            pTxnHandle->pFtrsTableWriterFactory,
            pStreamGraphHandle.get()));
    // When snapshots are enabled, allocate a DynamicDelegatingSegment for the
    // stream graph so if the stream graph is executed in different txns,
    // we can reset the delegate to whatever is the snapshot segment associated
    // with the current txn.
    if (pDb->areSnapshotsEnabled()) {
        pStreamGraphHandle->pSegment =
            pDb->getSegmentFactory()->newDynamicDelegatingSegment(
                pTxnHandle->pSnapshotSegment);
    }
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
    SharedExecStreamScheduler pScheduler;
    std::string schedulerName = "xo.scheduler";
    if (cmd.getDegreeOfParallelism() == 1) {
        pScheduler.reset(
            new DfsTreeExecStreamScheduler(
                pTxnHandle->pDb->getSharedTraceTarget(),
                schedulerName));
    } else {
        pScheduler.reset(
            new ParallelExecStreamScheduler(
                pTxnHandle->pDb->getSharedTraceTarget(),
                schedulerName,
                JniUtil::getThreadTracker(),
                cmd.getDegreeOfParallelism()));
    }
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
#if 0
    struct mallinfo minfo = mallinfo();
    std::cout << "Number of allocated bytes after stream graph construction = "
        << minfo.uordblks << " bytes" << std::endl;
#endif
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

void CmdInterpreter::visit(ProxyCmdAlterSystemDeallocate &cmd)
{
    DbHandle *pDbHandle = getDbHandle(cmd.getDbHandle());
    SharedDatabase pDb = pDbHandle->pDb;
    if (!pDb->areSnapshotsEnabled()) {
        // Nothing to do if snapshots aren't enabled
        return;
    } else {
        uint64_t paramVal = cmd.getOldestLabelCsn();
        TxnId labelCsn = isMAXU(paramVal) ? NULL_TXN_ID : TxnId(paramVal);
        pDb->deallocateOldPages(labelCsn);
    }
}

void CmdInterpreter::visit(ProxyCmdVersionIndexRoot &cmd)
{
    TxnHandle *pTxnHandle = getTxnHandle(cmd.getTxnHandle());
    SharedDatabase pDb = pTxnHandle->pDb;
    assert(pDb->areSnapshotsEnabled());

    SnapshotRandomAllocationSegment *pSnapshotSegment =
        SegmentFactory::dynamicCast<SnapshotRandomAllocationSegment *>(
            pTxnHandle->pSnapshotSegment);
    pSnapshotSegment->versionPage(
        PageId(cmd.getOldRootPageId()),
        PageId(cmd.getNewRootPageId()));
}

FENNEL_END_CPPFILE("$Id$");

// End CmdInterpreter.cpp
