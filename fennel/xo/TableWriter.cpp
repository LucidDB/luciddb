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
#include "fennel/xo/TableWriter.h"
#include "fennel/xo/TableWriterFactory.h"
#include "fennel/btree/BTreeWriter.h"
#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/txn/LogicalTxn.h"
#include "fennel/common/ByteOutputStream.h"
#include "fennel/common/ByteInputStream.h"
#include "fennel/synch/SXMutex.h"

#include <boost/bind.hpp>
#include <numeric>

FENNEL_BEGIN_CPPFILE("$Id$");

const LogicalActionType TableWriter::ACTION_INSERT = 1;

const LogicalActionType TableWriter::ACTION_DELETE = 2;

const LogicalActionType TableWriter::ACTION_UPDATE = 3;

const LogicalActionType TableWriter::ACTION_REVERSE_UPDATE = 4;

TableWriter::TableWriter(TableWriterParams const &params)
{
    updateProj = params.updateProj;
    pClusteredIndexWriter = NULL;
    indexWriters.resize(params.indexParams.size());
    std::transform(
        indexWriters.begin(),
        indexWriters.end(),
        params.indexParams.begin(),
        indexWriters.begin(),
        boost::bind(&TableWriter::createIndexWriter,this,_1,_2));
    assert(pClusteredIndexWriter);

    pTupleData = &(pClusteredIndexWriter->tupleData);
    
    if (!updateProj.empty()) {
        TupleDescriptor tupleDesc =
            pClusteredIndexWriter->pWriter->getTupleDescriptor();
        for (uint i = 0; i < updateProj.size(); ++i) {
            tupleDesc.push_back(tupleDesc[updateProj[i]]);
        }
        tupleAccessor.compute(tupleDesc);
        updateTupleData.compute(tupleDesc);
        pTupleData = &updateTupleData;
    }
    
    nAttrs = pClusteredIndexWriter->tupleData.size();
}

TableIndexWriter &TableWriter::createIndexWriter(
    TableIndexWriter &indexWriter,TableIndexWriterParams const &indexParams)
{
    indexWriter.pWriter = BTreeTupleStream::newWriter(indexParams);
    indexWriter.distinctness = indexParams.distinctness;
    indexWriter.updateInPlace = indexParams.updateInPlace;
    indexWriter.inputProj = indexParams.inputProj;
    indexWriter.pRootMap = indexParams.pRootMap;
    if (indexWriter.inputProj.size()) {
        indexWriter.tupleData.compute(
            indexWriter.pWriter->getTupleDescriptor());
        // TODO:  TupleProjection folding util
        TupleProjection const &keyProj =
            indexWriter.pWriter->getKeyProjection();
        for (uint i = 0; i < keyProj.size(); ++i) {
            indexWriter.inputKeyProj.push_back(
                indexWriter.inputProj[keyProj[i]]);
        }
    } else {
        // TODO:  tuple format?
        
        // this is the clustered index:  its tuple will drive the other indexes
        TupleDescriptor const &clusteredTupleDesc = 
            indexWriter.pWriter->getTupleDescriptor();
        tupleAccessor.compute(clusteredTupleDesc);
        indexWriter.tupleData.compute(clusteredTupleDesc);
        assert(!pClusteredIndexWriter);
        pClusteredIndexWriter = &indexWriter;

        indexWriter.inputKeyProj =
            indexWriter.pWriter->getKeyProjection();
    }
    return indexWriter;
}

inline bool TableWriter::searchForIndexKey(
    TableIndexWriter &indexWriter)
{
    TupleData &keyData = indexWriter.pWriter->getSearchKeyForWrite();
    for (uint i = 0; i < indexWriter.inputKeyProj.size(); ++i) {
        keyData[i] =
            pClusteredIndexWriter->tupleData[indexWriter.inputKeyProj[i]];
    }
    return indexWriter.pWriter->searchForKey(keyData,DUP_SEEK_ANY);
}

inline void TableWriter::insertIntoIndex(
    TableIndexWriter &indexWriter)
{
    for (uint i = 0; i < indexWriter.inputProj.size(); ++i) {
        indexWriter.tupleData[i] =
            pClusteredIndexWriter->tupleData[indexWriter.inputProj[i]];
    }
    if (indexWriter.updateInPlace) {
        if (searchForIndexKey(indexWriter)) {
            if (indexWriter.pWriter->updateCurrent(indexWriter.tupleData)) {
                indexWriter.pWriter->endSearch();
                return;
            }
            // couldn't update in place:  treat as a deletion+insertion instead
            indexWriter.pWriter->deleteCurrent();
            indexWriter.pWriter->endSearch();
        } else {
            // REVIEW:  can this happen?  If so, should we insert?
            assert(false);
        }
    }
    indexWriter.pWriter->insertTupleData(
        indexWriter.tupleData,
        indexWriter.distinctness);
}

inline void TableWriter::deleteFromIndex(
    TableIndexWriter &indexWriter)
{
    if (searchForIndexKey(indexWriter)) {
        // REVIEW:  under what circumstances can we assert when the key doesn't
        // exist?
        indexWriter.pWriter->deleteCurrent();
    }
    indexWriter.pWriter->endSearch();
}

inline void TableWriter::modifySomeIndexes(
    LogicalActionType actionType,
    IndexWriterVector::iterator &first,
    IndexWriterVector::iterator last)
{
    switch(actionType) {
    case ACTION_INSERT:
        for (; first != last; ++first) {
            insertIntoIndex(*first);
        }
        break;
    case ACTION_DELETE:
        for (; first != last; ++first) {
            if (!first->updateInPlace) {
                deleteFromIndex(*first);
            }
        }
        break;
    default:
        assert(false);
        break;
    }
}

inline void TableWriter::modifyAllIndexes(LogicalActionType actionType)
{
    IndexWriterVector::iterator first = indexWriters.begin();
    IndexWriterVector::iterator current = first;
    try {
        modifySomeIndexes(actionType,current,indexWriters.end());
    } catch (...) {
        // In case of exception, carefully roll back only those indexes which
        // were already modified.
        try {
            LogicalActionType compensatingActionType =
                (actionType == ACTION_INSERT) ? ACTION_DELETE : ACTION_INSERT;
            modifySomeIndexes(compensatingActionType,first,current);
        } catch (...) {
            // If this rollback fails, don't allow exception to hide original
            // exception.  But TODO:  trace.
            assert(false);
        }
        throw;
    }
}

inline void TableWriter::copyNewValues()
{
    for (uint i = 0; i < updateProj.size(); ++i) {
        pClusteredIndexWriter->tupleData[updateProj[i]] =
            (*pTupleData)[nAttrs + i];
    }
}

inline void TableWriter::copyOldValues()
{
    for (uint i = 0; i < updateProj.size(); ++i) {
        pClusteredIndexWriter->tupleData[updateProj[i]] =
            (*pTupleData)[updateProj[i]];
    }
}

void TableWriter::executeUpdate(bool reverse)
{
    // copy old values to be deleted
    std::copy(
        pTupleData->begin(),
        pTupleData->begin() + nAttrs,
        pClusteredIndexWriter->tupleData.begin());

    if (reverse) {
        // for reverse, overlay new values instead
        copyNewValues();
    }
    
    modifyAllIndexes(ACTION_DELETE);

    if (reverse) {
        // overlay old values to be inserted
        copyOldValues();
    } else {
        // overlay new values to be inserted
        copyNewValues();
    }
    
    try {
        modifyAllIndexes(ACTION_INSERT);
    } catch (...) {
        // In case of exception while inserting, put back original row.
        try {
            if (reverse) {
                copyNewValues();
            } else {
                copyOldValues();
            }
            modifyAllIndexes(ACTION_INSERT);
        } catch (...) {
            // If this rollback fails, don't allow exception to hide original
            // exception.  But TODO:  trace.
            assert(false);
        }
        throw;
    }
}

inline void TableWriter::executeTuple(LogicalActionType actionType)
{
    switch(actionType) {
    case ACTION_INSERT:
    case ACTION_DELETE:
        modifyAllIndexes(actionType);
        break;
    case ACTION_UPDATE:
        executeUpdate(false);
        break;
    case ACTION_REVERSE_UPDATE:
        executeUpdate(true);
        break;
    default:
        assert(false);
        break;
    }
}

RecordNum TableWriter::execute(
    SharedExecutionStream pInputStream,
    LogicalActionType actionType,
    SXMutex &actionMutex)
{
    // TODO:  assert pInputStream's output tupledesc and format
    // match clustered index

    LogicalTxn *pTxn = getLogicalTxn();
    assert(isLoggingEnabled());

    // block checkpoints while creating savepoint
    SXMutexSharedGuard actionMutexGuard(actionMutex);
    SavepointId svptId = pTxn->createSavepoint();
    actionMutexGuard.unlock();
    
    RecordNum nTuples = 0;
    // TODO:  bulk logging?

    try {
        ByteInputStream &inputResultStream =
            pInputStream->getProducerResultStream();
        for (;;) {
            PConstBuffer pTupleBuf = inputResultStream.getReadPointer(1);
            if (!pTupleBuf) {
                break;
            }
            tupleAccessor.setCurrentTupleBuf(pTupleBuf);
            tupleAccessor.unmarshal(*pTupleData);

            // Block checkpoints for each atomic operation, including
            // execution and logging.  REVIEW:  if lock/unlock overhead is too
            // high per-action, could do it only every so many.
            actionMutexGuard.lock();
            executeTuple(actionType);
            uint cb = tupleAccessor.getCurrentByteCount();
            // NOTE: use getWritePointer rather than writeBytes to ensure that
            // tuples are logged contiguously
            ByteOutputStream &logStream =
                pTxn->beginLogicalAction(*this,actionType);
            PBuffer pLogBuf = logStream.getWritePointer(cb);
            memcpy(pLogBuf,pTupleBuf,cb);
            logStream.consumeWritePointer(cb);
            pTxn->endLogicalAction();
            actionMutexGuard.unlock();
            
            inputResultStream.consumeReadPointer(cb);
            ++nTuples;
        }
    } catch (...) {
        try {
            if (!actionMutexGuard) {
                actionMutexGuard.lock();
            }
            pTxn->rollback(&svptId);
            pTxn->commitSavepoint(svptId);
            actionMutexGuard.unlock();
        } catch (...) {
            // TODO:  trace failed rollback
        }
        throw;
    }

    actionMutexGuard.lock();
    pTxn->commitSavepoint(svptId);
    actionMutexGuard.unlock();
    
    return nTuples;
}

LogicalTxnClassId TableWriter::getParticipantClassId() const
{
    return TableWriterFactory::getParticipantClassId();
}

void TableWriter::describeParticipant(
    ByteOutputStream &logStream)
{
    TupleDescriptor const &clusteredTupleDesc =
        pClusteredIndexWriter->pWriter->getTupleDescriptor();
    clusteredTupleDesc.writePersistent(logStream);
    uint nIndexes = indexWriters.size();
    logStream.writeValue(nIndexes);
    std::for_each(
        indexWriters.begin(),
        indexWriters.end(),
        boost::bind(&TableWriter::describeIndex,this,_1,&logStream));
    updateProj.writePersistent(logStream);
}

void TableWriter::describeIndex(
    TableIndexWriter &indexWriter,
    ByteOutputStream *pLogStream)
{
    pLogStream->writeValue(indexWriter.pWriter->getSegmentId());
    pLogStream->writeValue(indexWriter.pWriter->getPageOwnerId());
    pLogStream->writeValue(indexWriter.pWriter->getRootPageId());
    pLogStream->writeValue(indexWriter.distinctness);
    pLogStream->writeValue(indexWriter.updateInPlace);
    indexWriter.inputProj.writePersistent(*pLogStream);
    indexWriter.pWriter->getKeyProjection().writePersistent(
        *pLogStream);
}

void TableWriter::undoLogicalAction(
    LogicalActionType actionType,
    ByteInputStream &logStream)
{
    switch(actionType) {
    case ACTION_INSERT:
        redoLogicalAction(ACTION_DELETE,logStream);
        break;
    case ACTION_DELETE:
        redoLogicalAction(ACTION_INSERT,logStream);
        break;
    case ACTION_UPDATE:
        redoLogicalAction(ACTION_REVERSE_UPDATE,logStream);
        break;
    default:
        assert(false);
        break;
    }
}

void TableWriter::redoLogicalAction(
    LogicalActionType actionType,
    ByteInputStream &logStream)
{
    // REVIEW:  see comments in SpillOutputStream.cpp regarding page size
    // discrepancies.  For now it just happens to work since no footers are
    // added to log pages, but that could change in the future.
    PConstBuffer pLogBuf = logStream.getReadPointer(1);
    tupleAccessor.setCurrentTupleBuf(pLogBuf);
    uint cb = tupleAccessor.getCurrentByteCount();
    // TODO:  for delete, only need to unmarshal union of keys
    tupleAccessor.unmarshal(*pTupleData);
    executeTuple(actionType);
    logStream.consumeReadPointer(cb);
}

PageOwnerId TableWriter::getTableId()
{
    return pClusteredIndexWriter->pWriter->getPageOwnerId();
}

uint TableWriter::getIndexCount() const
{
    return indexWriters.size();
}

void TableWriter::openIndexWriters()
{
    for (uint i = 0; i < indexWriters.size(); ++i) {
        TableIndexWriter &indexWriter = indexWriters[i];
        if (!indexWriter.pRootMap) {
            continue;
        }
        PageId rootPageId = indexWriter.pRootMap->getRoot(
            indexWriter.pWriter->getPageOwnerId());
        indexWriter.pWriter->setRootPageId(rootPageId);
    }
}

void TableWriter::closeIndexWriters()
{
    for (uint i = 0; i < indexWriters.size(); ++i) {
        TableIndexWriter &indexWriter = indexWriters[i];
        indexWriter.pWriter->releaseScratchBuffers();
        // REVIEW: since this TableWriter may be reused by rollback, we can't
        // fully close it.  But we should find a way to do so at end of
        // transaction.
#if 0
        if (indexWriter.pRootMap) {
            indexWriter.pWriter->setRootPageId(NULL_PAGE_ID);
        }
#endif
    }
}

FENNEL_END_CPPFILE("$Id$");

// End TableWriter.cpp
