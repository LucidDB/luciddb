/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2006-2006 LucidEra, Inc.
// Copyright (C) 2006-2006 The Eigenbase Project
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
#include "fennel/lucidera/hashexe/LhxPartition.h"
#include "fennel/lucidera/hashexe/LhxHashGenerator.h"
#include "fennel/exec/ExecStreamBufAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void LhxPartitionWriter::open(
    SharedLhxPartition destPartitionInit,
    LhxJoinInfo const &joinInfo)
{
    destPartition = destPartitionInit;
    tupleAccessor.compute(joinInfo.inputDesc[destPartition->inputIndex]);
    pSegOutputStream = SegOutputStream::newSegOutputStream(
        joinInfo.externalSegmentAccessor);
}

void LhxPartitionWriter::close()
{
    destPartition->firstPageId = pSegOutputStream->getFirstPageId();
    pSegOutputStream.reset();
}

void LhxPartitionWriter::marshalTuple(TupleData const &inputTuple)
{
    uint tupleStorageLength = tupleAccessor.getByteCount(inputTuple);
    PBuffer pDestBuf = pSegOutputStream->getWritePointer(tupleStorageLength);
    tupleAccessor.marshal(inputTuple, pDestBuf);
    pSegOutputStream->consumeWritePointer(tupleStorageLength);    
}

void LhxPartitionReader::open(
    SharedLhxPartition srcPartitionInit,
    LhxJoinInfo const &joinInfo,
    bool setDeallocate)
{
    bufState = EXECBUF_NONEMPTY;
    srcPartition = srcPartitionInit;
    tupleAccessor.compute(joinInfo.inputDesc[srcPartition->inputIndex]);
    
    if (srcPartition->firstPageId == NULL_PAGE_ID) {
        srcIsStream = true;
    } else {
        srcIsStream = false;
    }

    if (srcIsStream) {
        streamBufAccessor = joinInfo.streamBufAccessor[srcPartition->inputIndex];
    } else {
        pSegInputStream = SegInputStream::newSegInputStream(
            joinInfo.externalSegmentAccessor, srcPartition->firstPageId);
        pSegInputStream->startPrefetch();
        /*
         * Once read the disk page can be deallocated.
         */
        pSegInputStream->setDeallocate(setDeallocate);
        tupleAccessor.resetCurrentTupleBuf();        
    }
}

void LhxPartitionReader::close() 
{
    if (srcIsStream) {
        /*
         * Do nothing if reading from stream.
         */
    } else {
        pSegInputStream.reset();
    }
}

void LhxPartitionReader::unmarshalTuple(TupleData &outputTuple)
{
    if (srcIsStream) {
        /*
         * Read from stream.
         */
        streamBufAccessor->unmarshalTuple(outputTuple);
    } else {
        tupleAccessor.unmarshal(outputTuple);
    }
}

void LhxPartitionReader::consumeTuple()
{
    if (srcIsStream) {
        streamBufAccessor->consumeTuple();
    } else {
        tupleAccessor.resetCurrentTupleBuf();
        pSegInputStream->consumeReadPointer(tupleStorageLength);        
    }
}

bool LhxPartitionReader::isTupleConsumptionPending()
{
    if (srcIsStream) {
        return streamBufAccessor->isTupleConsumptionPending();
    } else {
        if (tupleAccessor.getCurrentTupleBuf()) {
            return true;
        } else {
            return false;
        }
    }
}

bool LhxPartitionReader::demandData()
{
    if (srcIsStream) {
        return streamBufAccessor->demandData();
    } else {
        /*
         * Read from disk.
         */
        uint bytesReadable = 0;
        PConstBuffer pSrcBuf = pSegInputStream->getReadPointer(1, &bytesReadable);
        
        /*
         * If readable data does not fill a tuple, it means the segment stream
         * has reached EOD.
         */
        if (!pSrcBuf) {
           bufState = EXECBUF_EOS;
           return false;
        } else {
            tupleStorageLength = tupleAccessor.getBufferByteCount(pSrcBuf);
            if (bytesReadable < tupleStorageLength) {
                /*
                 * REVIEW: when will this happen
                 */
                bufState = EXECBUF_EOS;
                return false;
            } else {
               tupleAccessor.setCurrentTupleBuf(pSrcBuf);
               return true;
            }
        }
    }
}

void LhxPlan::init(
    uint partitionLevelInit,
    uint numChildPartInit,   
    SharedLhxPartition partitionInit0,
    SharedLhxPartition partitionInit1,
    LhxPlan *parentPlanInit)
{
    partitionLevel = partitionLevelInit;

    /*
     * After support is added for repartitioning using stats(gathered during previous round of
     * partitioning), numSubPart can be > numChildPart. A bin-packing
     * algorithm will be used to keep in memory some subpartitions and output
     * the rest to child partitions of similar size.
     * 
     */
    numSubPart   = numChildPart = numChildPartInit;
    partition[0] = partitionInit0;
    partition[1] = partitionInit1;
    parentPlan   = parentPlanInit;
}

void LhxPlan::createChildren(LhxJoinInfo const &joinInfo)
{
    LhxHashGenerator hashSubPart;
    hashSubPart.init(partitionLevel + 1);

    std::vector<SharedLhxPartition> destPartitionList(numChildPart * 2);

    LhxPartitionReader reader;
    LhxPartitionWriter writerList[numChildPart];
    uint childNum, i, j;
    TupleData outputTuple;

    for (j = 0; j < 2; j ++) {
        reader.open(partition[j], joinInfo, true);
        outputTuple.compute(joinInfo.inputDesc[j]);
    
        for (i = 0; i < numChildPart; i ++) {
            uint index = j * numChildPart + i;
            destPartitionList[index].reset(new LhxPartition());
            destPartitionList[index]->inputIndex = j;
            writerList[i].open(destPartitionList[index], joinInfo);
        }
    
        for (;;) {
            if (!reader.isTupleConsumptionPending()) {
                if (reader.getState() == EXECBUF_EOS) {
                    /*
                     * The current plan is completely partitioned.
                     */
                    break;                    
                }
                if (!reader.demandData()) {
                    break;
                }
                reader.unmarshalTuple(outputTuple);
            }
            
            childNum = hashSubPart.hash(outputTuple, joinInfo.keyProj[j]) % numChildPart;

            writerList[childNum].marshalTuple(outputTuple);
            reader.consumeTuple();
        }
    
        for (i = 0; i < numChildPart; i ++) {
            writerList[i].close();
        }
        
        reader.close();
    }

    for (i = 0; i < numChildPart; i ++) {
        SharedLhxPlan newChildPlan = SharedLhxPlan(new LhxPlan());
        newChildPlan->init(
            partitionLevel + 1,
            numChildPart,
            destPartitionList[i],
            destPartitionList[i + numChildPart],
            this);
        if (!firstChildPlan) {
            firstChildPlan = newChildPlan;
        } else {
            firstChildPlan->addSibling(newChildPlan);
        }
    }
}

void LhxPartitionInfo::init(
    uint curInputIndexInit,
    SharedLhxPartition partition,
    uint numChildPart,
    LhxJoinInfo const &joinInfo)
{
    assert (curInputIndexInit == LeftInputIndex);
    curInputIndex = curInputIndexInit;

    reader = &tmpReader;
    reader->open(partition, joinInfo, true);

    for (uint i = 0; i < numChildPart; i ++) {
        uint index = i + curInputIndex * numChildPart;
        destPartitionList.push_back(SharedLhxPartition(new LhxPartition()));
        destPartitionList[index]->inputIndex = curInputIndex;
        writerList.push_back(SharedLhxPartitionWriter(new LhxPartitionWriter()));
        writerList[i]->open(destPartitionList[index], joinInfo);
    }
}

void LhxPartitionInfo::init(
    uint curInputIndexInit,
    SharedLhxPartition partition,
    uint numChildPart,
    LhxJoinInfo const &joinInfo,
    LhxHashTableReader *hashTableReaderInit,
    LhxPartitionReader *rightReader,
    TupleData &rightTupleInit)
{
    assert (curInputIndexInit == RightInputIndex);
    curInputIndex = curInputIndexInit;

    hashTableReader = hashTableReaderInit;
    hashTableReader->bindKey(NULL);    

    /*
     * The right reader is from the LhxJoinExecStream and is already open.
     */
    reader = rightReader;

    /*
     * The inflight(between disk partition and hash table) tuple.
     */
    rightTuple = rightTupleInit;

    for (uint i = 0; i < numChildPart; i ++) {
        uint index = i + curInputIndex * numChildPart;
        destPartitionList.push_back(SharedLhxPartition(new LhxPartition()));
        destPartitionList[index]->inputIndex = curInputIndex;
        writerList.push_back(SharedLhxPartitionWriter(new LhxPartitionWriter()));
        writerList[i]->open(destPartitionList[index], joinInfo);
    }
}

void LhxPartitionInfo::reset(bool resetPartitions)
{
    uint i;

    for (i = 0; i < writerList.size(); i ++) {
        if (writerList[i]) {
            writerList[i]->close();
        }
    }
    writerList.clear();
        
    if (reader) {
        reader->close();
        reader = NULL;
    }

    if (resetPartitions) {
        destPartitionList.clear();
    }
}

LhxPartitionState LhxPlan::generatePartitions(LhxJoinInfo const &joinInfo,
    LhxPartitionInfo &partInfo, bool traceOn)
{
    LhxHashGenerator hashSubPart;
    hashSubPart.init(partitionLevel + 1);

    LhxPartitionReader *reader = partInfo.reader;
    std::vector<SharedLhxPartitionWriter> &writerList = partInfo.writerList;

    uint curInputIndex = partInfo.curInputIndex;

    uint childNum;
    TupleData outputTuple;

    outputTuple.compute(joinInfo.inputDesc[curInputIndex]);
    
    /*
     * If there's hash table to read from.
     */
    if (traceOn) {
        if (curInputIndex == RightInputIndex) {
            dataTrace << "Input [1] [Tuples from Hash Table]\n";
        } else {
            dataTrace << "Input [0]\n";
        }
    }

    if (curInputIndex == RightInputIndex) {
        while ((partInfo.hashTableReader)->getNext(outputTuple)) {

            if (traceOn) {
                tuplePrinter.print(dataTrace, joinInfo.inputDesc[curInputIndex],
                    outputTuple);
                dataTrace <<"\n";
            }

            childNum = hashSubPart.hash(outputTuple,
                joinInfo.keyProj[curInputIndex]) % numChildPart;
            
            writerList[childNum]->marshalTuple(outputTuple);            
        }
        /*
         * The tuple for which hash table full is detected is not in the hash
         * table yet.
         */
        outputTuple = partInfo.rightTuple;
    }

    if (traceOn) {
        dataTrace << "[Tuples from exec stream or disk partition]\n";
    }

    for (;;) {
        if (!reader->isTupleConsumptionPending()) {
            if (reader->getState() == EXECBUF_EOS) {
                /*
                 * The current plan is completely partitioned.
                 */
                return PartitionEndOfData;
            }
            
            if (!reader->demandData()) {
                if (partitionLevel == 0) {
                    /*
                     * Reading from a stream: indicate underflow to producer.
                     */
                    return PartitionUnderflow;
                } else {
                    return PartitionEndOfData;
                }
            }
            
            reader->unmarshalTuple(outputTuple);
        }
        
        if (traceOn) {    
            tuplePrinter.print(dataTrace, joinInfo.inputDesc[curInputIndex],
                outputTuple);
            dataTrace <<"\n";
        }

        childNum = hashSubPart.hash(outputTuple,
            joinInfo.keyProj[curInputIndex]) % numChildPart;
        
        writerList[childNum]->marshalTuple(outputTuple);
        reader->consumeTuple();
    }
}

void LhxPlan::createChildren(LhxPartitionInfo &partInfo)
{
    uint i;

    for (i = 0; i < numChildPart; i ++) {
        SharedLhxPlan newChildPlan = SharedLhxPlan(new LhxPlan());
        newChildPlan->init(
            partitionLevel + 1,
            numChildPart,
            partInfo.destPartitionList[i],
            partInfo.destPartitionList[i + numChildPart],
            this);
        if (!firstChildPlan) {
            firstChildPlan = newChildPlan;
        } else {
            firstChildPlan->addSibling(newChildPlan);
        }
    }
    partInfo.destPartitionList.clear();
}

LhxPlan *LhxPlan::getFirstLeaf()
{
    if (!firstChildPlan) {
        return this;
    } else {
        return firstChildPlan->getFirstLeaf();
    }
}

LhxPlan *LhxPlan::getNextLeaf()
{
    if (siblingPlan) {
        return siblingPlan->getFirstLeaf();
    } else {
        LhxPlan *parent = this->parentPlan;

        if (parent) {
            return parent->getNextLeaf();
        } else {
            return NULL;
        }
    }
}

FENNEL_END_CPPFILE("$Id$");

// End LhxPartition.cpp
