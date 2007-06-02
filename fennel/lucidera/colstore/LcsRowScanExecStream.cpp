/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 LucidEra, Inc.
// Copyright (C) 2005-2007 The Eigenbase Project
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
#include "fennel/tuple/StandardTypeDescriptor.h"
#include "fennel/lucidera/colstore/LcsRowScanExecStream.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/common/SearchEndpoint.h"

FENNEL_BEGIN_CPPFILE("$Id$");


void LcsRowScanExecStream::prepareResidualFilters(
    LcsRowScanExecStreamParams const &params)
{

    /*
     * compute the outputTupleData position of filter columns
     */
    std::vector<uint> valueCols;
    uint j, k = 0;
    for (uint i = 0;  i < params.residualFilterCols.size(); i++) {
        for (j = 0; j < params.outputProj.size(); j++) {
            if (params.outputProj[j] == params.residualFilterCols[i]) {
                valueCols.push_back(j);
                break;
            }
        }

        if (j >= params.outputProj.size()) {
            valueCols.push_back(params.outputProj.size() +  k);
            k++;
        }
    }

    /*
     * compute the cluster id and cluster position
     */
    uint valueClus;
    uint clusterPos;
    uint clusterStart = 0;
    uint realClusterStart = 0;

    filters.reset(new 
        PLcsResidualColumnFilters[params.residualFilterCols.size()]);

    for (uint i = 0; i < nClusters; i++) {
        uint clusterEnd = clusterStart +
            params.lcsClusterScanDefs[i].clusterTupleDesc.size() - 1;

        for (uint j = 0; j < params.residualFilterCols.size(); j++) {
            if (params.residualFilterCols[j] >= clusterStart &&
                params.residualFilterCols[j] <= clusterEnd)
            {
                valueClus = i;

                /*
                 * find the position within the cluster
                 */
                for (uint k = 0; k < projMap.size(); k++) {
                    if (projMap[k] == valueCols[j]) {
       
                        clusterPos = k - realClusterStart - 
                            nonClusterCols.size();

                        LcsResidualColumnFilters &filter =
                            pClusters[valueClus]->
                            clusterCols[clusterPos].
                            getFilters();

                        filters[j] = &filter;

                        filter.hasResidualFilters = true;
  
                        filter.readerKeyProj.push_back(valueCols[j]);
                        filter.inputKeyDesc.projectFrom(projDescriptor, 
                            filter.readerKeyProj);
                        filter.attrAccessor.compute(
                            filter.inputKeyDesc[0]);

                        filter.lowerBoundProj.push_back(1);
                        filter.upperBoundProj.push_back(3);
                        filter.readerKeyData.computeAndAllocate(
                            filter.inputKeyDesc);
                        
                        break;
                    }
                }
                // Continue with the same cluster for more filters
            }
        }
        // Look for filters in the next cluster; modify cluster boundaries
        clusterStart = clusterEnd + 1;
        realClusterStart += pClusters[i]->nColsToRead;
    }
}

void LcsRowScanExecStream::prepare(LcsRowScanExecStreamParams const &params)
{
    LcsRowScanBaseExecStream::prepare(params);

    isFullScan = params.isFullScan;
    hasExtraFilter = params.hasExtraFilter;

    // Set up rid bitmap input stream
    ridTupleData.compute(inAccessors[0]->getTupleDesc());
    
    // validate input stream parameters
    TupleDescriptor inputDesc = inAccessors[0]->getTupleDesc();
    assert(inputDesc.size() == 3);
    StandardTypeDescriptorFactory stdTypeFactory;
    TupleAttributeDescriptor expectedRidDesc(
        stdTypeFactory.newDataType(STANDARD_TYPE_RECORDNUM));
    assert(inputDesc[0] == expectedRidDesc);

    assert(hasExtraFilter == (inAccessors.size() > 1));

    if (hasExtraFilter) {
        prepareResidualFilters(params);
    }

    /* 
     * projDescriptor now also includes filter columns 
     */
    for (uint i = 0; i < params.outputProj.size(); i++) {
        outputProj.push_back(i);
    }

    pOutAccessor->setTupleShape(pOutAccessor->getTupleDesc());
    outputTupleData.computeAndAllocate(projDescriptor);

    /* 
     * build the real output accessor 
     * it will be used to unmarshal data into the 
     * real output row: projOutputTuple.
     */
    projOutputTupleData.compute(pOutAccessor->getTupleDesc());

    attrAccessors.resize(projDescriptor.size());
    for (uint i = 0; i < projDescriptor.size(); ++i) {
        attrAccessors[i].compute(projDescriptor[i]);
    }
}

void LcsRowScanExecStream::open(bool restart)
{
    LcsRowScanBaseExecStream::open(restart);
    producePending = false;
    tupleFound = false;
    nRidsRead = 0;
    if (isFullScan) {
        rid = LcsRid(0);
        readDeletedRid = true;
        deletedRidEos = false;
    }
    ridReader.init(inAccessors[0], ridTupleData);

    /*
     * read from the 1st input
     */
    iFilterToInitialize = 0;
}

void LcsRowScanExecStream::getResourceRequirements(
    ExecStreamResourceQuantity &minQuantity,
    ExecStreamResourceQuantity &optQuantity)
{
    LcsRowScanBaseExecStream::getResourceRequirements(minQuantity, optQuantity);
}

bool LcsRowScanExecStream::initializeFiltersIfNeeded()
{
    /*
     * initialize the filters local data
     */
    for (; iFilterToInitialize < inAccessors.size()-1; iFilterToInitialize++) {
        SharedExecStreamBufAccessor &pInAccessor = 
            inAccessors[iFilterToInitialize + 1];
        TupleAccessor &inputAccessor = 
            pInAccessor->getConsumptionTupleAccessor();
        
        if (pInAccessor->getState() != EXECBUF_EOS) {
            PLcsResidualColumnFilters filter = filters[iFilterToInitialize]; 

            while (pInAccessor->demandData()) {
                SharedLcsResidualFilter filterData(new LcsResidualFilter);

                pInAccessor->accessConsumptionTuple();

                /*
                 * Build lower and upper bound data
                 */
                filterData->boundData.compute(pInAccessor->getTupleDesc());
                filterData->boundBuf.reset(
                    new FixedBuffer[inputAccessor.getCurrentByteCount()]);
    
                memcpy(filterData->boundBuf.get(),  
                    pInAccessor->getConsumptionStart(),
                    inputAccessor.getCurrentByteCount());

                /*
                 * inputAccessor is used to unmarshal into boundData.
                 * in order to do this, its current buffer is set to
                 * boundBuf and restored.
                 */
                PConstBuffer tmpBuf;
                tmpBuf = inputAccessor.getCurrentTupleBuf();
                inputAccessor.setCurrentTupleBuf(filterData->boundBuf.get());
                inputAccessor.unmarshal(filterData->boundData);
                inputAccessor.setCurrentTupleBuf(tmpBuf);
      
                /*
                 * record directives.
                 */
                filterData->lowerBoundDirective =
                    SearchEndpoint(*filterData->boundData[0].pData);
                filterData->upperBoundDirective =
                    SearchEndpoint(*filterData->boundData[2].pData);
      
                filter->filterData.push_back(filterData);

                pInAccessor->consumeTuple();
            }
  
            if (pInAccessor->getState() != EXECBUF_EOS) {
                return false;
            }
        }
    } 
    return true;
}


ExecStreamResult LcsRowScanExecStream::execute(ExecStreamQuantum const &quantum)
{
    if (!isFullScan && inAccessors[0]->getState() == EXECBUF_EOS) {
        // Check for input EOS if not full table scan.
        // Full table scan does not have any input.
        pOutAccessor->markEOS();
        return EXECRC_EOS;
    }

    if (!initializeFiltersIfNeeded()) {
        return EXECRC_BUF_UNDERFLOW;
    }

    for (uint i = 0; i < quantum.nTuplesMax; i++) {

        uint iClu;
        bool passedFilter;

        while (!producePending) {
            ExecStreamResult rc;
            if (!isFullScan) {
                rc = ridReader.readRidAndAdvance(rid);
                if (rc == EXECRC_EOS) {
                    pOutAccessor->markEOS();
                    return rc;
                }
                if (rc != EXECRC_YIELD) {
                    return rc;
                }
            } else {
                if (!deletedRidEos && readDeletedRid) {
                    rc = ridReader.readRidAndAdvance(deletedRid);
                    if (rc == EXECRC_EOS) {
                        deletedRidEos = true;
                    } else if (rc != EXECRC_YIELD) {
                        return rc;
                    } else {
                        readDeletedRid = false;
                    }
                }
                // skip over deleted rids
                if (!deletedRidEos && rid == deletedRid) {
                    rid++;
                    readDeletedRid = true;
                    continue;
                }
            }

            uint prevClusterEnd = 0;
            // reset datum pointers, in case previous tuple had nulls
            outputTupleData.resetBuffer();

            // Read the non-cluster columns first
            for (uint j = 0; j < nonClusterCols.size(); j++) {
                if (nonClusterCols[j] == LCS_RID_COLUMN_ID) {
                    memcpy(
                        const_cast<PBuffer>(outputTupleData[projMap[j]].pData),
                        (PBuffer) &rid, sizeof(LcsRid));
                    prevClusterEnd++;
                } else {
                    permAssert(false);
                }
            }

            // Then go through each cluster, forming rows and checking ranges
            for (iClu = 0, passedFilter = true; iClu <  nClusters; iClu++) {

                SharedLcsClusterReader &pScan = pClusters[iClu];

                // if we have not read a batch yet or we've reached the
                // end of a batch, position to the rid we want to read

                if (!pScan->isPositioned() || rid >= pScan->getRangeEndRid()) {
                    bool rc = pScan->position(rid);

                    // rid not found, so just consume the rid and 
                    // continue
                    if (rc == false)
                        break;

                    assert(rid >= pScan->getRangeStartRid()
                           && rid < pScan->getRangeEndRid());

                    // Tell all column scans that the batch has changed.
                    syncColumns(pScan);
                } else {
                    // Should not have moved into previous batch.
                    assert(rid > pScan->getRangeStartRid());

                    // move to correct position within scan; we know we
                    // will not fall off end of batch, so use non-checking
                    // function (for speed)
                    pScan->advanceWithinBatch(
                        opaqueToInt(rid - pScan->getCurrentRid()));
                }

                passedFilter =
                    readColVals(
                        pScan,
                        outputTupleData,
                        prevClusterEnd);
                if (!passedFilter) {
                    break;
                }
                prevClusterEnd += pScan->nColsToRead;
            }

            if (!passedFilter) {
                if (isFullScan) {
                    rid++;
                }
                continue;
            }
            if (iClu == nClusters) {
                tupleFound = true;
            }
            producePending = true;
        }
            
        // produce tuple
        projOutputTupleData.projectFrom(outputTupleData, outputProj);
        if (tupleFound) {
            if (!pOutAccessor->produceTuple(projOutputTupleData)) {
                return EXECRC_BUF_OVERFLOW;
            }
        }
        producePending = false;
        
        if (isFullScan) {
            // if tuple not found, reached end of table,
            // else move to next rid
            if (!tupleFound) {
                pOutAccessor->markEOS();
                return EXECRC_EOS;
            }
            rid++;
        }

        tupleFound = false;
        nRidsRead++;
    }

    return EXECRC_QUANTUM_EXPIRED;
}

void LcsRowScanExecStream::closeImpl()
{
    LcsRowScanBaseExecStream::closeImpl();

    for (uint i = 0; i < inAccessors.size()-1; i++) {
        filters[i]->filterData.clear();
    }
}

void LcsRowScanExecStream::buildOutputProj(TupleProjection &outputProj,
                            LcsRowScanBaseExecStreamParams const &params)
{
    LcsRowScanExecStreamParams const &rowScanParams = 
        dynamic_cast<const LcsRowScanExecStreamParams&>(params);

    /*
     * Build a projection that contains filter columns
     */
    for (uint i = 0; i < rowScanParams.outputProj.size(); i++) {
        outputProj.push_back(rowScanParams.outputProj[i]);
    }
    for (uint i = 0; i < rowScanParams.residualFilterCols.size(); i++) {
        uint j;
        for (j = 0; j < rowScanParams.outputProj.size(); j++) {
            if (rowScanParams.outputProj[j] == 
                rowScanParams.residualFilterCols[i])
            {
                break;
            }
        }
    
        if (j >= rowScanParams.outputProj.size()) {
            outputProj.push_back(rowScanParams.residualFilterCols[i]);
        }
    }
}

FENNEL_END_CPPFILE("$Id$");

// End LcsRowScanExecStream.cpp
