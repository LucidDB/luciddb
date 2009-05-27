/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Copyright (C) 2005-2009 The Eigenbase Project
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
#include <math.h>

FENNEL_BEGIN_CPPFILE("$Id$");

int32_t LcsRowScanExecStreamParams::defaultSystemSamplingClumps = 10;

LcsRowScanExecStream::LcsRowScanExecStream()
:
    LcsRowScanBaseExecStream(),
    ridRunIter(&ridRuns)
{
    ridRuns.resize(4000);
}

void LcsRowScanExecStream::prepareResidualFilters(
    LcsRowScanExecStreamParams const &params)
{
    nFilters = params.residualFilterCols.size();

    /*
     * compute the outputTupleData position of filter columns
     */
    VectorOfUint valueCols;
    uint j, k = 0;
    for (uint i = 0;  i < nFilters; i++) {
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

    filters.reset(new PLcsResidualColumnFilters[nFilters]);

    for (uint i = 0; i < nClusters; i++) {
        uint clusterEnd = clusterStart +
            params.lcsClusterScanDefs[i].clusterTupleDesc.size() - 1;

        for (uint j = 0; j < nFilters; j++) {
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
    } else {
        nFilters = 0;
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

    /* configure sampling */
    samplingMode = params.samplingMode;

    if (samplingMode != SAMPLING_OFF) {
        samplingRate = params.samplingRate;
        rowCount = params.samplingRowCount;

        if (samplingMode == SAMPLING_BERNOULLI) {
            isSamplingRepeatable = params.samplingIsRepeatable;
            repeatableSeed = params.samplingRepeatableSeed;
            samplingClumps = -1;

            samplingRng.reset(new BernoulliRng(samplingRate));
        } else {
            assert(isFullScan);

            samplingClumps = params.samplingClumps;
            assert(samplingClumps > 0);

            isSamplingRepeatable = false;
        }
    }
}

void LcsRowScanExecStream::open(bool restart)
{
    LcsRowScanBaseExecStream::open(restart);
    producePending = false;
    tupleFound = false;
    nRidsRead = 0;
    ridRunsBuilt = false;
    currRidRun.startRid = LcsRid(MAXU);
    currRidRun.nRids = 0;
    ridRuns.clear();
    ridRunIter.reset();

    if (isFullScan) {
        inputRid = LcsRid(0);
        readDeletedRid = true;
        deletedRidEos = false;
    }
    nextRid = LcsRid(0);
    ridReader.init(inAccessors[0], ridTupleData);

    /*
     * Read from the 1st input, but only if we're not doing a restart.
     * Restarts can reuse the structures set up on the initial open
     * because the current assumption is that the residual filter
     * values don't change in between restarts.  If on restart, if a filter
     * wasn't completely initialized, then reinitialize it.
     */
    if (!restart) {
        iFilterToInitialize = 0;
    } else if (iFilterToInitialize < nFilters) {
        if (!filters[iFilterToInitialize]->filterDataInitialized) {
            filters[iFilterToInitialize]->filterData.clear();
        }
    }

    if (samplingMode == SAMPLING_BERNOULLI) {
        if (isSamplingRepeatable) {
            samplingRng->reseed(repeatableSeed);
        } else if (!restart) {
            samplingRng->reseed(static_cast<uint32_t>(time(0)));
        }
    } else if (samplingMode == SAMPLING_SYSTEM) {
        clumpSize = 0;
        clumpDistance = 0;
        clumpPos = 0;
        numClumpsBuilt = 0;

        initializeSystemSampling();
    }
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
    for (; iFilterToInitialize < nFilters; iFilterToInitialize++) {
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
        filters[iFilterToInitialize]->filterDataInitialized = true;
    }
    return true;
}


void LcsRowScanExecStream::initializeSystemSampling()
{
    clumpPos = 0;
    clumpSkipPos = 0;

    FENNEL_TRACE(TRACE_FINE, "rowCount = " << rowCount);
    FENNEL_TRACE(
        TRACE_FINE, "samplingRate = " << static_cast<double>(samplingRate));

    if (rowCount <= 0) {
        // Handle empty table or non-sense input.
        clumpSize = 1;
        clumpDistance = 0;
        numClumps = 0;
        return;
    }

    // Manipulate this value in a separate member field so we don't
    // mistakenly modify our stored copy of the parameter.
    numClumps = samplingClumps;

    // Compute clump size and distance
    int64_t sampleSize =
        static_cast<uint64_t>(
            round(
                static_cast<double>(rowCount) *
                static_cast<double>(samplingRate)));
    if (sampleSize < numClumps) {
        // Read at least as many rows as there are clumps, even if sample rate
        // is very small.
        sampleSize = numClumps;
    }

    if (sampleSize > rowCount) {
        // samplingRate should be < 1.0, but handle the case where it isn't,
        // or where there are fewer rows than clumps.
        sampleSize = rowCount;
        numClumps = 1;
    }

    FENNEL_TRACE(TRACE_FINE, "sampleSize = " << sampleSize);

    clumpSize =
        static_cast<uint64_t>(
            round(
                static_cast<double>(sampleSize) /
                static_cast<double>(numClumps)));
    assert(sampleSize >= clumpSize);
    assert(clumpSize >= 1);

    FENNEL_TRACE(TRACE_FINE, "clumpSize = " << clumpSize);

    if (numClumps > 1) {
        // Arrange for the last clump to end at the end of the table.
        clumpDistance =
            static_cast<uint64_t>(
                round(
                    static_cast<double>(rowCount - sampleSize) /
                    static_cast<double>(numClumps - 1)));

        // Rounding can cause us to push the final clump past the end of the
        // table.  Avoid this when possible.
        uint64_t rowsRequired =
            (clumpSize + clumpDistance) * (numClumps - 1) + clumpSize;
        if (rowsRequired > rowCount && clumpDistance > 0) {
            clumpDistance--;
        }
    } else {
        // The entire sample will come from the beginning of the table.
        clumpDistance = (rowCount - sampleSize);
    }

    FENNEL_TRACE(TRACE_FINE, "clumpDistance = " << clumpDistance);
}


ExecStreamResult LcsRowScanExecStream::execute(ExecStreamQuantum const &quantum)
{
    if (!initializeFiltersIfNeeded()) {
        return EXECRC_BUF_UNDERFLOW;
    }

    for (uint i = 0; i < quantum.nTuplesMax; i++) {
        uint iClu;
        bool passedFilter;

        while (!producePending) {
            // No need to fill the rid run buffer each time through the loop
            if (!ridRunsBuilt && ridRuns.nFreeSpace() > 100) {
                ExecStreamResult rc = fillRidRunBuffer();
                if (rc != EXECRC_YIELD) {
                    return rc;
                }
            }

            // Determine the rid that needs to be fetched based on the
            // contents of the rid run buffer.
            LcsRid rid =
                LcsClusterReader::getFetchRids(ridRunIter, nextRid, true);
            if (rid == LcsRid(MAXU)) {
                assert(ridRunIter.done());
                pOutAccessor->markEOS();
                return EXECRC_EOS;
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

                // Resync the cluster reader to the current rid position
                pScan->catchUp(ridRunIter.getCurrPos(), nextRid);

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
            // if tuple not found, reached end of table
            if (!tupleFound) {
                pOutAccessor->markEOS();
                return EXECRC_EOS;
            }
        }

        tupleFound = false;
        nRidsRead++;
    }

    return EXECRC_QUANTUM_EXPIRED;
}

ExecStreamResult LcsRowScanExecStream::fillRidRunBuffer()
{
    ExecStreamResult rc;
    RecordNum nRows;

    do {
        if (!isFullScan) {
            rc = ridReader.readRidAndAdvance(inputRid);
            if (rc == EXECRC_EOS) {
                ridRunsBuilt = true;
                break;
            }
            if (rc != EXECRC_YIELD) {
                return rc;
            }
            nRows = 1;

        } else {
            if (!deletedRidEos && readDeletedRid) {
                rc = ridReader.readRidAndAdvance(deletedRid);
                if (rc == EXECRC_EOS) {
                    deletedRidEos = true;
                    if (samplingMode == SAMPLING_OFF) {
                        ridRunsBuilt = true;
                    } else if (samplingMode == SAMPLING_SYSTEM &&
                        numClumps == 0)
                    {
                        ridRunsBuilt = true;
                        break;
                    }
                } else if (rc != EXECRC_YIELD) {
                    return rc;
                } else {
                    readDeletedRid = false;
                }
            }
            // skip over deleted rids
            if (!deletedRidEos && inputRid == deletedRid) {
                inputRid++;
                readDeletedRid = true;
                continue;
            } else {
                if (deletedRidEos) {
                    nRows = MAXU;
                } else {
                    nRows = opaqueToInt(deletedRid - inputRid);
                }
            }
        }

        if (samplingMode != SAMPLING_OFF) {
            if (samplingMode == SAMPLING_SYSTEM) {
                if (clumpSkipPos > 0) {
                    // We need to skip clumpSkipPos RIDs, taking into
                    // account deleted RIDs.  If all deleted RIDs have been
                    // processed (a), we can just skip forward to the next
                    // clump.  If we know the next deleted RID, skip to the
                    // next clump if we can (b), else skip to the deleted
                    // RID (c).  Processing will return here to handle the
                    // remaining clumpSkipPos rows when we reach the next
                    // live RID.  If we don't know the next deleted RID
                    // (d), skip the current live RID, let the deleted RID
                    // processing occur above and then processing will
                    // return here to deal with the remaining clumpSkipPos
                    // rows.
                    if (deletedRidEos) {
                        // (a)
                        inputRid += clumpSkipPos;
                        clumpSkipPos = 0;
                    } else if (!readDeletedRid) {
                        if (deletedRid > inputRid + clumpSkipPos) {
                            // (b)
                            inputRid += clumpSkipPos;
                            clumpSkipPos = 0;
                            nRows = opaqueToInt(deletedRid - inputRid);
                        } else {
                            // (c)
                            clumpSkipPos -= opaqueToInt(deletedRid - inputRid);
                            inputRid = deletedRid;
                            continue;
                        }
                    } else {
                        // (d)
                        clumpSkipPos--;
                        inputRid++;
                        continue;
                    }
                }

                if (nRows >= clumpSize - clumpPos) {
                    // Scale back the size of the rid run based on the
                    // clump size
                    nRows = clumpSize - clumpPos;
                    clumpPos = 0;
                    clumpSkipPos = clumpDistance;
                    if (++numClumpsBuilt == numClumps) {
                        ridRunsBuilt = true;
                    }
                } else {
                    // We only have enough rids for a partial clump
                    clumpPos += nRows;
                }
            } else {
                // Bernoulli sampling
                if (opaqueToInt(inputRid) >= opaqueToInt(rowCount)) {
                    ridRunsBuilt = true;
                    break;
                }
                if (!samplingRng->nextValue()) {
                    inputRid++;
                    continue;
                }
                nRows = 1;
            }
        }

        if (currRidRun.startRid == LcsRid(MAXU)) {
            currRidRun.startRid = inputRid;
            currRidRun.nRids = nRows;
        } else if (currRidRun.startRid + currRidRun.nRids == inputRid) {
            // If the next set of rids is contiguous with the previous,
            // continue adding on to the current run
            if (nRows == RecordNum(MAXU)) {
                currRidRun.nRids = MAXU;
            } else {
                currRidRun.nRids += nRows;
            }
        } else {
            // Otherwise, end the current one
            ridRuns.push_back(currRidRun);

            // And start a new one
            currRidRun.startRid = inputRid;
            currRidRun.nRids = nRows;
        }

        if (isFullScan) {
            inputRid += nRows;
        }
    } while (ridRuns.spaceAvailable() && !ridRunsBuilt);

    // Write out the last run
    if (ridRunsBuilt && currRidRun.startRid != LcsRid(MAXU)) {
        ridRuns.push_back(currRidRun);
    }

    if (ridRunsBuilt) {
        ridRuns.setReadOnly();
    }
    return EXECRC_YIELD;
}

void LcsRowScanExecStream::closeImpl()
{
    LcsRowScanBaseExecStream::closeImpl();

    for (uint i = 0; i < nFilters; i++) {
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
