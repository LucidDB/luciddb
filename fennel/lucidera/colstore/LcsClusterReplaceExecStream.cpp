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
#include "fennel/lucidera/colstore/LcsClusterReplaceExecStream.h"
#include "fennel/lucidera/colstore/LcsClusterReader.h"
#include "fennel/segment/SegmentFactory.h"
#include "fennel/btree/BTreeBuilder.h"
#include "fennel/exec/ExecStreamBufAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void LcsClusterReplaceExecStream::prepare(
    LcsClusterReplaceExecStreamParams const &params)
{
    LcsClusterAppendExecStream::prepare(params);
    newClusterRootParamId = params.rootPageIdParamId;

    // Save the original root pageId at prepare time because the treeDescriptor
    // will be reset at open time with the new cluster's rootPageId
    origRootPageId = treeDescriptor.rootPageId;
}

void LcsClusterReplaceExecStream::initTupleLoadParams(
    const TupleProjection &inputProj)
{
    numColumns = inputProj.size() - 1;

    projInputTupleDesc.projectFrom(tableColsTupleDesc, inputProj);
    projInputTupleData.compute(projInputTupleDesc);

    // Setup the cluster reader to read all columns from the original cluster
    // without any pre-fetch
    //
    // TODO - Extend this class to use pre-fetches when reading from the
    // original cluster.  This will require reading ahead from the input
    // stream to detect gaps in the rid values and then setting up rid runs
    // for each block of missing rids.
    pOrigClusterReader =
        SharedLcsClusterReader(new LcsClusterReader(treeDescriptor));
    TupleProjection proj;
    proj.resize(numColumns);
    for (uint i = 0; i < numColumns; i++) {
        proj[i] = i;
    }
    pOrigClusterReader->initColumnReaders(numColumns, proj);

    // Setup the objects for accessing just the cluster columns by excluding
    // the rid column
    std::copy(inputProj.begin() + 1, inputProj.end(), proj.begin());
    TupleAccessor &inputAccessor = pInAccessor->getConsumptionTupleAccessor();
    clusterColsTupleAccessor.bind(inputAccessor, proj);
    clusterColsTupleDesc.projectFrom(pInAccessor->getTupleDesc(), proj);
    clusterColsTupleData.compute(clusterColsTupleDesc);

    attrAccessors.resize(clusterColsTupleDesc.size());
    for (uint i = 0; i < clusterColsTupleDesc.size(); i++) {
        attrAccessors[i].compute(clusterColsTupleDesc[i]);
    }

    origClusterTupleData.computeAndAllocate(clusterColsTupleDesc);

    // setup one tuple descriptor per cluster column
    colTupleDesc.reset(new TupleDescriptor[numColumns]);
    for (int i = 0; i < numColumns; i++) {
        // +1 to skip over the rid column
        colTupleDesc[i].push_back(tableColsTupleDesc[inputProj[i + 1]]);
    }
}

void LcsClusterReplaceExecStream::getResourceRequirements(
    ExecStreamResourceQuantity &minQuantity,
    ExecStreamResourceQuantity &optQuantity)
{
    LcsClusterAppendExecStream::getResourceRequirements(
        minQuantity,
        optQuantity);

    // Need to allocate two more pages for the cluster reader that reads
    // original cluster values -- one for the rid to pageId btree and another
    // for the actual cluster page.
    minQuantity.nCachePages += 2;

    optQuantity = minQuantity;
}

void LcsClusterReplaceExecStream::open(bool restart)
{
    newData = false;

    // Need to call this after the setup above because the cluster append
    // stream depends on the new cluster being in place
    LcsClusterAppendExecStream::open(restart);

    // Determine how many rows are in the original cluster
    origNumRows = pOrigClusterReader->getNumRows();

    if (!restart) {
        // Save the root pageId in a dynamic parameter so it can be read
        // downstream, if a parameter is specified
        if (opaqueToInt(newClusterRootParamId) > 0) {
            pDynamicParamManager->createParam(
                newClusterRootParamId,
                pInAccessor->getTupleDesc()[0]);
        }

        // Retrieve the snapshot segment.  This needs to be done at open time
        // because the segment changes across transaction boundaries.
        pSnapshotSegment =
            SegmentFactory::getSnapshotSegment(
                treeDescriptor.segmentAccessor.pSegment);
        assert(pSnapshotSegment != NULL);
    }

    if (opaqueToInt(newClusterRootParamId) > 0) {
        TupleDatum rootPageIdDatum;
        rootPageIdDatum.pData = (PConstBuffer) &(treeDescriptor.rootPageId);
        rootPageIdDatum.cbData = sizeof(treeDescriptor.rootPageId);
        pDynamicParamManager->writeParam(
            newClusterRootParamId,
            rootPageIdDatum);
    }

    pOrigClusterReader->open();
    currLoadRid = LcsRid(0);
    currInputRid = LcsRid(MAXU);
    needTuple = true;
}

ExecStreamResult LcsClusterReplaceExecStream::getTupleForLoad()
{
    // If the last tuple provided has not been processed yet, then there's no
    // work to be done
    if (!needTuple) {
        return EXECRC_YIELD;
    }

    if (pInAccessor->getState() == EXECBUF_EOS) {
        // No more input rows, but that doesn't mean we're finished because
        // we have to match the number of rows in the original cluster.
        // Therefore, if there's a gap at the end of the cluster, read the
        // original rows until we read the rid corresponding to the last tuple
        // tuple in the original cluster, at which point, we can finally
        // say that we're done.  However, if there wasn't at least one new
        // row, then there's no need to replace the column.  We can simply
        // keep the original.
        if (!newData) {
            return EXECRC_EOS;
        }
        if (opaqueToInt(currLoadRid) < origNumRows) {
            readOrigClusterRow();
            needTuple = false;
            // in case this wasn't already called
            initLoad();
            return EXECRC_YIELD;
        } else {
            pSnapshotSegment->versionPage(
                origRootPageId,
                treeDescriptor.rootPageId);
            return EXECRC_EOS;
        }
    }

    if (!pInAccessor->demandData()) {
        return EXECRC_BUF_UNDERFLOW;
    }

    // Create a new rid to pageId btree map for this cluster, once we know
    // at least one row is being updated
    if (!newData) {
        treeDescriptor.rootPageId = NULL_PAGE_ID;
        BTreeBuilder builder(
            treeDescriptor,
            treeDescriptor.segmentAccessor.pSegment);
        builder.createEmptyRoot();
        treeDescriptor.rootPageId = builder.getRootPageId();
        newData = true;
    }

    initLoad();

    if (currLoadRid == LcsRid(0) || currLoadRid > currInputRid) {
        assert(!pInAccessor->isTupleConsumptionPending());
        pInAccessor->unmarshalProjectedTuple(projInputTupleData);
        currInputRid =
            *reinterpret_cast<LcsRid const *> (projInputTupleData[0].pData);
    }

    // If there's a gap between the last input tuple read and the
    // current row that needs to be loaded, then read the original
    // cluster data; otherwise, unmarshal the last input row read.
    if (currInputRid > currLoadRid) {
        readOrigClusterRow();
    } else {
        assert(currInputRid == currLoadRid);
        clusterColsTupleAccessor.unmarshal(clusterColsTupleData);
    }

    needTuple = false;
    return EXECRC_YIELD;
}

void LcsClusterReplaceExecStream::readOrigClusterRow()
{
    origClusterTupleData.resetBuffer();

    // Position to the current rid we want to load.  Then read each of the
    // column values, load them into the TupleDataWithBuffer, and then copy
    // those TupleDatum's into the TupleData that's used to load the
    // cluster.
    bool needSync = true;
    if (pOrigClusterReader->isPositioned()
        && currLoadRid < pOrigClusterReader->getRangeEndRid())
    {
        needSync = false;
    }
    bool rc = pOrigClusterReader->position(currLoadRid);
    assert(rc);
    for (uint i = 0; i < pOrigClusterReader->nColsToRead; i++) {
        if (needSync) {
            pOrigClusterReader->clusterCols[i].sync();
        }
        PBuffer colValue = pOrigClusterReader->clusterCols[i].getCurrentValue();
        attrAccessors[i].loadValue(origClusterTupleData[i], colValue);
        clusterColsTupleData[i] = origClusterTupleData[i];
    }
}

void LcsClusterReplaceExecStream::postProcessTuple()
{
    // Consume the current input tuple if we've completed processing that
    // input.
    if (currInputRid == currLoadRid) {
        LcsClusterAppendExecStream::postProcessTuple();
    }
    currLoadRid++;
    needTuple = true;
}

void LcsClusterReplaceExecStream::close()
{
    LcsClusterAppendExecStream::close();
    pOrigClusterReader->close();
}

FENNEL_END_CPPFILE("$Id$");

// End LcsClusterReplaceExecStream.cpp
