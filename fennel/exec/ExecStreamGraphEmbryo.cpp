/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 2004-2004 John V. Sichi.
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
#include "fennel/exec/ExecStreamGraphEmbryo.h"
#include "fennel/exec/ExecStreamGraph.h"
#include "fennel/exec/ExecStream.h"
#include "fennel/exec/ExecStreamEmbryo.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/exec/ExecStreamScheduler.h"
#include "fennel/cache/QuotaCacheAccessor.h"
#include "fennel/segment/SegmentAccessor.h"
#include "fennel/segment/SegmentFactory.h"
#include "fennel/cache/Cache.h"
#include <iostream>

FENNEL_BEGIN_CPPFILE("$Id$");

inline std::string toStr(std::ostream const &ostr) 
{
    return static_cast<std::ostringstream const &>(ostr).str();
}

ExecStreamGraphEmbryo::ExecStreamGraphEmbryo(
    SharedExecStreamGraph pGraphInit, 
    SharedExecStreamScheduler pSchedulerInit, 
    SharedCache pCacheInit,
    SharedSegmentFactory pSegmentFactoryInit, 
    bool enforceCacheQuotasInit)
{
    pGraph = pGraphInit;
    pScheduler = pSchedulerInit;
    pCacheAccessor = pCacheInit;
    scratchAccessor =
        pSegmentFactoryInit->newScratchSegment(pCacheInit);
    enforceCacheQuotas = enforceCacheQuotasInit;
    
    pGraph->setScratchSegment(scratchAccessor.pSegment);
}

ExecStreamGraphEmbryo::~ExecStreamGraphEmbryo()
{
}

SharedExecStream ExecStreamGraphEmbryo::addAdapterFor(
    const std::string &name,
    uint iOutput,
    ExecStreamBufProvision requiredDataflow)
{
    // REVIEW jvs 18-Nov-2004:  in the case of multiple outputs from one
    // stream, with consumers having different provisioning, this
    // could result in chains of adapters, which would be less than optimal
    
    // Get available dataflow from last stream of group
    SharedExecStream pLastStream = pGraph->findLastStream(name, iOutput);
    ExecStreamBufProvision availableDataflow =
        pLastStream->getOutputBufProvision();
    assert(availableDataflow != BUFPROV_NONE);

    // Generate a name.
    std::string adapterName;
    {
        int id = pGraph->getOutputCount(pLastStream->getStreamId());
        std::ostringstream oss;
        oss << pLastStream->getName() << "#" << id << ".provisioner";
        adapterName = oss.str();
    }

    // If necessary, create an adapter based on the last stream
    switch (requiredDataflow) {
    case BUFPROV_CONSUMER:
        if (availableDataflow == BUFPROV_PRODUCER) {
            ExecStreamEmbryo embryo;
            pScheduler->createCopyProvisionAdapter(embryo);
            initializeAdapter(embryo, name, iOutput, adapterName);
            return embryo.getStream();
        }
        break;
    case BUFPROV_PRODUCER:
        if (availableDataflow == BUFPROV_CONSUMER) {
            ExecStreamEmbryo embryo;
            pScheduler->createBufferProvisionAdapter(embryo);
            initializeAdapter(embryo, name, iOutput, adapterName);
            return embryo.getStream();
        }
        break;
    default:
        permAssert(false);
    }
    return pLastStream;
}

void ExecStreamGraphEmbryo::initializeAdapter(
    ExecStreamEmbryo &embryo,
    std::string const &streamName,
    uint iOutput,
    std::string const &adapterName)
{
    initStreamParams(*(embryo.getParams()));
    embryo.getStream()->setName(adapterName);
    saveStreamEmbryo(embryo);
    pGraph->interposeStream(
        streamName, iOutput, embryo.getStream()->getStreamId());
}

void ExecStreamGraphEmbryo::saveStreamEmbryo(ExecStreamEmbryo &embryo)
{
    allStreamEmbryos[embryo.getStream()->getName()] = embryo;
    pGraph->addStream(embryo.getStream());
}

ExecStreamEmbryo &ExecStreamGraphEmbryo::getStreamEmbryo(
    std::string const &name)
{
    StreamMapIter pPair = allStreamEmbryos.find(name);
    assert(pPair != allStreamEmbryos.end());
    return pPair->second;
}

void ExecStreamGraphEmbryo::addDataflow(
    const std::string &source,
    const std::string &target)
{
    SharedExecStream pSourceStream = 
        pGraph->findStream(source);
    SharedExecStream pTargetStream = 
        pGraph->findStream(target);
    ExecStreamBufProvision requiredDataflow =
        pTargetStream->getInputBufProvision();
    uint iOutput = pGraph->getOutputCount(pSourceStream->getStreamId());
    addAdapterFor(source, iOutput, requiredDataflow);
    SharedExecStream pInput = 
        pGraph->findLastStream(source, iOutput);
    pGraph->addDataflow(
        pInput->getStreamId(),
        pTargetStream->getStreamId());
}

void ExecStreamGraphEmbryo::initStreamParams(ExecStreamParams &params)
{
    params.pCacheAccessor = pCacheAccessor;
    params.scratchAccessor = scratchAccessor;
    if (!enforceCacheQuotas) {
        params.enforceQuotas = false;
        return;
    }
    
    params.enforceQuotas = true;
    // All cache access should be wrapped by quota checks.  Actual
    // quotas will be set per-execution.
    uint quota = 0;
    SharedQuotaCacheAccessor pQuotaAccessor(
        new QuotaCacheAccessor(
            SharedQuotaCacheAccessor(),
            params.pCacheAccessor,
            quota));
    params.pCacheAccessor = pQuotaAccessor;

    // scratch access has to go through a separate CacheAccessor, but
    // delegates quota checking to pQuotaAccessor
    params.scratchAccessor.pCacheAccessor.reset(
        new QuotaCacheAccessor(
            pQuotaAccessor,
            params.scratchAccessor.pCacheAccessor,
            quota));
}

ExecStreamGraph &ExecStreamGraphEmbryo::getGraph()
{
    return *pGraph;
}

SegmentAccessor &ExecStreamGraphEmbryo::getScratchAccessor()
{
    return scratchAccessor;
}

void ExecStreamGraphEmbryo::prepareGraph(
    TraceTarget *pTraceTarget,
    std::string const &tracePrefix)
{
    pGraph->prepare(*pScheduler);
    std::vector<SharedExecStream> sortedStreams = 
        pGraph->getSortedStreams();
    std::vector<SharedExecStream>::iterator pos;
    for (pos = sortedStreams.begin(); pos != sortedStreams.end(); pos++) {
        std::string name = (*pos)->getName();
        ExecStreamEmbryo &embryo = getStreamEmbryo(name);
        // Give streams a source name with an XO prefix so that users can 
        // choose to trace XOs as a group
        std::string traceName = tracePrefix + name;
        ExecStreamId streamId = embryo.getStream()->getStreamId();
        embryo.getStream()->initTraceSource(
            pTraceTarget,
            traceName);
        embryo.prepareStream();

        // Check that stream remembered to initialize its outputs.
        uint outputCount = pGraph->getOutputCount(streamId);
        for (uint i = 0; i < outputCount; ++i) {
            SharedExecStreamBufAccessor outAccessor = 
                pGraph->getStreamOutputAccessor(streamId, i);
            if (outAccessor->getTupleDesc().empty()) {
                permFail("Forgot to initialize output #" << i << "of stream '"
                         << traceName << "'");
            }
        }
    }

    pScheduler->addGraph(pGraph);
}

FENNEL_END_CPPFILE("$Id$");

// End ExecStreamGraphEmbryo.cpp
