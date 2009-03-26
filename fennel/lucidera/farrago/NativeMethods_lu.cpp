/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2004-2009 LucidEra, Inc.
// Copyright (C) 2005-2009 The Eigenbase Project
// Portions Copyright (C) 2004-2009 John V. Sichi
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
#include "fennel/farrago/ExecStreamFactory.h"
#include "fennel/lucidera/colstore/LcsClusterAppendExecStream.h"
#include "fennel/lucidera/colstore/LcsClusterReplaceExecStream.h"
#include "fennel/lucidera/colstore/LcsRowScanExecStream.h"
#include "fennel/lucidera/bitmap/LbmGeneratorExecStream.h"
#include "fennel/lucidera/bitmap/LbmSplicerExecStream.h"
#include "fennel/lucidera/bitmap/LbmSearchExecStream.h"
#include "fennel/lucidera/bitmap/LbmChopperExecStream.h"
#include "fennel/lucidera/bitmap/LbmUnionExecStream.h"
#include "fennel/lucidera/bitmap/LbmIntersectExecStream.h"
#include "fennel/lucidera/bitmap/LbmMinusExecStream.h"
#include "fennel/lucidera/bitmap/LbmBitOpExecStream.h"
#include "fennel/lucidera/bitmap/LbmNormalizerExecStream.h"
#include "fennel/lucidera/bitmap/LbmSortedAggExecStream.h"
#include "fennel/db/Database.h"
#include "fennel/segment/SegmentFactory.h"
#include "fennel/exec/ExecStreamEmbryo.h"
#include "fennel/cache/QuotaCacheAccessor.h"

#ifdef __MINGW32__
#include <windows.h>
#endif

FENNEL_BEGIN_CPPFILE("$Id$");

class ExecStreamSubFactory_lu
    : public ExecStreamSubFactory,
        public FemVisitor
{
    ExecStreamFactory *pExecStreamFactory;
    ExecStreamEmbryo *pEmbryo;

    bool created;

    void readClusterScan(
        ProxyLcsRowScanStreamDef &streamDef,
        LcsRowScanBaseExecStreamParams &params)
    {
        SharedProxyLcsClusterScanDef pClusterScan = streamDef.getClusterScan();
        for (; pClusterScan; ++pClusterScan) {
            LcsClusterScanDef clusterScanParam;
            clusterScanParam.pCacheAccessor = params.pCacheAccessor;
            pExecStreamFactory->readBTreeStreamParams(clusterScanParam,
                                                      *pClusterScan);
            pExecStreamFactory->readTupleDescriptor(
                clusterScanParam.clusterTupleDesc,
                pClusterScan->getClusterTupleDesc());
            params.lcsClusterScanDefs.push_back(clusterScanParam);
        }
    }

    // implement FemVisitor
    virtual void visit(ProxyLcsClusterAppendStreamDef &streamDef)
    {
        LcsClusterAppendExecStreamParams params;
        readClusterAppendParams(streamDef, params);

        pEmbryo->init(
            new LcsClusterAppendExecStream(),
            params);
    }

    // implement FemVisitor
    virtual void visit(ProxyLcsClusterReplaceStreamDef &streamDef)
    {
        LcsClusterReplaceExecStreamParams params;
        readClusterAppendParams(streamDef, params);

        pEmbryo->init(
            new LcsClusterReplaceExecStream(),
            params);
    }

    void readClusterAppendParams(
        ProxyLcsClusterAppendStreamDef &streamDef,
        LcsClusterAppendExecStreamParams &params)
    {
        pExecStreamFactory->readTupleStreamParams(params, streamDef);
        pExecStreamFactory->readBTreeStreamParams(params, streamDef);

        // LcsClusterAppendExecStream requires a private ScratchSegment.
        pExecStreamFactory->createPrivateScratchSegment(params);

        CmdInterpreter::readTupleProjection(
            params.inputProj,
            streamDef.getClusterColProj());
    }

    //implement FemVisitor
    virtual void visit(ProxyLcsRowScanStreamDef &streamDef)
    {
        LcsRowScanExecStreamParams params;

        pExecStreamFactory->readTupleStreamParams(params, streamDef);
        readClusterScan(streamDef, params);
        CmdInterpreter::readTupleProjection(params.outputProj,
                                            streamDef.getOutputProj());
        params.isFullScan = streamDef.isFullScan();
        params.hasExtraFilter = streamDef.isHasExtraFilter();

        params.samplingMode = streamDef.getSamplingMode();
        params.samplingRate = streamDef.getSamplingRate();
        params.samplingIsRepeatable = streamDef.isSamplingRepeatable();
        params.samplingRepeatableSeed = streamDef.getSamplingRepeatableSeed();
        params.samplingClumps =
            LcsRowScanExecStreamParams::defaultSystemSamplingClumps;
        params.samplingRowCount = streamDef.getSamplingRowCount();

        CmdInterpreter::readTupleProjection(params.residualFilterCols,
            streamDef.getResidualFilterColumns());
        pEmbryo->init(new LcsRowScanExecStream(), params);
    }

    // implement FemVisitor
    virtual void visit(ProxyLbmGeneratorStreamDef &streamDef)
    {
        LbmGeneratorExecStreamParams params;

        pExecStreamFactory->readTupleStreamParams(params, streamDef);
        pExecStreamFactory->readBTreeStreamParams(params, streamDef);

        // LbmGeneratorExecStream requires a private ScratchSegment.
        pExecStreamFactory->createPrivateScratchSegment(params);

        readClusterScan(streamDef, params);
        CmdInterpreter::readTupleProjection(
            params.outputProj, streamDef.getOutputProj());
        params.insertRowCountParamId =
            pExecStreamFactory->readDynamicParamId(
                streamDef.getInsertRowCountParamId());
        params.createIndex = streamDef.isCreateIndex();

        pEmbryo->init(new LbmGeneratorExecStream(), params);
    }

    // implement FemVisitor
    virtual void visit(ProxyLbmSplicerStreamDef &streamDef)
    {
        LbmSplicerExecStreamParams params;
        pExecStreamFactory->readExecStreamParams(params, streamDef);
        pExecStreamFactory->readTupleDescriptor(
            params.outputTupleDesc,
            streamDef.getOutputDesc());
        SharedProxySplicerIndexAccessorDef pIndexAccessorDef =
            streamDef.getIndexAccessor();
        for (; pIndexAccessorDef; ++pIndexAccessorDef) {
            BTreeExecStreamParams bTreeParams;
            pExecStreamFactory->readBTreeParams(
                bTreeParams,
                *pIndexAccessorDef);
            params.bTreeParams.push_back(bTreeParams);
        }
        params.insertRowCountParamId =
            pExecStreamFactory->readDynamicParamId(
                streamDef.getInsertRowCountParamId());
        params.writeRowCountParamId =
            pExecStreamFactory->readDynamicParamId(
                streamDef.getWriteRowCountParamId());
        params.createNewIndex = streamDef.isCreateNewIndex();
        pEmbryo->init(new LbmSplicerExecStream(), params);
    }

    // implement FemVisitor
    virtual void visit(ProxyLbmSearchStreamDef &streamDef)
    {
        LbmSearchExecStreamParams params;
        pExecStreamFactory->initBTreePrefetchSearchParams(params, streamDef);

        params.rowLimitParamId =
            pExecStreamFactory->readDynamicParamId(
                streamDef.getRowLimitParamId());

        params.startRidParamId =
            pExecStreamFactory->readDynamicParamId(
                streamDef.getStartRidParamId());

        pEmbryo->init(new LbmSearchExecStream(), params);
    }

    // implement FemVisitor
    virtual void visit(ProxyLbmChopperStreamDef &streamDef)
    {
        LbmChopperExecStreamParams params;
        pExecStreamFactory->readTupleStreamParams(params, streamDef);

        params.ridLimitParamId =
            pExecStreamFactory->readDynamicParamId(
                streamDef.getRidLimitParamId());
        pEmbryo->init(new LbmChopperExecStream(), params);
    }

    // implement FemVisitor
    virtual void visit(ProxyLbmUnionStreamDef &streamDef)
    {
        LbmUnionExecStreamParams params;
        pExecStreamFactory->readTupleStreamParams(params, streamDef);

        // LbmUnionExecStream requires a private ScratchSegment.
        pExecStreamFactory->createPrivateScratchSegment(params);

        params.startRidParamId =
            pExecStreamFactory->readDynamicParamId(
                streamDef.getConsumerSridParamId());

        params.segmentLimitParamId =
            pExecStreamFactory->readDynamicParamId(
                streamDef.getSegmentLimitParamId());

        params.ridLimitParamId =
            pExecStreamFactory->readDynamicParamId(
                streamDef.getRidLimitParamId());

        params.maxRid = (LcsRid) 0;

        pEmbryo->init(new LbmUnionExecStream(), params);
    }

    // implement FemVisitor
    virtual void visit(ProxyLbmIntersectStreamDef &streamDef)
    {
        LbmIntersectExecStreamParams params;
        pExecStreamFactory->readTupleStreamParams(params, streamDef);
        readBitOpDynamicParams(streamDef, params);

        pEmbryo->init(new LbmIntersectExecStream(), params);
    }

    virtual void visit(ProxyLbmMinusStreamDef &streamDef)
    {
        LbmMinusExecStreamParams params;
        pExecStreamFactory->readTupleStreamParams(params, streamDef);
        readBitOpDynamicParams(streamDef, params);

        pEmbryo->init(new LbmMinusExecStream(), params);
    }

    void readBitOpDynamicParams(
        ProxyLbmBitOpStreamDef &streamDef, LbmBitOpExecStreamParams &params)
    {
        params.rowLimitParamId =
            pExecStreamFactory->readDynamicParamId(
                streamDef.getRowLimitParamId());
        params.startRidParamId =
            pExecStreamFactory->readDynamicParamId(
                streamDef.getStartRidParamId());
    }

    virtual void visit(ProxyLbmNormalizerStreamDef &streamDef)
    {
        LbmNormalizerExecStreamParams params;
        pExecStreamFactory->readTupleStreamParams(params, streamDef);
        TupleProjection keyProj;
        for (int i = 0; i <  params.outputTupleDesc.size(); i++) {
            keyProj.push_back(i);
        }
        params.keyProj = keyProj;

        pEmbryo->init(new LbmNormalizerExecStream(), params);
    }

    virtual void visit(ProxyLbmSortedAggStreamDef &streamDef)
    {
        LbmSortedAggExecStreamParams params;
        pExecStreamFactory->readAggStreamParams(params, streamDef);
        pEmbryo->init(new LbmSortedAggExecStream(), params);
    }

    // implement JniProxyVisitor
    virtual void unhandledVisit()
    {
        // not a stream type we know about
        created = false;
    }

    // implement ExecStreamSubFactory
    virtual bool createStream(
        ExecStreamFactory &factory,
        ProxyExecutionStreamDef &streamDef,
        ExecStreamEmbryo &embryo)
    {
        pExecStreamFactory = &factory;
        pEmbryo = &embryo;
        created = true;

        // dispatch based on polymorphic stream type
        FemVisitor::visitTbl.accept(*this, streamDef);

        return created;
    }
};

#ifdef __MINGW32__
extern "C" JNIEXPORT BOOL APIENTRY DllMain(
    HANDLE hModule,
    DWORD  ul_reason_for_call,
    LPVOID lpReserved)
{
    return TRUE;
}
#endif

extern "C" JNIEXPORT jint JNICALL
JNI_OnLoad(JavaVM *vm,void *)
{
    JniUtil::initDebug("FENNEL_RS_JNI_DEBUG");
    FENNEL_JNI_ONLOAD_COMMON();
    return JniUtil::jniVersion;
}

extern "C" JNIEXPORT void JNICALL
Java_com_lucidera_farrago_fennel_LucidEraJni_registerStreamFactory(
    JNIEnv *pEnvInit, jclass, jlong hStreamGraph)
{
    JniEnvRef pEnv(pEnvInit);
    try {
        CmdInterpreter::StreamGraphHandle &streamGraphHandle =
            CmdInterpreter::getStreamGraphHandleFromLong(hStreamGraph);
        if (streamGraphHandle.pExecStreamFactory) {
            streamGraphHandle.pExecStreamFactory->addSubFactory(
                SharedExecStreamSubFactory(
                    new ExecStreamSubFactory_lu()));
        }
    } catch (std::exception &ex) {
        pEnv.handleExcn(ex);
    }
}

FENNEL_END_CPPFILE("$Id$");

// End NativeMethods_lu.cpp
