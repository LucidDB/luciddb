/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2004-2005 LucidEra, Inc.
// Copyright (C) 2005-2005 The Eigenbase Project
// Portions Copyright (C) 2004-2005 John V. Sichi
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
#include "fennel/lucidera/sorter/ExternalSortExecStream.h"
#include "fennel/lucidera/flatfile/FlatFileExecStream.h"
#include "fennel/lucidera/colstore/LcsClusterAppendExecStream.h"
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
#include "fennel/lucidera/hashexe/LhxJoinExecStream.h"
#include "fennel/lucidera/hashexe/LhxAggExecStream.h"
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
    
    char readCharParam(const std::string &val)
    {
        assert(val.size() <= 1);
        if (val.size() == 0) {
            return 0;
        }
        return val.at(0);
    }

    DynamicParamId readDynamicParamId(const int val)
    {
        // NOTE: zero is a special code for no parameter id
        uint id = (val < 0) ? 0 : (uint) val;
        return (DynamicParamId) id;
    }

    void readClusterScan(
        ProxyLcsRowScanStreamDef &streamDef,
        LcsRowScanBaseExecStreamParams &params)
    {
        SharedProxyLcsClusterScanDef pClusterScan = streamDef.getClusterScan();
        for ( ; pClusterScan; ++pClusterScan) {
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
    virtual void visit(ProxySortingStreamDef &streamDef)
    {
        if (streamDef.getDistinctness() != DUP_ALLOW) {
            // can't handle it
            created = false;
            return;
        }

        SharedDatabase pDatabase = pExecStreamFactory->getDatabase();
        
        ExternalSortExecStreamParams params;

        pExecStreamFactory->readTupleStreamParams(params, streamDef);
        
        // ExternalSortStream requires a private ScratchSegment.
        pExecStreamFactory->createPrivateScratchSegment(params);
        
        params.distinctness = streamDef.getDistinctness();
        params.pTempSegment = pDatabase->getTempSegment();
        params.storeFinalRun = false;
        CmdInterpreter::readTupleProjection(
            params.keyProj,
            streamDef.getKeyProj());
        pEmbryo->init(
            ExternalSortExecStream::newExternalSortExecStream(),
            params);
    }

    // implement FemVisitor
    virtual void visit(ProxyFlatFileTupleStreamDef &streamDef)
    {
        FlatFileExecStreamParams params;
        pExecStreamFactory->readTupleStreamParams(params, streamDef);

        assert(streamDef.getDataFilePath().size() > 0);
        params.dataFilePath = streamDef.getDataFilePath();
        params.errorFilePath = streamDef.getErrorFilePath();
        params.fieldDelim = readCharParam(streamDef.getFieldDelimiter());
        params.rowDelim = readCharParam(streamDef.getRowDelimiter());
        params.quoteChar = readCharParam(streamDef.getQuoteCharacter());
        params.escapeChar = readCharParam(streamDef.getEscapeCharacter());
        params.header = streamDef.isHasHeader();
        
        params.numRowsScan = streamDef.getNumRowsScan();
        params.calcProgram = streamDef.getCalcProgram();
        if (params.numRowsScan > 0 && params.calcProgram.size() > 0) {
            params.mode = FLATFILE_MODE_SAMPLE;
        } else if (params.numRowsScan > 0) {
            params.mode = FLATFILE_MODE_DESCRIBE;
        } else if (params.numRowsScan == 0 && params.calcProgram.size() == 0) {
            params.mode = FLATFILE_MODE_QUERY_TEXT;
        }
        pEmbryo->init(FlatFileExecStream::newFlatFileExecStream(), params);
    }

    // implement FemVisitor
    virtual void visit(ProxyLcsClusterAppendStreamDef &streamDef)
    {
        LcsClusterAppendExecStreamParams params;
        pExecStreamFactory->readTupleStreamParams(params, streamDef);
        pExecStreamFactory->readBTreeStreamParams(params, streamDef);
        
        // LcsClusterAppendExecStream requires a private ScratchSegment.
        pExecStreamFactory->createPrivateScratchSegment(params);

        params.overwrite = streamDef.isOverwrite();
        
        CmdInterpreter::readTupleProjection(
            params.inputProj,
            streamDef.getClusterColProj());

        pEmbryo->init(
            new LcsClusterAppendExecStream(),
            params);
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
        params.dynParamId =
            readDynamicParamId(streamDef.getRowCountParamId());
        params.createIndex = streamDef.isCreateIndex();

        pEmbryo->init(new LbmGeneratorExecStream(), params);
    }

    // implement FemVisitor
    virtual void visit(ProxyLbmSplicerStreamDef &streamDef)
    {
        LbmSplicerExecStreamParams params;
        pExecStreamFactory->readTupleStreamParams(params, streamDef);
        pExecStreamFactory->readBTreeStreamParams(params, streamDef);
        params.dynParamId =
            readDynamicParamId(streamDef.getRowCountParamId());
        pEmbryo->init(new LbmSplicerExecStream(), params);
    }

    // implement FemVisitor
    virtual void visit(ProxyLbmSearchStreamDef &streamDef)
    {
        LbmSearchExecStreamParams params;
        pExecStreamFactory->readBTreeSearchStreamParams(params, streamDef);

        params.rowLimitParamId =
            readDynamicParamId(streamDef.getRowLimitParamId());

        params.startRidParamId =
            readDynamicParamId(streamDef.getStartRidParamId());

        pEmbryo->init(new LbmSearchExecStream(), params);
    }

    // implement FemVisitor
    virtual void visit(ProxyLbmChopperStreamDef &streamDef)
    {
        LbmChopperExecStreamParams params;
        pExecStreamFactory->readTupleStreamParams(params, streamDef);

        params.ridLimitParamId =
            readDynamicParamId(streamDef.getRidLimitParamId());
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
            readDynamicParamId(streamDef.getConsumerSridParamId());

        params.segmentLimitParamId =
            readDynamicParamId(streamDef.getSegmentLimitParamId());

        params.ridLimitParamId =
            readDynamicParamId(streamDef.getRidLimitParamId());

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
            readDynamicParamId(streamDef.getRowLimitParamId());
        params.startRidParamId =
            readDynamicParamId(streamDef.getStartRidParamId());
    }
    
    // implement FemVisitor
    virtual void visit(ProxyLhxJoinStreamDef &streamDef)
    {
        TupleProjection tmpProj;

        LhxJoinExecStreamParams params;
        pExecStreamFactory->readTupleStreamParams(params, streamDef);

        /*
         * LhxJoinExecStream requires a private ScratchSegment.
         */
        pExecStreamFactory->createPrivateScratchSegment(params);

        /*
         * External segment to store partitions.
         */
        SharedDatabase pDatabase = pExecStreamFactory->getDatabase();
        params.pTempSegment = pDatabase->getTempSegment();

        /*
         * These fields are currently not used by the optimizer. We know that
         * optimizer only supports inner equi hash join.
         */
        params.leftInner     = streamDef.isLeftInner();
        params.leftOuter     = streamDef.isLeftOuter();
        params.rightInner    = streamDef.isRightInner();
        params.rightOuter    = streamDef.isRightOuter();        
        params.setopDistinct = streamDef.isSetopDistinct();
        params.setopAll      = streamDef.isSetopAll();

        /*
         * Set forcePartitionLevel to 0 to turn off force partitioning.
         */
        params.forcePartitionLevel = 0;
        params.enableJoinFilter    = true;
        params.enableSubPartStat   = true;
        params.enableSwing         = true;

        CmdInterpreter::readTupleProjection(
            params.leftKeyProj, streamDef.getLeftKeyProj());

        CmdInterpreter::readTupleProjection(
            params.rightKeyProj, streamDef.getRightKeyProj());

        /*
         * The optimizer currently estimates these two values.
         */
        params.cndKeys = streamDef.getCndBuildKeys();
        params.numRows = streamDef.getNumBuildRows();

        pEmbryo->init(new LhxJoinExecStream(), params);
    }

    virtual void visit(ProxyLhxAggStreamDef &streamDef)
    {
        LhxAggExecStreamParams params;
        pExecStreamFactory->readAggStreamParams(params, streamDef);

        /*
         * LhxAggExecStream requires a private ScratchSegment.
         */
        pExecStreamFactory->createPrivateScratchSegment(params);

        /*
         * External segment to store partitions.
         */
        SharedDatabase pDatabase = pExecStreamFactory->getDatabase();
        params.pTempSegment = pDatabase->getTempSegment();

        /*
         * The optimizer currently estimates these two values.
         */
        params.cndGroupByKeys = streamDef.getCndGroupByKeys();
        params.numRows = streamDef.getNumRows();

        /*
         * Set forcePartitionLevel to 0 to turn off force partitioning.
         */
        params.forcePartitionLevel = 0;

        /*
         * NOTE:
         * Hash aggregation partitions partially aggregated results to disk.
         * The stat currently keeps track of the tuple count before
         * aggregation, so it is not very accurate. Disable sub partition stats
         * for now.
         */
        params.enableSubPartStat   = false;

        pEmbryo->init(new LhxAggExecStream(), params);
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

FENNEL_END_CPPFILE("$Id: //open/dt/dev/fennel/lucidera/farrago/NativeMethods_lu.cpp#22 $");

// End NativeMethods_lu.cpp
