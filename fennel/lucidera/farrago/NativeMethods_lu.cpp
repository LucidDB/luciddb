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
#include "fennel/lucidera/bitmap/LbmIndexScanExecStream.h"
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
        
        // ExternalSortStream requires a private ScratchSegment.  It's very
        // important to do this AFTER readTupleStreamParams, otherwise the
        // private one gets reset to the ScratchSegment shared across the whole
        // graph, and then quite bad things happen.
        params.scratchAccessor =
            pDatabase->getSegmentFactory()->newScratchSegment(
                pDatabase->getCache());

        // TODO jvs 9-Feb-2006:  call factory to get help with
        // setting up quota for private scratch segment
        SharedQuotaCacheAccessor pSuperQuotaAccessor =
            boost::dynamic_pointer_cast<QuotaCacheAccessor>(
                params.pCacheAccessor);
        params.scratchAccessor.pCacheAccessor.reset(
            new QuotaCacheAccessor(
                pSuperQuotaAccessor,
                params.scratchAccessor.pCacheAccessor,
                UINT_MAX));
        
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
        }
        pEmbryo->init(FlatFileExecStream::newFlatFileExecStream(), params);
    }

    // implement FemVisitor
    virtual void visit(ProxyLcsClusterAppendStreamDef &streamDef)
    {
        LcsClusterAppendExecStreamParams params;
        pExecStreamFactory->readTupleStreamParams(params, streamDef);
        pExecStreamFactory->readBTreeStreamParams(params, streamDef);
        
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
        readClusterScan(streamDef, params);
        CmdInterpreter::readTupleProjection(
            params.outputProj, streamDef.getOutputProj());
        params.dynParamId =
            readDynamicParamId(streamDef.getRowCountParamId());

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
    virtual void visit(ProxyLbmIndexScanStreamDef &streamDef)
    {
        LbmIndexScanExecStreamParams params;
        pExecStreamFactory->readBTreeSearchStreamParams(params, streamDef);

        params.rowLimitParamId =
            readDynamicParamId(streamDef.getRowLimitParamId());

        params.ignoreRowLimit = streamDef.isIgnoreRowLimit();

        params.startRidParamId =
            readDynamicParamId(streamDef.getStartRidParamId());

        pEmbryo->init(new LbmIndexScanExecStream(), params);
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
