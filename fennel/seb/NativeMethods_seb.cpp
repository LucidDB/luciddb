/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2009-2009 The Eigenbase Project
// Copyright (C) 2009-2009 SQLstream, Inc.
// Copyright (C) 2009-2009 LucidEra, Inc.
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
#include "fennel/seb/SebCmdInterpreter.h"
#include "fennel/seb/SebQueryExecStream.h"
#include "fennel/seb/SebInsertExecStream.h"
#include "fennel/segment/SegmentFactory.h"
#include "fennel/exec/ExecStreamEmbryo.h"
#include "fennel/cache/QuotaCacheAccessor.h"
#include "fennel/ftrs/BTreeScanExecStream.h"
#include "fennel/ftrs/FtrsTableWriterExecStream.h"

#include "scaledb/incl/SdbStorageAPI.h"

#ifdef __MSVC__
#include <windows.h>
#endif

FENNEL_BEGIN_CPPFILE("$Id$");

class ExecStreamSubFactory_seb
    : public ExecStreamSubFactory,
        public FemVisitor
{
    ExecStreamFactory *pExecStreamFactory;
    ExecStreamEmbryo *pEmbryo;

    bool created;

    inline unsigned short getTableId(PageId pageId)
    {
        uint i = opaqueToInt(pageId);
        return (unsigned short) (i & 0xFFFF);
    }

    inline unsigned short getIndexId(PageId pageId)
    {
        uint i = opaqueToInt(pageId);
        return (unsigned short) ((i & 0xFFFF0000) >> 16);
    }

    // implement FemVisitor
    virtual void visit(ProxyTableInserterDef &streamDef)
    {
        FtrsTableWriterExecStreamParams ftrsParams;
        ftrsParams.actionType = FtrsTableWriter::ACTION_INSERT;
        pExecStreamFactory->readTableWriterStreamParams(ftrsParams, streamDef);
        SebInsertExecStreamParams params;
        params.pCacheAccessor = ftrsParams.pCacheAccessor;
        params.scratchAccessor = ftrsParams.scratchAccessor;
        params.outputTupleDesc = ftrsParams.outputTupleDesc;
        params.outputTupleFormat = ftrsParams.outputTupleFormat;
        params.tableId = MAXU;
        for (uint i = 0; i < ftrsParams.indexParams.size(); ++i) {
            if (ftrsParams.indexParams[i].inputProj.size() == 0) {
                // REVIEW jvs 13-Jul-2009:  deal with dynamic root?
                params.tableId =
                    getTableId(ftrsParams.indexParams[i].rootPageId);
                break;
            }
        }
        assert(!isMAXU(params.tableId));

        pEmbryo->init(
            new SebInsertExecStream(),
            params);
    }

    virtual void visit(ProxyIndexScanDef &streamDef)
    {
        BTreeScanExecStreamParams ftrsParams;
        pExecStreamFactory->readBTreeReadStreamParams(ftrsParams, streamDef);
        SebQueryExecStreamParams params;
        params.pCacheAccessor = ftrsParams.pCacheAccessor;
        params.scratchAccessor = ftrsParams.scratchAccessor;
        params.outputTupleDesc = ftrsParams.outputTupleDesc;
        params.outputTupleFormat = ftrsParams.outputTupleFormat;
        params.tableId = getTableId(ftrsParams.rootPageId);
        params.indexId = getIndexId(ftrsParams.rootPageId);
        params.outputProj = ftrsParams.outputProj;
        pEmbryo->init(
            new SebQueryExecStream(),
            params);
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

#ifdef __MSVC__
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
    JniUtil::initDebug("FENNEL_SEB_JNI_DEBUG");
    FENNEL_JNI_ONLOAD_COMMON();
    return JniUtil::jniVersion;
}

extern "C" JNIEXPORT jlong JNICALL
Java_net_sf_farrago_fennel_SebJni_executeJavaCmd(
    JNIEnv *pEnvInit, jclass, jobject jCmd, jlong jExecHandle)
{
    JniEnvRef pEnv(pEnvInit);
    try {
        ProxyCmd cmd;
        cmd.init(pEnv, jCmd);
        SebCmdInterpreter cmdInterpreter;
        if (jExecHandle == 0) {
            cmdInterpreter.pExecHandle = NULL;
        } else {
            CmdInterpreter::ExecutionHandle &execHandle =
                CmdInterpreter::getExecutionHandleFromLong(jExecHandle);
            cmdInterpreter.pExecHandle = &execHandle;
        }
        return cmdInterpreter.executeCommand(cmd);
    } catch (std::exception &ex) {
        pEnv.handleExcn(ex);
        return 0;
    }
}

extern "C" JNIEXPORT void JNICALL
Java_net_sf_farrago_fennel_SebJni_registerStreamFactory(
    JNIEnv *pEnvInit, jclass, jlong hStreamGraph)
{
    JniEnvRef pEnv(pEnvInit);
    try {
        CmdInterpreter::StreamGraphHandle &streamGraphHandle =
            CmdInterpreter::getStreamGraphHandleFromLong(hStreamGraph);
        if (streamGraphHandle.pExecStreamFactory) {
            streamGraphHandle.pExecStreamFactory->addSubFactory(
                SharedExecStreamSubFactory(
                    new ExecStreamSubFactory_seb()));
        }
    } catch (std::exception &ex) {
        pEnv.handleExcn(ex);
    }
}

FENNEL_END_CPPFILE("$Id$");

// End NativeMethods_seb.cpp
