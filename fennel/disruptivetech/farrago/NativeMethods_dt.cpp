/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 2004-2004 Disruptive Tech
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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
#include "fennel/disruptivetech/xo/CalcExecStream.h"
#include "fennel/exec/ExecStreamEmbryo.h"

// DEPRECATED
#include "fennel/farrago/ExecutionStreamFactory.h"
#include "fennel/disruptivetech/xo/CalcTupleStream.h"

#ifdef __MINGW32__
#include <windows.h>
#endif

FENNEL_BEGIN_CPPFILE("$Id$");

class ExecStreamSubFactory_dt
    : public ExecutionStreamSubFactory, // DEPRECATED
        public ExecStreamSubFactory,
        virtual public FemVisitor
{
    ExecStreamFactory *pExecStreamFactory;
    ExecStreamEmbryo *pEmbryo;
    bool created;

    // DEPRECATED
    ExecutionStreamFactory *pStreamFactory;
    ExecutionStreamParts *pParts;
    
    // implement FemVisitor
    virtual void visit(ProxyCalcTupleStreamDef &streamDef)
    {
        if (pStreamFactory) {
            // DEPRECATED
            CalcTupleStream *pStream = new CalcTupleStream();
            CalcTupleStreamParams *pParams = new CalcTupleStreamParams();
            pStreamFactory->readTupleStreamParams(*pParams,streamDef);
            pParams->program = streamDef.getProgram();
            pParams->isFilter = streamDef.isFilter();
            pParts->setParts(pStream,pParams);
        } else {
            CalcExecStreamParams params;
            pExecStreamFactory->readTupleStreamParams(params, streamDef);
            params.program = streamDef.getProgram();
            params.isFilter = streamDef.isFilter();
            pEmbryo->init(
                new CalcExecStream(),
                params);
        }
    }

    // implement JniProxyVisitor
    virtual void unhandledVisit()
    {
        // not a stream type we know about
        created = false;
    }

    // implement JniProxyVisitor
    virtual void *getLeafPtr()
    {
        return static_cast<FemVisitor *>(this);
    }
    
    // implement JniProxyVisitor
    virtual const char *getLeafTypeName()
    {
        return "FemVisitor";
    }
    
    // DEPRECATED
    // implement ExecutionStreamSubFactory
    virtual bool createStream(
        ExecutionStreamFactory &factory,
        ProxyExecutionStreamDef &streamDef,
        ExecutionStreamParts &parts)
    {
        pStreamFactory = &factory;
        pExecStreamFactory = NULL;
        pParts = &parts;
        pEmbryo = NULL;
        created = true;
        
        // dispatch based on polymorphic stream type
        FemVisitor::visitTbl.accept(*this,streamDef);
        
        return created;
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
        
        // DEPRECATED
        pStreamFactory = NULL;
        pParts = NULL;
        
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
    JniUtil::initDebug("FENNEL_DT_JNI_DEBUG");
    FENNEL_JNI_ONLOAD_COMMON();
    return JniUtil::jniVersion;
}

extern "C" JNIEXPORT void JNICALL
Java_com_disruptivetech_farrago_fennel_DisruptiveTechJni_registerStreamFactory(
    JNIEnv *pEnvInit, jclass, jlong hStreamGraph)
{
    JniEnvRef pEnv(pEnvInit);
    try {
        CmdInterpreter::StreamGraphHandle &streamGraphHandle =
            CmdInterpreter::getStreamGraphHandleFromLong(hStreamGraph);
        if (streamGraphHandle.pStreamFactory) {
            // DEPRECATED
            streamGraphHandle.pStreamFactory->addSubFactory(
                SharedExecutionStreamSubFactory(
                    new ExecStreamSubFactory_dt()));
        }
        if (streamGraphHandle.pExecStreamFactory) {
            streamGraphHandle.pExecStreamFactory->addSubFactory(
                SharedExecStreamSubFactory(
                    new ExecStreamSubFactory_dt()));
        }
    } catch (std::exception &ex) {
        pEnv.handleExcn(ex);
    }
}

FENNEL_END_CPPFILE("$Id$");

// End NativeMethods_dt.cpp
