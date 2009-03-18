/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2004-2007 Disruptive Tech
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
#include "fennel/farrago/ExecStreamFactory.h"
#include "fennel/disruptivetech/xo/CalcExecStream.h"
#include "fennel/disruptivetech/xo/CollectExecStream.h"
#include "fennel/disruptivetech/xo/UncollectExecStream.h"
#include "fennel/disruptivetech/xo/CorrelationJoinExecStream.h"
#include "fennel/exec/ExecStreamEmbryo.h"

#ifdef __MINGW32__
#include <windows.h>
#endif

FENNEL_BEGIN_CPPFILE("$Id$");

class ExecStreamSubFactory_dt
    : public ExecStreamSubFactory,
        public FemVisitor
{
    ExecStreamFactory *pExecStreamFactory;
    ExecStreamEmbryo *pEmbryo;
    bool created;

    // implement FemVisitor
    virtual void visit(ProxyCalcTupleStreamDef &streamDef)
    {
        CalcExecStreamParams params;
        pExecStreamFactory->readTupleStreamParams(params, streamDef);
        params.program = streamDef.getProgram();
        params.isFilter = streamDef.isFilter();
        pEmbryo->init(
            new CalcExecStream(),
            params);
    }

    virtual void visit(ProxyCorrelationJoinStreamDef &streamDef)
    {
        CorrelationJoinExecStreamParams params;
        pExecStreamFactory->readTupleStreamParams(params, streamDef);
        SharedProxyCorrelation pCorrelation = streamDef.getCorrelations();
        for ( /* empty */; pCorrelation; ++pCorrelation) {
            Correlation correlation(
                DynamicParamId(pCorrelation->getId()),
                pCorrelation->getOffset());
            params.correlations.push_back(correlation);
        }
        pEmbryo->init(new CorrelationJoinExecStream(), params);
    }

    virtual void visit(ProxyCollectTupleStreamDef &streamDef)
    {
        CollectExecStreamParams params;
        pExecStreamFactory->readTupleStreamParams(params, streamDef);
        pEmbryo->init(new CollectExecStream(), params);
    }

    virtual void visit(ProxyUncollectTupleStreamDef &streamDef)
    {
        UncollectExecStreamParams params;
        pExecStreamFactory->readTupleStreamParams(params, streamDef);
        pEmbryo->init(new UncollectExecStream(), params);
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
