/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 2004 Red Square
// Copyright (C) 2004 John V. Sichi
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
#include "fennel/redsquare/sorter/ExternalSortExecStream.h"
#include "fennel/db/Database.h"
#include "fennel/segment/SegmentFactory.h"
#include "fennel/exec/ExecStreamEmbryo.h"

// DEPRECATED
#include "fennel/redsquare/sorter/ExternalSortStream.h"
#include "fennel/farrago/ExecutionStreamFactory.h"

#ifdef __MINGW32__
#include <windows.h>
#endif

FENNEL_BEGIN_CPPFILE("$Id$");

class ExecStreamSubFactory_rs
    : public ExecutionStreamSubFactory, // DEPRECATED
        public ExecStreamSubFactory,
        public FemVisitor
{
    ExecStreamFactory *pExecStreamFactory;
    ExecStreamEmbryo *pEmbryo;
    
    bool created;
    
    // DEPRECATED
    ExecutionStreamFactory *pStreamFactory;
    ExecutionStreamParts *pParts;
    
    // implement FemVisitor
    virtual void visit(ProxySortingStreamDef &streamDef)
    {
        if (streamDef.getDistinctness() != DUP_ALLOW) {
            // can't handle it
            created = false;
            return;
        }

        if (pStreamFactory) {
            // DEPRECATED
            SharedDatabase pDatabase = pStreamFactory->getDatabase();
        
            ExternalSortStream *pStream =
                &(ExternalSortStream::newExternalSortStream());

            ExternalSortStreamParams *pParams = new ExternalSortStreamParams();

            // ExternalSortStream requires a private ScratchSegment
            pParams->scratchAccessor =
                pDatabase->getSegmentFactory()->newScratchSegment(
                    pDatabase->getCache());
        
            pStreamFactory->readTupleStreamParams(*pParams,streamDef);
            pParams->distinctness = streamDef.getDistinctness();
            pParams->pTempSegment = pDatabase->getTempSegment();
            pParams->storeFinalRun = false;
            CmdInterpreter::readTupleProjection(
                pParams->keyProj,
                streamDef.getKeyProj());
            pParts->setParts(pStream,pParams);
        }

        if (pExecStreamFactory) {
            SharedDatabase pDatabase = pExecStreamFactory->getDatabase();
        
            ExternalSortExecStreamParams params;

            // REVIEW jvs 18-Nov-2004:  what about quota accessor?

            // ExternalSortStream requires a private ScratchSegment
            params.scratchAccessor =
                pDatabase->getSegmentFactory()->newScratchSegment(
                    pDatabase->getCache());
        
            pExecStreamFactory->readTupleStreamParams(params, streamDef);
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
    }

    // implement JniProxyVisitor
    virtual void unhandledVisit()
    {
        // not a stream type we know about
        created = false;
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
    JniUtil::initDebug("FENNEL_RS_JNI_DEBUG");
    FENNEL_JNI_ONLOAD_COMMON();
    return JniUtil::jniVersion;
}

extern "C" JNIEXPORT void JNICALL
Java_com_redsquare_farrago_fennel_RedSquareJni_registerStreamFactory(
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
                    new ExecStreamSubFactory_rs()));
        }
        if (streamGraphHandle.pExecStreamFactory) {
            streamGraphHandle.pExecStreamFactory->addSubFactory(
                SharedExecStreamSubFactory(
                    new ExecStreamSubFactory_rs()));
        }
    } catch (std::exception &ex) {
        pEnv.handleExcn(ex);
    }
}

FENNEL_END_CPPFILE("$Id$");

// End NativeMethods_rs.cpp
