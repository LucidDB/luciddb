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
#include "fennel/farrago/NativeMethods.h"
#include "fennel/farrago/ExecutionStreamFactory.h"
#include "fennel/redsquare/sorter/ExternalSortStream.h"
#include "fennel/db/Database.h"
#include "fennel/segment/SegmentFactory.h"

#ifdef __MINGW32__
#include <windows.h>
#endif

FENNEL_BEGIN_CPPFILE("$Id$");

class ExecutionStreamSubFactory_rs
    : public ExecutionStreamSubFactory, virtual public FemVisitor
{
    ExecutionStreamFactory *pFactory;
    ExecutionStreamParts *pParts;
    bool created;
    
    // implement FemVisitor
    virtual void visit(ProxySortingStreamDef &streamDef)
    {
        if (streamDef.getDistinctness() != DUP_ALLOW) {
            // can't handle it
            created = false;
            return;
        }

        SharedDatabase pDatabase = pFactory->getDatabase();
        
        ExternalSortStream *pStream =
            &(ExternalSortStream::newExternalSortStream());

        ExternalSortStreamParams *pParams = new ExternalSortStreamParams();

        // ExternalSortStream requires a private ScratchSegment
        pParams->scratchAccessor =
            pDatabase->getSegmentFactory()->newScratchSegment(
                pDatabase->getCache());
        
        pFactory->readTupleStreamParams(*pParams,streamDef);
        pParams->distinctness = streamDef.getDistinctness();
        pParams->pTempSegment = pDatabase->getTempSegment();
        pParams->storeFinalRun = false;
        ExecutionStreamFactory::readTupleProjection(
            pParams->keyProj,
            streamDef.getKeyProj());
        pParts->setParts(pStream,pParams);
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
    
    // implement ExecutionStreamSubFactory
    virtual bool createStream(
        ExecutionStreamFactory &factory,
        ProxyExecutionStreamDef &streamDef,
        ExecutionStreamParts &parts)
    {
        pFactory = &factory;
        pParts = &parts;
        created = true;
        
        // dispatch based on polymorphic stream type
        FemVisitor::visitTbl.accept(*this,streamDef);
        
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
        streamGraphHandle.pStreamFactory->addSubFactory(
            SharedExecutionStreamSubFactory(
                new ExecutionStreamSubFactory_rs()));
    } catch (std::exception &ex) {
        pEnv.handleExcn(ex);
    }
}

FENNEL_END_CPPFILE("$Id$");

// End NativeMethods_rs.cpp
