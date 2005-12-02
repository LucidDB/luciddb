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
#include "fennel/db/Database.h"
#include "fennel/segment/SegmentFactory.h"
#include "fennel/exec/ExecStreamEmbryo.h"

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
        assert(val.size() == 1);
        return val.at(0);
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
        params.calcProgram = streamDef.getCalcProgram();
        pEmbryo->init(FlatFileExecStream::newFlatFileExecStream(), params);
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
