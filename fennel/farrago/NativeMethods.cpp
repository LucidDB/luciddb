/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 1999-2004 John V. Sichi.
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
#include "fennel/farrago/NativeMethods.h"
#include "fennel/farrago/CmdInterpreter.h"
#include "fennel/farrago/TupleStreamBuilder.h"
#include "fennel/farrago/ExecutionStreamFactory.h"
#include "fennel/farrago/JniUtil.h"
#include "fennel/xo/TupleStreamGraph.h"
#include "fennel/xo/TupleStream.h"
#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/tuple/TupleAccessor.h"
#include "fennel/tuple/AttributeAccessor.h"
#include "fennel/farrago/Fem.h"
#include "fennel/tuple/StandardTypeDescriptor.h"
#include "fennel/common/ByteInputStream.h"

#include <sstream>

#ifdef __MINGW32__
#include <process.h>
#include <windows.h>
#endif

// TODO:  figure out how to get compiler to cross-check method declarations in
// NativeMethods.h!

FENNEL_BEGIN_CPPFILE("$Id$");

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
JNI_OnLoad(JavaVM *vm,void *reserved)
{
    char *pDebug = getenv("FENNEL_JNI_DEBUG");
    if (pDebug && (atoi(pDebug) == 1)) {
        std::cout << "Waiting for debugger; pid=" << getpid() << std::endl;
        // At least on Linux, a "cont" in gdb will wake this sleep up
        // immediately, which is disturbing but useful.
#ifdef __MINGW32__
        _sleep(600000);
#else
        sleep(60000);
#endif
    }
    jint version = JniUtil::init(vm);
    JniEnvAutoRef pEnv;
    staticInitFem(pEnv,FemVisitor::visitTbl);
    return version;
}

extern "C" JNIEXPORT jlong JNICALL
Java_net_sf_farrago_fennel_FennelStorage_executeJavaCmd(
    JNIEnv *pEnvInit, jclass, jobject jCmd)
{
    JniEnvRef pEnv(pEnvInit);
    try {
        ProxyCmd cmd;
        cmd.init(pEnv,jCmd);
        CmdInterpreter cmdInterpreter;
        return cmdInterpreter.executeCommand(cmd);
    } catch (std::exception &ex) {
        pEnv.handleExcn(ex);
        return 0;
    }
}

extern "C" JNIEXPORT jint JNICALL
Java_net_sf_farrago_fennel_FennelStorage_tupleStreamFetch(
    JNIEnv *pEnvInit, jclass, jobject hStream, jbyteArray byteArray)
{
    JniEnvRef pEnv(pEnvInit);
    try {
        ExecutionStream &stream = CmdInterpreter::getStreamFromObj(
            pEnv,hStream);
        uint cbActual;
        ByteInputStream &inputResultStream = stream.getProducerResultStream();
        PConstBuffer pBuffer = inputResultStream.getReadPointer(
            1,&cbActual);
        if (pBuffer) {
            assert(uint(pEnv->GetArrayLength(byteArray)) >= cbActual);
            pEnv->SetByteArrayRegion(
                byteArray,0,cbActual,(jbyte *)(pBuffer));
            inputResultStream.consumeReadPointer(cbActual);
            return cbActual;
        } else {
            return 0;
        }
    } catch (std::exception &ex) {
        pEnv.handleExcn(ex);
        return 0;
    }
}

extern "C" JNIEXPORT void JNICALL
Java_net_sf_farrago_fennel_FennelStorage_tupleStreamGraphOpen(
    JNIEnv *pEnvInit, jclass, jobject hStreamGraph, jobject hTxn,
    jobject hJavaStreamMap)
{
    JniEnvRef pEnv(pEnvInit);
    try {
        CmdInterpreter::StreamGraphHandle &streamGraphHandle =
            CmdInterpreter::getStreamGraphHandleFromObj(pEnv,hStreamGraph);
        CmdInterpreter::TxnHandle &txnHandle =
            CmdInterpreter::getTxnHandleFromObj(pEnv,hTxn);
        streamGraphHandle.javaRuntimeContext = hJavaStreamMap;
        streamGraphHandle.pTupleStreamGraph->setTxn(txnHandle.pTxn);
        streamGraphHandle.pTupleStreamGraph->open();
        // TODO:  finally?
        streamGraphHandle.javaRuntimeContext = NULL;
    } catch (std::exception &ex) {
        pEnv.handleExcn(ex);
    }
}

extern "C" JNIEXPORT void JNICALL
Java_net_sf_farrago_fennel_FennelStorage_tupleStreamGraphClose(
    JNIEnv *pEnvInit, jclass, jobject hStreamGraph, jboolean deallocate)
{
    JniEnvRef pEnv(pEnvInit);
    try {
        CmdInterpreter::StreamGraphHandle &streamGraphHandle =
            CmdInterpreter::getStreamGraphHandleFromObj(pEnv,hStreamGraph);
        if (deallocate) {
            delete &streamGraphHandle;
            --JniUtil::handleCount;
        } else {
            if (streamGraphHandle.pTupleStreamGraph) {
                streamGraphHandle.pTupleStreamGraph->close();
            }
        }
    } catch (std::exception &ex) {
        pEnv.handleExcn(ex);
    }
}

extern "C" JNIEXPORT jstring JNICALL
Java_net_sf_farrago_fennel_FennelStorage_getAccessorXmiForTupleDescriptor(
    JNIEnv *pEnvInit, jclass, jobject jTupleDesc)
{
    JniEnvRef pEnv(pEnvInit);
    
    // NOTE: Since JniProxies are currently read-only, generate XMI
    // representation instead.  If more of this kind of thing starts to
    // accumulate, making the JniProxies read-write would be a good idea.

    ProxyTupleDescriptor proxyTupleDesc;
    proxyTupleDesc.init(pEnv,jTupleDesc);

    // TODO:  excn handling?
    
    // TODO:  should take database handle and use its factory instead
    StandardTypeDescriptorFactory typeFactory;
    TupleDescriptor tupleDescriptor;
    ExecutionStreamFactory::readTupleDescriptor(
        tupleDescriptor,
        proxyTupleDesc,
        typeFactory);
    TupleAccessor tupleAccessor;
    tupleAccessor.compute(tupleDescriptor);
    std::ostringstream oss;
    oss << "<XMI xmi.version = '1.2' "
        << "xmlns:FEMFennel = 'org.omg.xmi.namespace.FEMFennel'>" << std::endl;
    oss << "<XMI.content>" << std::endl;
    oss << "<FEMFennel:TupleAccessor minByteLength='";
    oss << tupleAccessor.getMinByteCount();
    oss << "' bitFieldOffset='";
    if (!isMAXU(tupleAccessor.getBitFieldOffset())) {
        oss << tupleAccessor.getBitFieldOffset();
    } else {
        oss << "-1";
    }
    oss << "'>" << std::endl;
    for (uint i = 0; i < tupleDescriptor.size(); ++i) {
        AttributeAccessor const &attrAccessor =
            tupleAccessor.getAttributeAccessor(i);
        oss << "<FEMFennel:TupleAccessor.AttrAccessor>";
        oss << "<FEMFennel:TupleAttrAccessor ";
        oss << "nullBitIndex='";
        if (!isMAXU(attrAccessor.iNullBit)) {
            oss << attrAccessor.iNullBit;
        } else {
            oss << "-1";
        }
        oss << "' ";
        oss << "fixedOffset='";
        if (!isMAXU(attrAccessor.iFixedOffset)) {
            oss << attrAccessor.iFixedOffset;
        } else {
            oss << "-1";
        }
        oss << "' ";
        oss << "endIndirectOffset='";
        if (!isMAXU(attrAccessor.iEndIndirectOffset)) {
            oss << attrAccessor.iEndIndirectOffset;
        } else {
            oss << "-1";
        }
        oss << "' ";
        oss << "bitValueIndex='";
        if (!isMAXU(attrAccessor.iValueBit)) {
            oss << attrAccessor.iValueBit;
        } else {
            oss << "-1";
        }
        oss << "' ";
        oss << "/>" << std::endl;
        oss << "</FEMFennel:TupleAccessor.AttrAccessor>";
    }
    oss << "</FEMFennel:TupleAccessor>" << std::endl;
    oss << "</XMI.content>" << std::endl;
    oss << "</XMI>" << std::endl;
    std::string s = oss.str();
    return pEnv->NewStringUTF(s.c_str());
}

extern "C" JNIEXPORT jlong JNICALL
Java_net_sf_farrago_fennel_FennelStorage_newObjectHandle(
    JNIEnv *pEnvInit, jclass, jobject obj)
{
    // TODO:  excn handling?
    
    JniEnvRef pEnv(pEnvInit);
    jobject jGlobalRef;
    if (obj) {
        jGlobalRef = pEnv->NewGlobalRef(obj);
        // TODO:  convert to Java excn instead
        assert(jGlobalRef);
    } else {
        jGlobalRef = NULL;
    }
    jobject *pGlobalRef = new jobject;
    ++JniUtil::handleCount;
    *pGlobalRef = jGlobalRef;
    return reinterpret_cast<jlong>(pGlobalRef);
}

extern "C" JNIEXPORT void JNICALL
Java_net_sf_farrago_fennel_FennelStorage_deleteObjectHandle(
    JNIEnv *pEnvInit, jclass, jlong handle)
{
    // TODO:  excn handling?
    
    JniEnvRef pEnv(pEnvInit);
    jobject *pGlobalRef = reinterpret_cast<jobject *>(handle);
    jobject jGlobalRef = *pGlobalRef;
    if (jGlobalRef) {
        pEnv->DeleteGlobalRef(jGlobalRef);
    }
    delete pGlobalRef;
    --JniUtil::handleCount;
}

// TODO:  share code with new/delete

extern "C" JNIEXPORT void JNICALL
Java_net_sf_farrago_fennel_FennelStorage_setObjectHandle(
    JNIEnv *pEnvInit, jclass, jlong handle, jobject obj)
{
    // TODO:  excn handling?
    
    JniEnvRef pEnv(pEnvInit);
    jobject *pGlobalRef = reinterpret_cast<jobject *>(handle);
    jobject jGlobalRef = *pGlobalRef;
    if (jGlobalRef) {
        pEnv->DeleteGlobalRef(jGlobalRef);
    }
    if (obj) {
        jGlobalRef = pEnv->NewGlobalRef(obj);
        // TODO:  convert to Java excn instead
        assert(jGlobalRef);
    } else {
        jGlobalRef = NULL;
    }
    *pGlobalRef = jGlobalRef;
}

extern "C" JNIEXPORT jint JNICALL
Java_net_sf_farrago_fennel_FennelStorage_getHandleCount(
    JNIEnv *pEnvInit, jclass)
{
    return JniUtil::handleCount;
}

FENNEL_END_CPPFILE("$Id$");

// End NativeMethods.cpp
