/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 1999-2007 John V. Sichi
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
#include "fennel/farrago/JniUtil.h"
#include "fennel/farrago/JavaExcn.h"
#include "fennel/common/FennelResource.h"
#include "fennel/common/ConfigMap.h"
#include "fennel/common/Backtrace.h"
#include "fennel/tuple/StoredTypeDescriptor.h"

#ifdef __MINGW32__
#include <process.h>
#endif

#ifndef __MINGW32__
#include <signal.h>
#endif

FENNEL_BEGIN_CPPFILE("$Id$");

ParamName JniUtilParams::paramJniHandleTraceFile = "jniHandleTraceFile";

JavaVM *JniUtil::pVm = NULL;
jmethodID JniUtil::methGetClassName = 0;
jmethodID JniUtil::methGetInterfaces = 0;
jmethodID JniUtil::methGetModifiers = 0;
jclass JniUtil::classModifier = 0;
jmethodID JniUtil::methIsPublic = 0;
jmethodID JniUtil::methHasNext = 0;
jmethodID JniUtil::methNext = 0;
jmethodID JniUtil::methIterator = 0;
jmethodID JniUtil::methGetJavaStreamHandle = 0;
jmethodID JniUtil::methGetIndexRoot = 0;
jmethodID JniUtil::methToString = 0;
jclass JniUtil::classRhBase64;
jmethodID JniUtil::methRandomUUID;
jclass JniUtil::classUUID;
jmethodID JniUtil::methFarragoTransformInit = 0;
jmethodID JniUtil::methFarragoTransformExecute = 0;
jmethodID JniUtil::methFarragoTransformRestart = 0;
jclass JniUtil::classFarragoTransformInputBinding = 0;
jmethodID JniUtil::methFarragoTransformInputBindingCons = 0;
jmethodID JniUtil::methFarragoRuntimeContextStatementClassForName = 0;
jclass JniUtil::classLong;
jclass JniUtil::classInteger;
jclass JniUtil::classShort;
jclass JniUtil::classDouble;
jclass JniUtil::classFloat;
jclass JniUtil::classBoolean;
jmethodID JniUtil::methLongValueOf = 0;
jmethodID JniUtil::methIntegerValueOf = 0;
jmethodID JniUtil::methShortValueOf = 0;
jmethodID JniUtil::methDoubleValueOf = 0;
jmethodID JniUtil::methFloatValueOf = 0;
jmethodID JniUtil::methBooleanValueOf = 0;
jmethodID JniUtil::methLongValue = 0;
jmethodID JniUtil::methIntValue = 0;
jmethodID JniUtil::methShortValue = 0;
jmethodID JniUtil::methDoubleValue = 0;
jmethodID JniUtil::methFloatValue = 0;
jmethodID JniUtil::methBooleanValue = 0;
jmethodID JniUtil::methBase64Decode;

AtomicCounter JniUtil::handleCount;

bool JniUtil::traceHandleCountEnabled = false;
bool JniUtil::closeHandleCountTraceOnZero = false;
std::ofstream JniUtil::handleCountTraceStream;

#ifndef __MINGW32__
static void debugger_signalHandler(int signum)
{
    // do nothing
}
#endif

JniUtilParams::JniUtilParams()
{
    jniHandleTraceFile = "";
}

void JniUtilParams::readConfig(ConfigMap const &configMap)
{
    jniHandleTraceFile = configMap.getStringParam(paramJniHandleTraceFile);
}

void JniUtil::initDebug(char const *envVarName)
{
    char *pDebug = getenv(envVarName);
    if (pDebug && (atoi(pDebug) >= 1)) {
        char pidstr[32];
        snprintf(pidstr, 32, "%d", getpid());
        std::cout << "Waiting for debugger; pid=" << pidstr << std::endl;
        std::cout.flush();
#ifdef __MINGW32__
        // A "cont" in gdb will wake this sleep up immediately, which
        // is disturbing but useful.
        _sleep(600000);
#else
        // On older versions of Linux, a "cont" in gdb will wake this
        // sleep up immediately, which is disturbing but useful.
        // On newer versions, the continue command resumes
        // the sleep().  So, if $envVarName > 1, wait for SIGHUP.
        // Use the "signal 1" command to wake the pause up.
        if (atoi(pDebug) == 1) {
            sleep(60000);
        } else {
            struct sigaction act;
            struct sigaction oldact;

            act.sa_handler = debugger_signalHandler;
            sigemptyset(&act.sa_mask);
            act.sa_flags = 0;

            if (!sigaction(SIGHUP, &act, &oldact)) {
                // Signal handler installed properly.  Wait for signal.
                pause();

                // Restore the old signal handler.
                sigaction(SIGHUP, &oldact, NULL);
            } else {
                // Fall back on sleeping.
                sleep(60000);
            }
        }        
#endif
    }
}

void JniUtil::configure(const JniUtilParams &params)
{
    // Check if the stream is already open.  During unit tests Fennel is
    // sometimes stopped and restarted in a single process.  Flag an error
    // if this happens, it means the previous shutdown failed.
    if (handleCountTraceStream.is_open()) {
        assert(false);

        // Non-debug builds: clean up
        handleCountTraceStream
            << "ERROR: trace stream already open" << std::endl;
        handleCountTraceStream.flush();
        handleCountTraceStream.close();
        traceHandleCountEnabled = false;
        closeHandleCountTraceOnZero = false;
    }

    if (params.jniHandleTraceFile.length() > 0) {
        handleCountTraceStream.open(
            params.jniHandleTraceFile.c_str(), std::ios::app);

        handleCountTraceStream
            << "# Fennel JNI Handle Trace (see //open/util/bin/checkJniHandleTrace.pl)"
            << std::endl;

        assert(handleCountTraceStream.good());

        traceHandleCountEnabled = true;
        closeHandleCountTraceOnZero = false;
    }
}

void JniUtil::shutdown()
{
    // JavaTraceTarget decrements after DbHandle.  Set a flag to close the
    // trace stream when the counter hits zero.
    if (traceHandleCountEnabled) {
        closeHandleCountTraceOnZero = true;
    }
}


jint JniUtil::init(JavaVM *pVmInit)
{
    // Install handler to print backtrace on fatal error.  Note that
    // we pass false to suppress handling of SIGSEGV, because Java
    // raises this spuriously.  Instead, if a real SIGSEGV occurs
    // in native code, Java will abort, raising SIGABRT from the same
    // stack frame, so we'll catch that and backtrace it instead.
    AutoBacktrace::install(false);
    pVm = pVmInit;
    JniEnvAutoRef pEnv;
    jclass classClass = pEnv->FindClass("java/lang/Class");
    jclass classObject = pEnv->FindClass("java/lang/Object");
    jclass classCollection = pEnv->FindClass("java/util/Collection");
    jclass classIterator = pEnv->FindClass("java/util/Iterator");

    // Make sure this jclass is a global ref or the JVM might move it on us.
    // This is only required for classes on which we need to invoke
    // static methods.
    classRhBase64 = pEnv->FindClass("org/eigenbase/util/RhBase64");
    classRhBase64 = (jclass) pEnv->NewGlobalRef(classRhBase64);
    classUUID = pEnv->FindClass("java/util/UUID");
    classUUID = (jclass) pEnv->NewGlobalRef(classUUID);

    jclass classFennelJavaStreamMap = pEnv->FindClass(
        "net/sf/farrago/fennel/FennelJavaStreamMap");
    jclass classFarragoTransform = pEnv->FindClass(
        "net/sf/farrago/runtime/FarragoTransform");

    // Make sure this jclass is a global ref of the JVM might move it on us.
    jclass tempInputBinding =
        pEnv->FindClass(
            "net/sf/farrago/runtime/FarragoTransform$InputBinding");
    classFarragoTransformInputBinding = 
        (jclass)pEnv->NewGlobalRef(tempInputBinding);

    jclass classFarragoRuntimeContext = pEnv->FindClass(
        "net/sf/farrago/runtime/FarragoRuntimeContext");
    methGetClassName = pEnv->GetMethodID(
        classClass,"getName","()Ljava/lang/String;");
    methGetInterfaces = pEnv->GetMethodID(
        classClass,"getInterfaces","()[Ljava/lang/Class;");
    methGetModifiers = pEnv->GetMethodID(
        classClass,"getModifiers","()I");

    jclass tempClassModifier = pEnv->FindClass("java/lang/reflect/Modifier");
    classModifier = (jclass)pEnv->NewGlobalRef(tempClassModifier);
    methIsPublic = pEnv->GetStaticMethodID(classModifier, "isPublic", "(I)Z");

    methIterator = pEnv->GetMethodID(
        classCollection,"iterator","()Ljava/util/Iterator;");
    methHasNext = pEnv->GetMethodID(
        classIterator,"hasNext","()Z");
    methNext = pEnv->GetMethodID(
        classIterator,"next","()Ljava/lang/Object;");
    methGetJavaStreamHandle = pEnv->GetMethodID(
        classFennelJavaStreamMap,"getJavaStreamHandle",
        "(I)J");
    methGetIndexRoot = pEnv->GetMethodID(
        classFennelJavaStreamMap,"getIndexRoot",
        "(J)J");
    methToString = pEnv->GetMethodID(
        classObject,"toString","()Ljava/lang/String;");

    jclass tempClassLong = pEnv->FindClass("java/lang/Long");
    classLong = (jclass)pEnv->NewGlobalRef(tempClassLong);
    methLongValueOf = 
        pEnv->GetStaticMethodID(classLong, "valueOf", "(J)Ljava/lang/Long;");
    methLongValue = pEnv->GetMethodID(classLong, "longValue", "()J");

    jclass tempClassInteger = pEnv->FindClass("java/lang/Integer");
    classInteger = (jclass)pEnv->NewGlobalRef(tempClassInteger);
    methIntegerValueOf = 
        pEnv->GetStaticMethodID(
            classInteger, "valueOf", "(I)Ljava/lang/Integer;");
    methIntValue = pEnv->GetMethodID(classInteger, "intValue", "()I");

    jclass tempClassShort = pEnv->FindClass("java/lang/Short");
    classShort = (jclass)pEnv->NewGlobalRef(tempClassShort);
    methShortValueOf = 
        pEnv->GetStaticMethodID(classShort, "valueOf", "(S)Ljava/lang/Short;");
    methShortValue = pEnv->GetMethodID(classShort, "shortValue", "()S");

    jclass tempClassDouble = pEnv->FindClass("java/lang/Double");
    classDouble = (jclass)pEnv->NewGlobalRef(tempClassDouble);
    methDoubleValueOf = 
        pEnv->GetStaticMethodID(
            classDouble, "valueOf", "(D)Ljava/lang/Double;");
    methDoubleValue = pEnv->GetMethodID(classDouble, "doubleValue", "()D");

    jclass tempClassFloat = pEnv->FindClass("java/lang/Float");
    classFloat = (jclass)pEnv->NewGlobalRef(tempClassFloat);
    methFloatValueOf = 
        pEnv->GetStaticMethodID(classFloat, "valueOf", "(F)Ljava/lang/Float;");
    methFloatValue = pEnv->GetMethodID(classFloat, "floatValue", "()F");

    jclass tempClassBoolean = pEnv->FindClass("java/lang/Boolean");
    classBoolean = (jclass)pEnv->NewGlobalRef(tempClassBoolean);
    methBooleanValueOf = 
        pEnv->GetStaticMethodID(
            classBoolean, "valueOf", "(Z)Ljava/lang/Boolean;");
    methBooleanValue = pEnv->GetMethodID(classBoolean, "booleanValue", "()Z");

    methBase64Decode = pEnv->GetStaticMethodID(
        classRhBase64,"decode","(Ljava/lang/String;)[B");
    methRandomUUID = pEnv->GetStaticMethodID(
        classUUID,"randomUUID","()Ljava/util/UUID;");
    methFarragoTransformInit = pEnv->GetMethodID(
        classFarragoTransform, "init", 
        "(Lnet/sf/farrago/runtime/FarragoRuntimeContext;Ljava/lang/String;[Lnet/sf/farrago/runtime/FarragoTransform$InputBinding;)V");
    methFarragoTransformExecute = pEnv->GetMethodID(
        classFarragoTransform, "execute", "(Ljava/nio/ByteBuffer;J)I");
    methFarragoTransformRestart = pEnv->GetMethodID(
        classFarragoTransform, "restart", "()V");
    methFarragoTransformInputBindingCons =
        pEnv->GetMethodID(
            classFarragoTransformInputBinding, "<init>", 
            "(Ljava/lang/String;I)V");
    methFarragoRuntimeContextStatementClassForName =
        pEnv->GetMethodID(
            classFarragoRuntimeContext,
            "statementClassForName",
            "(Ljava/lang/String;)Ljava/lang/Class;");

    return jniVersion;
}

JNIEnv *JniUtil::getAttachedJavaEnv(bool &needDetach)
{
    void *pEnv = NULL;
    jint rc = pVm->GetEnv(&pEnv,jniVersion);
    if (rc == JNI_OK) {
        // previously attached, so it would be wrong to detach in destructor
        needDetach = false;
        return static_cast<JNIEnv *>(pEnv);
    }
    needDetach = true;
    rc = pVm->AttachCurrentThread(&pEnv,NULL);
    assert(rc == 0);
    assert(pEnv);
    return static_cast<JNIEnv *>(pEnv);
}

void JniUtil::detachJavaEnv()
{
    jint rc = pVm->DetachCurrentThread();
    assert(rc == 0);
}

std::string JniUtil::getClassName(jclass jClass)
{
    JniEnvAutoRef pEnv;
    jstring jString = reinterpret_cast<jstring>(
        pEnv->CallObjectMethod(jClass,methGetClassName));
    assert(jString);
    return toStdString(pEnv,jString);
}

std::string JniUtil::getFirstPublicInterfaceName(jclass jClass)
{
    JniEnvAutoRef pEnv;
    
    jobjectArray interfaces = 
        reinterpret_cast<jobjectArray>(
            pEnv->CallObjectMethod(jClass, methGetInterfaces));
    assert(interfaces);

    for(jsize i = 0, len = pEnv->GetArrayLength(interfaces); i < len; i++) {
        jclass interface = 
            reinterpret_cast<jclass>(
                pEnv->GetObjectArrayElement(interfaces, i));
        assert(interface);

        jint modifiers = 
            pEnv->CallIntMethod(interface, methGetModifiers);

        jboolean isPublic =
            pEnv->CallStaticBooleanMethod(
                classModifier, methIsPublic, modifiers);

        if (isPublic) {
            return getClassName(interface);
        }
    }

    return std::string("(none)");
}

std::string JniUtil::toStdString(JniEnvRef pEnv,jstring jString)
{
    const char *pChars = pEnv->GetStringUTFChars(jString,NULL);
    assert(pChars);
    std::string str(pChars,pEnv->GetStringUTFLength(jString));
    pEnv->ReleaseStringUTFChars(jString,pChars);
    return str;
}

jstring JniUtil::toString(JniEnvRef pEnv,jobject jObject)
{
    return reinterpret_cast<jstring>(
        pEnv->CallObjectMethod(jObject,methToString));
}

uint JniUtil::lookUpEnum(std::string *pSymbols,std::string const &symbol)
{
    for (uint i = 0; ; ++i) {
        assert(pSymbols[i].size());
        if (pSymbols[i] == symbol) {
            return i;
        }
    }
}

jobject JniUtil::getIter(JniEnvRef pEnv,jobject jCollection)
{
    return pEnv->CallObjectMethod(jCollection,methIterator);
}

jobject JniUtil::getNextFromIter(JniEnvRef pEnv,jobject jIter)
{
    if (!pEnv->CallBooleanMethod(jIter,methHasNext)) {
        return NULL;
    }
    return pEnv->CallObjectMethod(jIter,methNext);
}

void JniUtil::incrementHandleCount(const char *pType, const void *pHandle)
{
    ++handleCount;
    
    traceHandleCount("INC", pType, pHandle);
}

void JniUtil::decrementHandleCount(const char *pType, const void *pHandle)
{
    --handleCount;
    
    assert(handleCount >= 0);

    traceHandleCount("DEC", pType, pHandle);
}

void JniUtil::traceHandleCount(
    const char *pAction, const char *pType, const void *pHandle)
{
    if (traceHandleCountEnabled) {
        handleCountTraceStream
            << pAction << " " << pType << ": " << pHandle << std::endl;

        if (handleCount == 0 && closeHandleCountTraceOnZero && 
            strcmp(pAction, "DEC") == 0) {
            traceHandleCountEnabled = false;
            closeHandleCountTraceOnZero = false;
            
            handleCountTraceStream.flush();
            handleCountTraceStream.close();   
        }
    }
}

std::string JniUtil::getXmi(const TupleDescriptor &tupleDescriptor)
{
    std::ostringstream oss;
    oss << "<XMI xmi.version = '1.2' "
        << "xmlns:FEMFennel = 'org.omg.xmi.namespace.FEMFennel'>" << std::endl;
    oss << "<XMI.content>" << std::endl;
    oss << "<FEMFennel:TupleDescriptor>" << std::endl;
    for (uint i = 0; i < tupleDescriptor.size(); ++i) {
        TupleAttributeDescriptor const &attrDescriptor =
            tupleDescriptor[i];
        oss << "<FEMFennel:TupleDescriptor.AttrDescriptor>";
        oss << "<FEMFennel:TupleAttrDescriptor ";
        oss << "typeOrdinal='";
        oss << attrDescriptor.pTypeDescriptor->getOrdinal();
        oss << "' ";
        oss << "isNullable='";
        oss << (attrDescriptor.isNullable ? "true" : "false");
        oss << "' ";
        oss << "byteLength='";
        oss << attrDescriptor.cbStorage;
        oss << "' ";
        oss << "/>" << std::endl;
        oss << "</FEMFennel:TupleDescriptor.AttrDescriptor>";
    }
    oss << "</FEMFennel:TupleDescriptor>" << std::endl;
    oss << "</XMI.content>" << std::endl;
    oss << "</XMI>" << std::endl;
    std::string s = oss.str();
    return s;
}

void JniExceptionChecker::checkExceptions()
{
    jthrowable excn = pEnv->ExceptionOccurred();
    if (excn) {
        pEnv->ExceptionClear();
        throw JavaExcn(excn);
    }
}

JniExceptionChecker::~JniExceptionChecker()
{
    checkExceptions();
}

JniEnvAutoRef::JniEnvAutoRef()
    : JniEnvRef(JniUtil::getAttachedJavaEnv(needDetach))
{
}

JniEnvAutoRef::~JniEnvAutoRef()
{
    if (needDetach) {
        JniUtil::detachJavaEnv();
    }
}

void JniEnvAutoRef::suppressDetach()
{
    needDetach = false;
}

void JniEnvRef::handleExcn(std::exception &ex)
{
    JavaExcn *pJavaExcn = dynamic_cast<JavaExcn *>(&ex);
    if (pJavaExcn) {
        pEnv->Throw(pJavaExcn->getJavaException());
        return;
    }
    std::string what;
    FennelExcn *pFennelExcn = dynamic_cast<FennelExcn *>(&ex);
    if (pFennelExcn) {
        what = pFennelExcn->getMessage();
    } else {
        what = FennelResource::instance().internalError(ex.what());
    }
    // TODO:  need special-case handling for out-of-memory here
    jclass classSQLException = pEnv->FindClass("java/sql/SQLException");
    jstring jMessage = pEnv->NewStringUTF(what.c_str());
    jmethodID constructor = pEnv->GetMethodID(
        classSQLException,"<init>","(Ljava/lang/String;)V");
    jthrowable t = (jthrowable)
        pEnv->NewObject(classSQLException,constructor,jMessage);
    pEnv->Throw(t);
}

void JniPseudoUuidGenerator::generateUuid(PseudoUuid &pseudoUuid)
{
    JniEnvAutoRef pEnv;
    jobject jUuid = pEnv->CallStaticObjectMethod(
        JniUtil::classUUID,
        JniUtil::methRandomUUID);
    jstring jsUuid = JniUtil::toString(pEnv, jUuid);
    std::string sUuid = JniUtil::toStdString(pEnv, jsUuid);
    pseudoUuid.parse(sUuid);
}

FENNEL_END_CPPFILE("$Id$");

// End JniUtil.cpp
