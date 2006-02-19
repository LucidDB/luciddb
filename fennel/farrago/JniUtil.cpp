/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 1999-2005 John V. Sichi
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
jmethodID JniUtil::methHasNext = 0;
jmethodID JniUtil::methNext = 0;
jmethodID JniUtil::methIterator = 0;
jmethodID JniUtil::methFillBuffer = 0;
jmethodID JniUtil::methRestart = 0;
jmethodID JniUtil::methGetJavaStreamHandle = 0;
jmethodID JniUtil::methGetIndexRoot = 0;
jmethodID JniUtil::methToString = 0;
jmethodID JniUtil::methBase64Decode;
jclass JniUtil::classRhBase64;

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
    classRhBase64 = pEnv->FindClass("org/eigenbase/util/RhBase64");
    jclass classJavaTupleStream = pEnv->FindClass(
        "net/sf/farrago/runtime/JavaTupleStream");
    jclass classJavaPullTupleStream = pEnv->FindClass(
        "net/sf/farrago/runtime/JavaPullTupleStream");
    jclass classFennelJavaStreamMap = pEnv->FindClass(
        "net/sf/farrago/fennel/FennelJavaStreamMap");
    methGetClassName = pEnv->GetMethodID(
        classClass,"getName","()Ljava/lang/String;");
    methIterator = pEnv->GetMethodID(
        classCollection,"iterator","()Ljava/util/Iterator;");
    methHasNext = pEnv->GetMethodID(
        classIterator,"hasNext","()Z");
    methNext = pEnv->GetMethodID(
        classIterator,"next","()Ljava/lang/Object;");
    methFillBuffer = pEnv->GetMethodID(
        classJavaPullTupleStream,"fillBuffer","(Ljava/nio/ByteBuffer;)I");
    methRestart = pEnv->GetMethodID(
        classJavaTupleStream,"restart","()V");
    methGetJavaStreamHandle = pEnv->GetMethodID(
        classFennelJavaStreamMap,"getJavaStreamHandle",
        "(I)J");
    methGetIndexRoot = pEnv->GetMethodID(
        classFennelJavaStreamMap,"getIndexRoot",
        "(J)J");
    methToString = pEnv->GetMethodID(
        classObject,"toString","()Ljava/lang/String;");
    methBase64Decode = pEnv->GetStaticMethodID(
        classRhBase64,"decode","(Ljava/lang/String;)[B");
    return jniVersion;
}

JNIEnv *JniUtil::getJavaEnv()
{
    void *pEnv;
    // REVIEW:  need to DetachCurrentThread somewhere?
    jint rc = pVm->AttachCurrentThreadAsDaemon(&pEnv,NULL);
    assert(rc == JNI_OK);
    assert(pEnv);
    return static_cast<JNIEnv *>(pEnv);
}

std::string JniUtil::getClassName(jclass jClass)
{
    JniEnvAutoRef pEnv;
    jstring jString = reinterpret_cast<jstring>(
        pEnv->CallObjectMethod(jClass,methGetClassName));
    assert(jString);
    return toStdString(pEnv,jString);
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


JniExceptionChecker::~JniExceptionChecker()
{
    jthrowable excn = pEnv->ExceptionOccurred();
    if (excn) {
        throw JavaExcn(excn);
    }
}

JniEnvAutoRef::JniEnvAutoRef()
    : JniEnvRef(JniUtil::getJavaEnv())
{
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

FENNEL_END_CPPFILE("$Id$");

// End JniUtil.cpp
