/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/

#include "fennel/common/CommonPreamble.h"
#include "fennel/farrago/JavaTraceTarget.h"

#include "boost/lexical_cast.hpp"

FENNEL_BEGIN_CPPFILE("$Id$");

#define JAVATRACETARGET_TYPE_STR ("JavaTraceTarget")

JavaTraceTarget::JavaTraceTarget()
{
    JniEnvAutoRef pEnv;
    jclass classNativeTrace = pEnv->FindClass(
        "net/sf/farrago/util/NativeTrace");

    jmethodID methInstance =
        pEnv->GetStaticMethodID(
            classNativeTrace, "instance",
            "()Lnet/sf/farrago/util/NativeTrace;");

    jobject javaTraceInit =
        pEnv->CallStaticObjectMethod(classNativeTrace, methInstance);

    JniUtil::incrementHandleCount(JAVATRACETARGET_TYPE_STR, this);
    javaTrace = pEnv->NewGlobalRef(javaTraceInit);

    // TODO:  convert to Java excn instead
    assert(javaTrace);

    methTrace = pEnv->GetMethodID(
        classNativeTrace, "trace",
        "(Ljava/lang/String;ILjava/lang/String;)V");
    methGetSourceTraceLevel = pEnv->GetMethodID(
        classNativeTrace, "getSourceTraceLevel",
        "(Ljava/lang/String;)I");
}

JavaTraceTarget::JavaTraceTarget(
    jobject javaTraceInit, jmethodID methTraceInit,
    jmethodID methGetSourceTraceLevelInit)
{
    JniEnvAutoRef pEnv;

    JniUtil::incrementHandleCount(JAVATRACETARGET_TYPE_STR, this);
    javaTrace = pEnv->NewGlobalRef(javaTraceInit);

    // TODO:  convert to Java excn instead
    assert(javaTrace);

    methTrace = methTraceInit;
    methGetSourceTraceLevel = methGetSourceTraceLevelInit;
}

JavaTraceTarget::~JavaTraceTarget()
{
    JniEnvAutoRef pEnv;

    pEnv->DeleteGlobalRef(javaTrace);
    JniUtil::decrementHandleCount(JAVATRACETARGET_TYPE_STR, this);

    javaTrace = NULL;
}

void JavaTraceTarget::notifyTrace(
    std::string source, TraceLevel level, std::string message)
{
    JniEnvAutoRef pEnv;

    // NOTE jvs 21-Aug-2007:  use ref reapers here since this
    // may be called over and over before control returns to Java

    jstring javaSource = pEnv->NewStringUTF(source.c_str());
    JniLocalRefReaper javaSourceReaper(pEnv, javaSource);
    jstring javaMessage = pEnv->NewStringUTF(message.c_str());
    JniLocalRefReaper javaMessageReaper(pEnv, javaMessage);
    pEnv->CallVoidMethod(javaTrace, methTrace, javaSource, level, javaMessage);
}

TraceLevel JavaTraceTarget::getSourceTraceLevel(std::string source)
{
    JniEnvAutoRef pEnv;
    jstring javaSource = pEnv->NewStringUTF(source.c_str());
    int level = pEnv->CallIntMethod(
        javaTrace, methGetSourceTraceLevel, javaSource);
    return static_cast<TraceLevel>(level);
}

void JavaTraceTarget::beginSnapshot()
{
    notifyTrace(
        "", TRACE_PERFCOUNTER_BEGIN_SNAPSHOT, "");
}

void JavaTraceTarget::endSnapshot()
{
    notifyTrace(
        "", TRACE_PERFCOUNTER_END_SNAPSHOT, "");
}

void JavaTraceTarget::writeCounter(std::string name, int64_t value)
{
    std::string s = boost::lexical_cast<std::string>(value);
    notifyTrace(
        name, TRACE_PERFCOUNTER_UPDATE, s);
}

void JavaTraceTarget::onThreadStart()
{
    JniEnvAutoRef pEnv;
    // We want to stay attached for the duration of the timer thread,
    // so suppress detach here and do it explicitly in onThreadEnd
    // instead.  See comments on suppressDetach about the need for a
    // cleaner approach to attaching native-spawned threads.
    pEnv.suppressDetach();
}

void JavaTraceTarget::onThreadEnd()
{
    JniUtil::detachJavaEnv();
}

FENNEL_END_CPPFILE("$Id$");

// End JavaTraceTarget.cpp
