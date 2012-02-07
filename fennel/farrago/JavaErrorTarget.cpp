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
#include "fennel/farrago/JavaErrorTarget.h"
#include "fennel/farrago/JavaExcn.h"

#include "boost/lexical_cast.hpp"

FENNEL_BEGIN_CPPFILE("$Id$");

#define JAVAERRORTARGET_TYPE_STR ("JavaErrorTarget")

JavaErrorTarget::JavaErrorTarget(jobject javaErrorInit)
{
    JniEnvAutoRef pEnv;

    JniUtil::incrementHandleCount(JAVAERRORTARGET_TYPE_STR, this);
    javaError = pEnv->NewGlobalRef(javaErrorInit);

    jclass classErrorTarget = pEnv->FindClass(
        "net/sf/farrago/fennel/FennelJavaErrorTarget");
    methNotifyError = pEnv->GetMethodID(
        classErrorTarget,
        "handleRowError",
        "(Ljava/lang/String;ZLjava/lang/String;Ljava/nio/ByteBuffer;I)Ljava/lang/Object;");
}

JavaErrorTarget::~JavaErrorTarget()
{
    JniEnvAutoRef pEnv;

    pEnv->DeleteGlobalRef(javaError);
    JniUtil::decrementHandleCount(JAVAERRORTARGET_TYPE_STR, this);
    javaError = NULL;
}

void JavaErrorTarget::notifyError(
    const std::string &source,
    ErrorLevel level,
    const std::string &message,
    void *address,
    long capacity,
    int index)
{
    JniEnvAutoRef pEnv;

    // NOTE jvs 21-Aug-2007:  use ref reapers here since this
    // may be called over and over before control returns to Java

    jstring javaSource = pEnv->NewStringUTF(source.c_str());
    JniLocalRefReaper javaSourceReaper(pEnv, javaSource);
    jboolean javaIsWarning = (level == ROW_WARNING) ? true : false;
    jstring javaMessage = pEnv->NewStringUTF(message.c_str());
    JniLocalRefReaper javaMessageReaper(pEnv, javaMessage);
    jobject javaByteBuffer =
        pEnv->NewDirectByteBuffer(address, capacity);
    JniLocalRefReaper javaByteBufferReaper(pEnv, javaByteBuffer);
    jint javaIndex = index;

    pEnv->CallObjectMethod(
        javaError, methNotifyError,
        javaSource, javaIsWarning, javaMessage,
        javaByteBuffer, javaIndex);
}

FENNEL_END_CPPFILE("$Id$");

// End JavaErrorTarget.cpp
