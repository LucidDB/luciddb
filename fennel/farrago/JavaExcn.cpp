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
#include "fennel/farrago/JavaExcn.h"

FENNEL_BEGIN_CPPFILE("$Id$");

JavaExcn::JavaExcn(jthrowable javaExceptionInit)
    : FennelExcn("FennelJavaExcn")
{
    javaException = javaExceptionInit;

    // Initialize the msg field to the stack trace. It is necessary to
    // store the stack trace, so that 'what' can hand out a 'const
    // char *'.
    JniEnvAutoRef pEnv;
    jstring s = reinterpret_cast<jstring>(
        pEnv->CallStaticObjectMethod(
            JniUtil::classUtil,
            JniUtil::methUtilGetStackTrace,
            javaException));
    msg = JniUtil::toStdString(pEnv, s);
}

jthrowable JavaExcn::getJavaException() const
{
    return javaException;
}

const std::string& JavaExcn::getStackTrace() const
{
    return msg;
}

void JavaExcn::throwSelf()
{
    throw *this;
}

FENNEL_END_CPPFILE("$Id$");

// End JavaExcn.cpp
