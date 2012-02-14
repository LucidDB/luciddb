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
#include "fennel/farrago/JavaThreadTracker.h"
#include "fennel/farrago/JniUtil.h"
#include "fennel/farrago/JavaExcn.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void JavaThreadTracker::onThreadStart()
{
    JniEnvAutoRef pEnv;
    // We want to stay attached for the duration of the timer thread,
    // so suppress detach here and do it explicitly in onThreadEnd
    // instead.  See comments on suppressDetach about the need for a
    // cleaner approach to attaching native-spawned threads.
    pEnv.suppressDetach();
}

void JavaThreadTracker::onThreadEnd()
{
    JniUtil::detachJavaEnv();
}

FennelExcn *JavaThreadTracker::cloneExcn(std::exception &ex)
{
    JavaExcn *pJavaExcn = dynamic_cast<JavaExcn *>(&ex);
    if (!pJavaExcn) {
        return ThreadTracker::cloneExcn(ex);
    }
    return new JavaExcn(pJavaExcn->getJavaException());
}

FENNEL_END_CPPFILE("$Id$");

// End JavaThreadTracker.cpp
