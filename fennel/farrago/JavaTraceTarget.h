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

#ifndef Fennel_JavaTraceTarget_Included
#define Fennel_JavaTraceTarget_Included

#include "fennel/common/TraceTarget.h"
#include "fennel/common/StatsTarget.h"
#include "fennel/farrago/JniUtil.h"

FENNEL_BEGIN_NAMESPACE

/**
 * JavaTraceTarget implements TraceTarget by calling back into the
 * java.util.logging facility.  It also implements StatsTarget by
 * converting performance counter updates into trace events which
 * are published inside of Java.
 */
class FENNEL_FARRAGO_EXPORT JavaTraceTarget
    : public TraceTarget, public StatsTarget
{
    /**
     * net.sf.farrago.util.NativeTrace object to which trace messages should be
     * written.
     */
    jobject javaTrace;

    /**
     * Method NativeTrace.trace.
     */
    jmethodID methTrace;

    /**
     * Method NativeTrace.getSourceTraceLevel.
     */
    jmethodID methGetSourceTraceLevel;

public:
    explicit JavaTraceTarget();

    explicit JavaTraceTarget(
        jobject javaTraceInit, jmethodID methTraceInit,
        jmethodID methGetSourceTraceLevelInit);

    virtual ~JavaTraceTarget();

    // implement TraceTarget
    virtual void notifyTrace(
        std::string source, TraceLevel level, std::string message);
    virtual TraceLevel getSourceTraceLevel(std::string source);

    // implement StatsTarget
    virtual void beginSnapshot();
    virtual void endSnapshot();
    virtual void writeCounter(std::string name, int64_t value);
    virtual void onThreadStart();
    virtual void onThreadEnd();
};

FENNEL_END_NAMESPACE

#endif

// End JavaTraceTarget.h
