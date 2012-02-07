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

#ifndef Fennel_TraceSource_Included
#define Fennel_TraceSource_Included

#include <sstream>
#include "fennel/common/TraceTarget.h"

FENNEL_BEGIN_NAMESPACE

/**
 * TraceSource is a common base for all classes which write messages to
 * a TraceTarget.
 */
class FENNEL_COMMON_EXPORT TraceSource
{
    SharedTraceTarget pTraceTarget;

    std::string name;

    TraceLevel minimumLevel;

protected:
    /**
     * Constructs a new uninitialized TraceSource.
     */
    explicit TraceSource();

    /**
     * Constructs a new TraceSource.
     *
     * @param pTraceTarget the TraceTarget to which messages will be sent,
     * or NULL to disable tracing entirely
     *
     * @param name the name of this source (can be empty string for
     * deferred init)
     */
    explicit TraceSource(
        SharedTraceTarget pTraceTarget,
        std::string name);

public:
    virtual ~TraceSource();

    /**
     * For use when initialization has to be deferred until after construction.
     *
     * @param pTraceTarget the TraceTarget to which messages will be sent
     *
     * @param name the name of this source
     */
    virtual void initTraceSource(
        SharedTraceTarget pTraceTarget, std::string name);

    /**
     * Records a trace message.  Normally only called via FENNEL_TRACE.
     *
     * @param level severity level of event being trace
     *
     * @param message the text of the message
     */
    void trace(TraceLevel level, std::string message) const;

    /**
     * @return true iff tracing is enabled for this source
     */
    bool isTracing() const
    {
        return pTraceTarget.get() ? true : false;
    }

    /**
     * Determines whether a particular level is being traced.
     *
     * @param level trace level to test
     *
     * @return true iff tracing is enabled for the given level
     */
    bool isTracingLevel(TraceLevel level) const
    {
        return level >= minimumLevel;
    }

    /**
     * @return the TraceTarget for this source
     */
    TraceTarget &getTraceTarget() const
    {
        assert(isTracing());
        return *(pTraceTarget.get());
    }

    /**
     * @return the SharedTraceTarget for this source
     */
    SharedTraceTarget getSharedTraceTarget() const
    {
        return pTraceTarget;
    }

    /**
     * Gets the name of this source. Useful to construct nested names for
     * subcomponents that are also TraceSources.
     * @return the name
     */
    std::string getTraceSourceName() const
    {
        return name;
    }

    /**
     * Sets the name of this source. Useful to construct dynamic names for
     * fine-grained filtering.
     */
    void setTraceSourceName(std::string const& n)
    {
        name = n;
    }

    TraceLevel getMinimumTraceLevel() const
    {
        return minimumLevel;
    }

    void disableTracing();
};

/**
 * FENNEL_TRACE can be used from within any class which implements
 * TraceSource. FENNEL_DELEGATE_TRACE is used from a lightweight class that
 * isn't a TraceSource but belongs to and traces as one.
 * msg can be an ostream expression like a << b << c.
 */
#define FENNEL_TRACE(level, msg) FENNEL_DELEGATE_TRACE(level, this, msg)
#define FENNEL_DELEGATE_TRACE(level, tracer, msg) \
do { \
    if ((tracer)->isTracingLevel(level)) { \
        std::ostringstream oss; \
        oss << msg; \
        (tracer)->trace(level, oss.str()); \
    } \
} while (false)

// REVIEW jvs 18-Mar-2005:  Why can't we just pass getCurrentThreadId()
// to operator <<?

// return a string id for the current thread
inline char *get_tid(char *tidstr, int cb)
{
    snprintf(tidstr, cb, "%" FMT_INT64, getCurrentThreadId());
    return tidstr;
}

/**
 * FENNEL_TRACE_THREAD can be used from within any class which implements
 * TraceSource.  As FENNEL_TRACE, but also displays the current thread.
 */
#define FENNEL_TRACE_THREAD(level, expr) \
{ \
    char tidstr[32]; \
    FENNEL_TRACE(\
        level, \
        "[thread " << fennel::get_tid(tidstr, sizeof(tidstr)) << "] " \
        << expr); \
}

FENNEL_END_NAMESPACE

#endif

// End TraceSource.h
