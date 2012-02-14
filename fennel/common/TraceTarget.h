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

#ifndef Fennel_TraceTarget_Included
#define Fennel_TraceTarget_Included

FENNEL_BEGIN_NAMESPACE

/**
 * Trace severity levels.  Except for TRACE_PERFCOUNTER_*, the values correspond
 * to java.util.logging.Level.  TRACE_PERFCOUNTER_* are Fennel-specific
 * values used to communicate performance-counter information from
 * a StatsSource to a JavaTraceTarget (which doubles as a StatsTarget).
 */
enum TraceLevel {
    TRACE_PERFCOUNTER_BEGIN_SNAPSHOT = 20002,
    TRACE_PERFCOUNTER_END_SNAPSHOT = 20001,
    TRACE_PERFCOUNTER_UPDATE = 20000,
    TRACE_OFF = 10000,
    TRACE_SEVERE = 1000,
    TRACE_WARNING = 900,
    TRACE_INFO = 800,
    TRACE_CONFIG = 700,
    TRACE_FINE = 500,
    TRACE_FINER = 400,
    TRACE_FINEST = 300
};

/**
 * TraceTarget defines a tracing interface to be implemented by callers to
 * Fennel.
 */
class FENNEL_COMMON_EXPORT TraceTarget
{
public:
    virtual ~TraceTarget();

    /**
     * Receives notification when a trace event occurs.
     *
     * @param source the facility from which the message originated
     *
     * @param level the trace event severity level
     *
     * @param message the text of the message
     */
    virtual void notifyTrace(
        std::string source,
        TraceLevel level,
        std::string message) = 0;

    /**
     * Gets the level at which a particular source should be traced.
     *
     * @param source name of source to be traced
     *
     * @return minimum severity level which should be traced
     */
    virtual TraceLevel getSourceTraceLevel(
        std::string source) = 0;
};

FENNEL_END_NAMESPACE

#endif

// End TraceTarget.h
