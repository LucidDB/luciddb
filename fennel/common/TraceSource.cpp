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
#include "fennel/common/TraceSource.h"
#include "fennel/common/TraceTarget.h"

FENNEL_BEGIN_CPPFILE("$Id$");

TraceSource::TraceSource()
{
    minimumLevel = TRACE_OFF;
}

TraceSource::TraceSource(
    SharedTraceTarget pTraceTargetInit,
    std::string nameInit)
{
    initTraceSource(pTraceTargetInit, nameInit);
}

TraceSource::~TraceSource()
{
}

void TraceSource::initTraceSource(
    SharedTraceTarget pTraceTargetInit,
    std::string nameInit)
{
    assert(!pTraceTarget.get());

    pTraceTarget = pTraceTargetInit;
    name = nameInit;
    if (isTracing()) {
        minimumLevel = pTraceTarget->getSourceTraceLevel(name);
    } else {
        minimumLevel = TRACE_OFF;
    }
}

void TraceSource::trace(TraceLevel level, std::string message) const
{
    if (isTracing()) {
        getTraceTarget().notifyTrace(name, level, message);
    }
}

void TraceSource::disableTracing()
{
    pTraceTarget.reset();
    minimumLevel = TRACE_OFF;
}

FENNEL_END_CPPFILE("$Id$");

// End TraceSource.cpp
