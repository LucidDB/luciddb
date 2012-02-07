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

#ifndef Fennel_StatsTarget_Included
#define Fennel_StatsTarget_Included

FENNEL_BEGIN_NAMESPACE

/**
 * StatsTarget defines an interface implemented by classes which consume
 * performance/activity statistics.
 */
class FENNEL_COMMON_EXPORT StatsTarget
{
public:
    virtual ~StatsTarget();

    /**
     * Receives notification that stats polling is starting via a TimerThread.
     */
    virtual void onThreadStart();

    /**
     * Receives notification that stats polling via a TimerThread is ending.
     */
    virtual void onThreadEnd();

    /**
     * Begins recording a snapshot.  Called before all writeCounter invocations
     * for the snapshot.
     */
    virtual void beginSnapshot() = 0;

    /**
     * Finishes recording a snapshot.  Called after all writeCounter invocations
     * for the snapshot.
     */
    virtual void endSnapshot() = 0;

    /**
     * Writes one int counter.  This is called from a StatsSource
     * implementation in response to writeStats().
     *
     * @param name name of counter
     *
     * @param value snapshot value
     */
    virtual void writeCounter(std::string name, int64_t value) = 0;
};

FENNEL_END_NAMESPACE

#endif

// End StatsTarget.h
