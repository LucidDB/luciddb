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

#ifndef Fennel_GroupLock_Included
#define Fennel_GroupLock_Included

#include "fennel/synch/SynchMonitoredObject.h"

FENNEL_BEGIN_NAMESPACE

/**
 * GroupLock is a synchronization object for enforcing mutual exclusion among
 * an indefinite number of groups with indefinite cardinalities.  As an
 * example, suppose you only had a single bathroom, and you wanted to prevent
 * members of the opposite sex from occupying it simultaneously.  You could do
 * this by slapping a GroupLock on the door; men would enter with group key "1"
 * and women would enter with group key "2"; the GroupLock would allow any
 * number of men to enter together, or any number of women, but would never
 * allow them to mix.  In this case, there are only two groups, but any number
 * of groups is supported, as long as they have unique integer identifiers.
 * Note that there are no provisions for preventing starvation, or whatever the
 * equally unpleasant equivalent is in this example.
 */
class FENNEL_SYNCH_EXPORT GroupLock : public SynchMonitoredObject
{
    uint nHolders;
    uint iHeldGroup;

public:
    explicit GroupLock();
    ~GroupLock();

    bool waitFor(uint iGroup, uint iTimeout = ETERNITY);

    /**
     * // TODO:  pass the group key to release as well,
     * and assert that it matches iHeldGroup
     */
    void release();
};

FENNEL_END_NAMESPACE

#endif

// End GroupLock.h
