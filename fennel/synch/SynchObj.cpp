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
#include "fennel/synch/SynchObj.h"
#include "fennel/synch/Thread.h"
#include "fennel/synch/NullMutex.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void convertTimeout(uint iMilliseconds, boost::xtime &atv)
{
    boost::xtime_get(&atv,boost::TIME_UTC);
    if (isMAXU(iMilliseconds)) {
        // FIXME:  Solaris doesn't like bogus huge times like
        // ACE_Time_Value::max_time, so this uses NOW+10HRS.  Instead, should
        // precalculate a real time much farther in the future and keep it
        // around as a singleton.
        atv.sec += 36000;
    } else if (iMilliseconds) {
        long sec = iMilliseconds / 1000;
        long nsec = (iMilliseconds % 1000) * 1000000;
        atv.sec += sec;
        atv.nsec += nsec;
    }
}

// force references to some classes which aren't referenced elsewhere
#ifdef __MSVC__
class UnreferencedSynchStructs
{
    NullMutex nullMutex;
};
#endif

FENNEL_END_CPPFILE("$Id$");

// End SynchObj.cpp
