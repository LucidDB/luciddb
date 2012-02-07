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

#ifndef Fennel_JavaThreadTracker_Included
#define Fennel_JavaThreadTracker_Included

#include "fennel/synch/ThreadTracker.h"

FENNEL_BEGIN_NAMESPACE

/**
 * JavaThreadTracker implements ThreadTracker by attaching and detaching
 * the Java environment.
 *
 * @author John Sichi
 * @version $Id$
 */
class FENNEL_FARRAGO_EXPORT JavaThreadTracker
    : public ThreadTracker
{
    virtual void onThreadStart();
    virtual void onThreadEnd();
    virtual FennelExcn *cloneExcn(std::exception &ex);
};

FENNEL_END_NAMESPACE

#endif

// End JavaThreadTracker.h
