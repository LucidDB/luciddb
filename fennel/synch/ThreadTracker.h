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

#ifndef Fennel_ThreadTracker_Included
#define Fennel_ThreadTracker_Included

FENNEL_BEGIN_NAMESPACE

class FennelExcn;

/**
 * ThreadTracker defines an interface for receiving callbacks
 * before and after a thread runs.  The default implementation
 * is a dummy (stub methods doing nothing).
 *
 * @author John Sichi
 * @version $Id$
 */
class FENNEL_SYNCH_EXPORT ThreadTracker
{
public:
    virtual ~ThreadTracker();

    /**
     * Called in new thread context before thread's body runs.
     */
    virtual void onThreadStart();

    /**
     * Called in thread context after thread's body runs.
     */
    virtual void onThreadEnd();

    /**
     * Clones an exception so that it can be rethrown in
     * a different thread context.
     *
     * @param ex the excn to be cloned
     *
     * @return cloned excn
     */
    virtual FennelExcn *cloneExcn(std::exception &ex);
};

FENNEL_END_NAMESPACE

#endif

// End ThreadTracker.h
