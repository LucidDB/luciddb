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

#ifndef Fennel_SynchObj_Included
#define Fennel_SynchObj_Included

#include <boost/thread/mutex.hpp>
#include <boost/thread/recursive_mutex.hpp>
#include <boost/thread/xtime.hpp>
#include <boost/thread/condition.hpp>

FENNEL_BEGIN_NAMESPACE

typedef boost::recursive_mutex RecursiveMutex;
typedef boost::mutex StrictMutex;
typedef boost::recursive_mutex::scoped_lock RecursiveMutexGuard;
typedef boost::mutex::scoped_lock StrictMutexGuard;
typedef boost::condition_variable LocalCondition;

extern void FENNEL_SYNCH_EXPORT convertTimeout(uint iMillis, boost::xtime &);

FENNEL_END_NAMESPACE

#endif

// End SynchObj.h
