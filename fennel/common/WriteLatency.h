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

#ifndef Fennel_WriteLatency_Included
#define Fennel_WriteLatency_Included

FENNEL_BEGIN_NAMESPACE

/**
 * Options controlling when to flush dirty data.
 */
enum WriteLatency
{
    WRITE_LAZY,
    WRITE_EAGER_ASYNC,
    WRITE_EAGER_SYNC
};

FENNEL_END_NAMESPACE

#endif

// End WriteLatency.h
