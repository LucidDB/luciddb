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

#ifndef Fennel_RandomAccessNullDevice_Included
#define Fennel_RandomAccessNullDevice_Included

#include "fennel/device/RandomAccessDevice.h"

FENNEL_BEGIN_NAMESPACE

class RandomAccessRequest;

/**
 * RandomAccessNullDevice is an implementation of RandomAccessDevice which acts
 * something like /dev/null, except that it does not allow any transfers at
 * all.
 */
class FENNEL_DEVICE_EXPORT RandomAccessNullDevice
    : public RandomAccessDevice
{
public:
    /**
     * Creates a new null device.
     */
    explicit RandomAccessNullDevice();

// ----------------------------------------------------------------------
// Implementation of RandomAccessDevice interface (q.v.)
// ----------------------------------------------------------------------
    FileSize getSizeInBytes();
    void setSizeInBytes(FileSize cbNew);
    void transfer(RandomAccessRequest const &request);
    void prepareTransfer(RandomAccessRequest &request);
    void flush();
    int getHandle();
};

FENNEL_END_NAMESPACE

#endif

// End RandomAccessNullDevice.h
