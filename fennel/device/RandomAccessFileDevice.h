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

#ifndef Fennel_RandomAccessFileDevice_Included
#define Fennel_RandomAccessFileDevice_Included

#include "fennel/device/RandomAccessDevice.h"
#include "fennel/device/FileDevice.h"

FENNEL_BEGIN_NAMESPACE

class RandomAccessRequest;

/**
 * RandomAccessFileDevice is an implementation of RandomAccessDevice in terms
 * of a FileDevice.
 */
class FENNEL_DEVICE_EXPORT RandomAccessFileDevice
    : public RandomAccessDevice, public FileDevice
{
public:
    /**
     * Opens a file device for random access, specifying an initial size on
     * creation.
     *
     * @param filename path to file
     * @param mode modifiers for how to open file
     * @param initialSize the initial size (in bytes) of the device, if
     * creating a new file
     */
    explicit RandomAccessFileDevice(
        std::string filename,
        DeviceMode mode,
        FileSize initialSize);

    /**
     * Opens a file device for random access.
     *
     * @param filename path to file
     * @param mode modifiers for how to open file
     */
    explicit RandomAccessFileDevice(
        std::string filename,
        DeviceMode mode);

// ----------------------------------------------------------------------
// Implementation of RandomAccessDevice interface (q.v.)
// ----------------------------------------------------------------------
    virtual FileSize getSizeInBytes();
    virtual void setSizeInBytes(FileSize cbNew);
    virtual void transfer(RandomAccessRequest const &request);
    virtual void prepareTransfer(RandomAccessRequest &request);
    virtual void flush();
    virtual int getHandle();
};

FENNEL_END_NAMESPACE

#endif

// End RandomAccessFileDevice.h
