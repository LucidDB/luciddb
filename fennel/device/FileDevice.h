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

#ifndef Fennel_FileDevice_Included
#define Fennel_FileDevice_Included

#include "fennel/device/DeviceMode.h"
#include "fennel/synch/SynchObj.h"

FENNEL_BEGIN_NAMESPACE

class DeviceIOListener;
class RandomAccessRequest;

/**
 * FileDevice is a base class for devices built atop the OS file system.
 */
class FENNEL_DEVICE_EXPORT FileDevice
{
protected:
    // REVIEW:  should this be synchronized here or at a higher level?
    /**
     * current file size in bytes
     */
    FileSize cbFile;

    /**
     * the opened OS file
     */
    int handle;

    /**
     * mode in which file was opened
     */
    DeviceMode mode;

    /**
     * path to file in file system
     */
    std::string filename;

    /**
     * On Cygwin, there's no pread/pwrite, so all I/O has to be
     * synchronized per device.
     */
    StrictMutex mutex;

public:
    /**
     * Opens a file device.
     *
     * @param filename path to file
     * @param mode modifiers for how to open file
     * @param initialSize the initial size (in bytes) of the device, if
     * creating a new file
     */
    FileDevice(std::string filename, DeviceMode mode, FileSize initialSize);
    virtual ~FileDevice();

    /**
     * Executes a synchronous transfer request for a single
     * random access binding.  This is an all-or-nothing request,
     * so unless the result size is equal to the requested
     * transfer size, the request is considered a failure.
     * However, no exception is thrown when failure occurs;
     * instead, the binding notification method is called.
     *
     * @param request transfer specification; must have exactly
     * one binding
     */
    void transfer(RandomAccessRequest const &request);

    void flush();

    void close();

    /**
     * @return whether the device file is currently open
     */
    bool isOpen() const
    {
        return handle == -1 ? 0 : 1;
    }

    FileSize getSizeInBytes()
    {
        return cbFile;
    }

    void setSizeInBytes(FileSize cbNew);
};

FENNEL_END_NAMESPACE

#endif

// End FileDevice.h
