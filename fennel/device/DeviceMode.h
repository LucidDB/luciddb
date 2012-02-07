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

#ifndef Fennel_DeviceMode_Included
#define Fennel_DeviceMode_Included

FENNEL_BEGIN_NAMESPACE

struct FENNEL_DEVICE_EXPORT DeviceMode
{
    bool create : 1;
    bool readOnly : 1;
    bool temporary : 1;
    bool direct : 1;
    bool sequential : 1;

    enum Initializer { load = 0, createNew = 1 };

    explicit DeviceMode()
    {
        init();
    }

    DeviceMode(Initializer i)
    {
        init();
        create = i;
    }

    DeviceMode(DeviceMode const &mode)
    {
        init(mode);
    }

    void operator = (DeviceMode const &mode)
    {
        init(mode);
    }

private:
    void init()
    {
        create = 0;
        readOnly = 0;
        temporary = 0;
        direct = 0;
        sequential = 0;
    }

    void init(DeviceMode const &mode)
    {
        create = mode.create;
        readOnly = mode.readOnly;
        temporary = mode.temporary;
        direct = mode.direct;
        sequential = mode.sequential;
    }
};

FENNEL_END_NAMESPACE

#endif

// End DeviceMode.h
