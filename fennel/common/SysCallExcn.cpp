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
#include "fennel/common/SysCallExcn.h"
#include "fennel/common/FennelResource.h"
#include <errno.h>
#include <sstream>

#ifdef __MSVC__
#include <windows.h>
#endif

FENNEL_BEGIN_CPPFILE("$Id$");

SysCallExcn::SysCallExcn(std::string msgInit)
    : FennelExcn(msgInit), errCode(getCurrentErrorCode())
{
    init();
}

SysCallExcn::SysCallExcn(std::string msgInit, int errCodeInit)
    : FennelExcn(msgInit), errCode(errCodeInit)
{
    init();
}

void SysCallExcn::init()
{
    std::ostringstream oss;
    oss << msg;
    oss << ": ";

#ifdef __MSVC__
    oss << "GetLastError() = ";
    oss << errCode;
#else
    char *pMsg = strerror(errCode);
    if (pMsg) {
        oss << pMsg;
    } else {
        oss << "errno = ";
        oss << errCode;
    }
#endif
    msg = oss.str();
    msg = FennelResource::instance().sysCallFailed(msg);
}


int SysCallExcn::getErrorCode()
{
    return errCode;
}

int SysCallExcn::getCurrentErrorCode()
{
#ifdef __MING32__
    return GetLastError();
#else
    return errno;
#endif
}

FENNEL_END_CPPFILE("$Id$");

// End SysCallExcn.cpp
