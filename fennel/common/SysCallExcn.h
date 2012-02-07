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

#ifndef Fennel_SysCallExcn_Included
#define Fennel_SysCallExcn_Included

#include "fennel/common/FennelExcn.h"

FENNEL_BEGIN_NAMESPACE

/**
 * Exception class for failed system calls.
 */
class FENNEL_COMMON_EXPORT SysCallExcn : public FennelExcn
{
private:
    int errCode;

    void init();

public:
    /**
     * Constructs a new SysCallExcn.  This should be called immediately after
     * the failed system call in order to get the correct information from the
     * OS.
     *
     * @param msgInit a description of the failure from the program's point of
     * view; SysCallExcn will append additional information from the OS
     */
    explicit SysCallExcn(std::string msgInit);

    /**
     * Constructs a new SysCallExcn.  This may be deferred until some time
     * after the failed system call, as long as the OS error code has been
     * saved.
     *
     * @param msgInit a description of the failure from the program's point of
     * view; SysCallExcn will append additional information from the OS
     *
     * @param errCodeInit OS error code used to generate additional
     * information
     */
    explicit SysCallExcn(std::string msgInit, int errCodeInit);

    /**
     * Returns the error code that caused this SysCallExcn.
     */
    int getErrorCode();

    /**
     * Returns the current OS error code.  This function may be used to
     * retrieve an error code for use with the 2 argument constructor.
     * The function should be called immediately after the failed system
     * call in order to get the correct information from the OS.
     *
     * @return the current OS error code
     */
    static int getCurrentErrorCode();
};

FENNEL_END_NAMESPACE

#endif

// End SysCallExcn.h
