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

#ifndef Fennel_ErrorTarget_Included
#define Fennel_ErrorTarget_Included

#include "fennel/tuple/TupleDescriptor.h"

FENNEL_BEGIN_NAMESPACE

/**
 * Row error severity levels. Keep this consistent with
 * net.sf.farrago.NativeRuntimeContext
 */
enum ErrorLevel
{
    ROW_ERROR = 1000,
    ROW_WARNING = 500
};

/**
 * ErrorTarget defines an interface for receiving Fennel row errors.
 * Typically, many or all ErrorSouce instances post errors to the same
 * ErrorTarget.
 */
class FENNEL_EXEC_EXPORT ErrorTarget
{
public:

    virtual ~ErrorTarget();

    /**
     * Receives notification when a row exception occurs.
     *
     * @param source the unique Fennel stream name
     *
     * @param level the severity of the exception
     *
     * @param message a description of the exception
     *
     * @param address pointer to the buffer containing the error record
     *
     * @param capacity the size of the error buffer
     *
     * @param index position of the column whose processing caused the
     *   exception to occur. -1 indicates that no column was culpable.
     *   0 indicates that a filter condition was being processed. Otherwise
     *   this parameter should be a 1-indexed column position.
     */
    virtual void notifyError(
        const std::string &source,
        ErrorLevel level,
        const std::string &message,
        void *address,
        long capacity,
        int index) = 0;
};

FENNEL_END_NAMESPACE

#endif

// End ErrorTarget.h
