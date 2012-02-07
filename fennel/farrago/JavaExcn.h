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

#ifndef Fennel_JavaExcn_Included
#define Fennel_JavaExcn_Included

#include "fennel/common/FennelExcn.h"

#include "fennel/farrago/JniUtil.h"

FENNEL_BEGIN_NAMESPACE

/**
 * Exception class for wrapping Java exceptions.
 *
 *<p>
 *
 * REVIEW jvs 23-Aug-2007:  If any code actually handles one of these
 * and carries on, it may need to delete the local jthrowable reference
 * to avoid a leak.
 */
class FENNEL_FARRAGO_EXPORT JavaExcn
    : public FennelExcn
{
    jthrowable javaException;

public:
    /**
     * Constructs a new JavaExcn.
     *
     * @param javaExceptionInit the wrapped Java exception
     */
    explicit JavaExcn(
        jthrowable javaExceptionInit);

    /**
     * @return the wrapped Java exception
     */
    jthrowable getJavaException() const;

    /**
     * @return the stack trace
     */
    const std::string& getStackTrace() const;

    // override FennelExcn
    virtual void throwSelf();
};

FENNEL_END_NAMESPACE

#endif

// End JavaExcn.h
