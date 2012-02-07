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

#ifndef Fennel_JavaErrorTarget_Included
#define Fennel_JavaErrorTarget_Included

#include "fennel/exec/ErrorTarget.h"
#include "fennel/farrago/JniUtil.h"

FENNEL_BEGIN_NAMESPACE

/**
 * JavaErrorTarget implements ErrorTarget by calling back into the
 * Farrago error handling facility.
 */
class FENNEL_FARRAGO_EXPORT JavaErrorTarget
    : public ErrorTarget
{
    /**
     * net.sf.farrago.fennel.FennelJavaErrorTarget object to which
     * errors should be forwarded.
     */
    jobject javaError;

    /**
     * FennelJavaErrorTarget.handleRowError method
     */
    jmethodID methNotifyError;

public:
    ~JavaErrorTarget();

    /**
     * Constructs a new JavaErrorTarget
     *
     * @param javaErrorInit pointer to a java object of type
     *   net.sf.farrago.fennel.FennelJavaErrorTarget
     */
    explicit JavaErrorTarget(jobject javaErrorInit);

    // implement ErrorTarget
    virtual void notifyError(
        const std::string &source,
        ErrorLevel level,
        const std::string &message,
        void *address,
        long capacity,
        int index);
};

FENNEL_END_NAMESPACE

#endif

// End JavaErrorTarget.h
