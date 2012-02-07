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

#ifndef Fennel_ErrorSource_Included
#define Fennel_ErrorSource_Included

#include "fennel/exec/ErrorTarget.h"
#include "fennel/tuple/TupleAccessor.h"

#include <sstream>

FENNEL_BEGIN_NAMESPACE

/**
 * ErrorSource is a common base for all classes that post row exceptions to
 * an ErrorTarget. An ErrorSource generally corresponds to an ExecStream.
 * Fennel data is typiclly interpreted as external data via separate
 * metadata, so it is usually required for an ErrorSource to be
 * preregistered with the external system. (For example, a Fennel long might
 * map to a decimal or datetime type.)
 */
class FENNEL_EXEC_EXPORT ErrorSource
{
    SharedErrorTarget pErrorTarget;
    std::string name;

    /**
     * Tuple accessor for error records handed by this error source
     */
    TupleAccessor errorAccessor;

    /**
     * A buffer for error records
     */
    boost::shared_ptr<FixedBuffer> pErrorBuf;

protected:
    /**
     * Constructs a new uninitialized ErrorSource.
     */
    explicit ErrorSource();

    /**
     * Constructs a new ErrorSource.
     *
     * @param pErrorTarget the ErrorTarget to which errors will be posted,
     * or NULL to ignore errors.
     *
     * @param name the unique name of this source, such as the name of
     *   a Fennel ExecStream
     */
    explicit ErrorSource(
        SharedErrorTarget pErrorTarget,
        const std::string &name);

public:
    virtual ~ErrorSource();

    /**
     * For use when initialization has to be deferred until after construction.
     *
     * @param pErrorTarget the ErrorTarget to which errors will be posted
     *
     * @param name the name of this source
     */
    virtual void initErrorSource(
        SharedErrorTarget pErrorTarget,
        const std::string &name);

    /**
     * Posts an exception, such as a row exception.
     *
     * @see ErrorTarget for a description of the parameters
     */
    void postError(
        ErrorLevel level, const std::string &message,
        void *address, long capacity, int index);

    /**
     * Posts an exception, such as a row exception.
     *
     * @see ErrorTarget for a description of the parameters
     */
    void postError(
        ErrorLevel level, const std::string &message,
        const TupleDescriptor &errorDesc, const TupleData &errorTuple,
        int index);

    /**
     * @return true iff an error target has been set
     */
    bool hasTarget() const
    {
        return pErrorTarget.get() ? true : false;
    }

    /**
     * @return the ErrorTarget for this source
     */
    ErrorTarget &getErrorTarget() const
    {
        assert(hasTarget());
        return *(pErrorTarget.get());
    }

    /**
     * @return the SharedErrorTarget for this source
     */
    SharedErrorTarget getSharedErrorTarget() const
    {
        return pErrorTarget;
    }

    /**
     * Gets the name of this source. Useful to construct nested names for
     * subcomponents that are also ErrorSources.
     * @return the name
     */
    std::string getErrorSourceName() const
    {
        return name;
    }

    /**
     * Sets the name of this source. Useful to construct dynamic names for
     * fine-grained filtering.
     */
    void setErrorSourceName(std::string const& n)
    {
        name = n;
    }

    /**
     * Forgets the current target if any.
     */
    void disableTarget();
};

FENNEL_END_NAMESPACE

#endif

// End ErrorSource.h
