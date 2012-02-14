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

#ifndef Fennel_DynamicParam_Included
#define Fennel_DynamicParam_Included

#include "fennel/common/SharedTypes.h"
#include "fennel/tuple/TupleData.h"
#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/synch/SynchObj.h"

#include <map>
#include <boost/scoped_array.hpp>


FENNEL_BEGIN_NAMESPACE

/**
 * Dynamic parameters are parameters (physically tuples) shared amongst streams.
 *
 * @author Wael Chatila
 * @version $Id$
 */

class FENNEL_EXEC_EXPORT DynamicParam
{
    friend class DynamicParamManager;

    boost::scoped_array<FixedBuffer> pBuffer;
    TupleAttributeDescriptor desc;
    TupleDatum datum;
    bool isCounter;

public:
    explicit DynamicParam(
        TupleAttributeDescriptor const &desc,
        bool isCounter = false);
    inline TupleDatum const &getDatum() const;
    inline TupleAttributeDescriptor const &getDesc() const;
};

/**
 * DynamicParamManager defines methods to allocate, access, and deallocate
 * dynamic parameters.  It is multi-thread safe (but see warning
 * on getParam).
 */
class FENNEL_EXEC_EXPORT DynamicParamManager
{
    typedef std::map<DynamicParamId, SharedDynamicParam> ParamMap;
    typedef ParamMap::const_iterator ParamMapConstIter;

    StrictMutex mutex;

    ParamMap paramMap;

    DynamicParam &getParamInternal(DynamicParamId dynamicParamId);

    void createParam(
        DynamicParamId dynamicParamId,
        SharedDynamicParam param,
        bool failIfExists);
public:
    /**
     * Creates a new dynamic parameter.  Initially, a dynamic parameter
     * has value NULL.
     *
     * @param dynamicParamId unique ID of parameter within this manager; IDs
     * need not be contiguous, and must be assigned by some other authority
     *
     * @param attrDesc descriptor for data values to be stored
     *
     * @param failIfExists if true (the default) an assertion failure
     * will occur if dynamicParamId is already in use
     */
    void createParam(
        DynamicParamId dynamicParamId,
        const TupleAttributeDescriptor &attrDesc,
        bool failIfExists = true);

    /**
     * Creates a new dynamic parameter that will be used as a counter.
     * Initializes the parameter value to 0.  The counter parameter can be
     * used to do atomic increments and decrements.
     *
     * @param dynamicParamId unique ID of parameter within this manager; IDs
     * need not be contiguous, and must be assigned by some other authority
     *
     * @param failIfExists if true (the default) an assertion failure
     * will occur if dynamicParamId is already in use
     */
    void createCounterParam(
        DynamicParamId dynamicParamId,
        bool failIfExists = true);

    /**
     * Deletes an existing dynamic parameter.
     *
     * @param dynamicParamId ID with which parameter was created
     */
    void deleteParam(DynamicParamId dynamicParamId);

    /**
     * Writes a value to a dynamic parameter, overwriting
     * any previous value.
     *
     * @param dynamicParamId ID with which parameter was created
     *
     * @param src source data from which to copy
     */
    void writeParam(DynamicParamId dynamicParamId, const TupleDatum &src);

    /**
     * Accesses a dynamic parameter by reference.
     *
     * @param dynamicParamId ID with which parameter was created
     *
     * @return read-only reference to dynamic parameter
     */
    DynamicParam const &getParam(DynamicParamId dynamicParamId);

    /**
     * Reads a dynamic parameter, copying the parameter into caller's
     * TupleDatum
     *
     * @param dynamicParamId ID with which parameter was created
     *
     * @param dest destination tupledata for parameter
     */
    void readParam(DynamicParamId dynamicParamId, TupleDatum &dest);

    /**
     * Increments a dynamic parameter that corresponds to a counter.
     *
     * @param dynamicParamId ID with which the counter parameter was created
     */
    void incrementCounterParam(DynamicParamId dynamicParamId);

    /**
     * Decrements a dynamic parameter that corresponds to a counter.
     *
     * @param dynamicParamId ID with which the counter parameter was created
     */
    void decrementCounterParam(DynamicParamId dynamicParamId);

    /**
     * Deletes all dynamic parameters
     */
    void deleteAllParams();
};

inline TupleDatum const &DynamicParam::getDatum() const
{
    return datum;
}

inline TupleAttributeDescriptor const &DynamicParam::getDesc() const
{
    return desc;
}

FENNEL_END_NAMESPACE

#endif

// End DynamicParam.h
