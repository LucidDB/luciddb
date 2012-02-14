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

#ifndef Fennel_ConfigMap_Included
#define Fennel_ConfigMap_Included

#include <map>

#include "fennel/common/TraceSource.h"

FENNEL_BEGIN_NAMESPACE

/**
 * ConfigMap defines a simple container for configuration parameter/value
 * pairs.
 */
class FENNEL_COMMON_EXPORT ConfigMap : public TraceSource
{
    typedef std::map<std::string, std::string> StringMap;
    typedef StringMap::iterator StringMapIter;
    typedef StringMap::const_iterator StringMapConstIter;

    StringMap paramVals;

public:
    /**
     * Creates an empty map.
     */
    explicit ConfigMap();

    /**
     * Destroys the map.
     */
    virtual ~ConfigMap();

    /**
     * Reads parameter name/value pairs from an input stream, until
     * end-of-stream is encountered.  Each pair should be on a line by itself,
     * with whitespace between the name and value.
     *
     * @param paramStream the stream from which to read parameters
     */
    void readParams(std::istream &paramStream);

    /**
     * Dumps all parameter settings to a stream.
     *
     * @param dumpStream target stream
     */
    void dumpParams(std::ostream &dumpStream) const;

    /**
     * Merges in all the parameters from another map.
     * New values override current values.
     */
    void mergeFrom(const ConfigMap&);


    /**
     * Gets the value of a string-typed parameter.
     *
     * @param paramName name of the parameter
     *
     * @param defaultVal the default value to return if the parameter is not set
     *
     * @return the parameter value
     */
    std::string getStringParam(
        std::string paramName,
        std::string defaultVal = "") const;

    /**
     * Gets the value of an integer-typed parameter.
     *
     * @param paramName name of the parameter
     *
     * @param defaultVal the default value to return if the parameter is not set
     *
     * @return the parameter value
     */
    int getIntParam(
        std::string paramName,
        int defaultVal = 0) const;

    /**
     * Gets the value of a long-typed parameter.
     *
     * @param paramName name of the parameter
     *
     * @param defaultVal the default value to return if the parameter is not set
     *
     * @return the parameter value
     */
    long getLongParam(
        std::string paramName,
        long defaultVal = 0) const;


    /**
     * Gets the value of a double-typed parameter.
     *
     * @param paramName name of the parameter
     *
     * @param defaultVal the default value to return if the parameter is not set
     *
     * @return the parameter value
     */
    double getDoubleParam(
        std::string paramName,
        double defaultVal = 0) const;

    /**
     * Gets the value of an boolean-typed parameter.
     *
     * @param paramName name of the parameter
     *
     * @param defaultVal the default value to return if the parameter is not set
     *
     * @return the parameter value
     */
    bool getBoolParam(
        std::string paramName,
        bool defaultVal = false) const;

    /**
     * Determines whether a parameter has an associated value.
     *
     * @param paramName name of the parameter
     *
     * @return true iff the named parameter has been set
     */
    bool isParamSet(std::string paramName) const;

    /**
     * Sets an individual parameter value.
     *
     * @param paramName name of the parameter
     *
     * @param paramVal new value to set
     */
    void setStringParam(
        std::string paramName,
        std::string paramVal);

    /**
     * Clears all parameters.
     */
    void clear();
};

FENNEL_END_NAMESPACE

#endif

// End ConfigMap.h
