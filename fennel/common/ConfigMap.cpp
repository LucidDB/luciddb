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
#include "fennel/common/ConfigMap.h"
#include <iostream>

#include <boost/lexical_cast.hpp>

FENNEL_BEGIN_CPPFILE("$Id$");

ConfigMap::ConfigMap()
{
}

ConfigMap::~ConfigMap()
{
}

void ConfigMap::readParams(std::istream &paramStream)
{
    for (;;) {
        std::string name, value;
        paramStream >> name;
        if (name == "") {
            break;
        }
        paramStream >> value;
        paramVals[name] = value;
    }
}

void ConfigMap::dumpParams(std::ostream &dumpStream) const
{
    for (StringMapConstIter pPair = paramVals.begin();
         pPair != paramVals.end(); ++pPair)
    {
        dumpStream << pPair->first;
        dumpStream << " ";
        dumpStream << pPair->second;
        dumpStream << std::endl;
    }
}

void ConfigMap::mergeFrom(const ConfigMap& that)
{
    for (StringMapConstIter pPair = that.paramVals.begin();
         pPair != that.paramVals.end();
         ++pPair)
    {
        this->paramVals[pPair->first] = pPair->second;
    }
}


std::string ConfigMap::getStringParam(
    std::string paramName,
    std::string defaultVal) const
{
    StringMapConstIter pPair = paramVals.find(paramName);
    if (pPair == paramVals.end()) {
        FENNEL_TRACE(
            TRACE_CONFIG,
            "parameter " << paramName
            << " using default value of '"
            << defaultVal << "'");
        return defaultVal;
    } else {
        FENNEL_TRACE(
            TRACE_CONFIG,
            "parameter " << paramName
            << " using specified value of '"
            << pPair->second << "'");
        return pPair->second;
    }
}

int ConfigMap::getIntParam(
    std::string paramName,
    int defaultVal) const
{
    StringMapConstIter pPair = paramVals.find(paramName);
    if (pPair == paramVals.end()) {
        FENNEL_TRACE(
            TRACE_CONFIG,
            "parameter " << paramName
            << " using default value of '"
            << defaultVal << "'");
        return defaultVal;
    } else {
        FENNEL_TRACE(
            TRACE_CONFIG,
            "parameter " << paramName
            << " using specified value of '"
            << pPair->second << "'");
        return boost::lexical_cast<int>(pPair->second);
    }
}

bool ConfigMap::getBoolParam(
    std::string paramName,
    bool defaultVal) const
{
    StringMapConstIter pPair = paramVals.find(paramName);
    if (pPair == paramVals.end()) {
        FENNEL_TRACE(
            TRACE_CONFIG,
            "parameter " << paramName
            << " using default value of '"
            << defaultVal << "'");
        return defaultVal;
    } else {
        FENNEL_TRACE(
            TRACE_CONFIG,
            "parameter " << paramName
            << " using specified value of '"
            << pPair->second << "'");
        // boost only likes 1/0, so preprocess true/false
        if (strcasecmp(pPair->second.c_str(), "true") == 0) {
            return true;
        } else if (strcasecmp(pPair->second.c_str(), "false") == 0) {
            return false;
        } else {
            return boost::lexical_cast<bool>(pPair->second);
        }
    }
}

long ConfigMap::getLongParam(
    std::string paramName,
    long defaultVal) const
{
    StringMapConstIter pPair = paramVals.find(paramName);
    if (pPair == paramVals.end()) {
        FENNEL_TRACE(
            TRACE_CONFIG,
            "parameter " << paramName
            << " using default value of '"
            << defaultVal << "'");
        return defaultVal;
    } else {
        FENNEL_TRACE(
            TRACE_CONFIG,
            "parameter " << paramName
            << " using specified value of '"
            << pPair->second << "'");
        // REVIEW jvs 25-Nov-2008:  There used to be a note here,
        // but it didn't actually explain why this doesn't use
        // boost::lexical_cast; probably an old Boost bug.
        return atol(pPair->second.c_str());
    }
}

double ConfigMap::getDoubleParam(
    std::string paramName,
    double defaultVal) const
{
    StringMapConstIter pPair = paramVals.find(paramName);
    if (pPair == paramVals.end()) {
        return defaultVal;
    } else {
        return strtod(pPair->second.c_str(), NULL);
    }
}


bool ConfigMap::isParamSet(std::string paramName) const
{
    return paramVals.find(paramName) != paramVals.end();
}

void ConfigMap::setStringParam(
    std::string paramName,
    std::string paramVal)
{
    paramVals[paramName] = paramVal;
}

void ConfigMap::clear()
{
    paramVals.clear();
}

FENNEL_END_CPPFILE("$Id$");

// End ConfigMap.cpp
