/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2006 The Eigenbase Project
// Copyright (C) 2005-2006 Disruptive Tech
// Copyright (C) 2005-2006 LucidEra, Inc.
// Portions Copyright (C) 1999-2006 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by the Free
// Software Foundation; either version 2 of the License, or (at your option)
// any later version approved by The Eigenbase Project.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
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
        std::string name,value;
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
        return defaultVal;
    } else {
        return pPair->second;
    }
}

int ConfigMap::getIntParam(
    std::string paramName,
    int defaultVal) const
{
    StringMapConstIter pPair = paramVals.find(paramName);
    if (pPair == paramVals.end()) {
        return defaultVal;
    } else {
        return boost::lexical_cast<int>(pPair->second);
    }
}

bool ConfigMap::getBoolParam(
    std::string paramName,
    bool defaultVal) const
{
    StringMapConstIter pPair = paramVals.find(paramName);
    if (pPair == paramVals.end()) {
        return defaultVal;
    } else {
        /* Support true/false? boost only likes 1/0 */
        if (strcasecmp(pPair->second.c_str(), "true") == 0) {
            return true;
        }
        else if (strcasecmp(pPair->second.c_str(), "false") == 0) {
            return false;
        }
        else return boost::lexical_cast<bool>(pPair->second);
    }
}

// REVIEW:  maybe use a template instead?
long ConfigMap::getLongParam(
    std::string paramName,
    long defaultVal) const
{
    StringMapConstIter pPair = paramVals.find(paramName);
    if (pPair == paramVals.end()) {
        return defaultVal;
    } else {
        // NOTE:  see above
        return atol(pPair->second.c_str());
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
