/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 1999-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

#include "fennel/common/CommonPreamble.h"
#include "fennel/common/ConfigMap.h"
#include <iostream>

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
        // NOTE:  this used to call boost::lexical_cast, but that broke
        // on Cygwin and Mingw, so for now use good old C calls
        return atoi(pPair->second.c_str());
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
