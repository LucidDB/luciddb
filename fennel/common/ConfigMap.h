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

#ifndef Fennel_ConfigMap_Included
#define Fennel_ConfigMap_Included

#include <map>

FENNEL_BEGIN_NAMESPACE

/**
 * ConfigMap defines a simple container for configuration parameter/value
 * pairs.
 */
class ConfigMap 
{
    typedef std::map<std::string,std::string> StringMap;
    typedef StringMap::iterator StringMapIter;
    typedef StringMap::const_iterator StringMapConstIter;
    
    StringMap paramVals;
    
public:
    /**
     * Create an empty map.
     */
    explicit ConfigMap();

    /**
     * Destroy the map.
     */
    virtual ~ConfigMap();

    /**
     * Read parameter name/value pairs from an input stream, until
     * end-of-stream is encountered.  Each pair should be on a line by itself,
     * with whitespace between the name and value.
     *
     * @param paramStream the stream from which to read parameters
     */
    void readParams(std::istream &paramStream);

    /**
     * Dump all parameter settings to a stream.
     *
     * @param dumpStream target stream
     */
    void dumpParams(std::ostream &dumpStream) const;
    
    /**
     * Get the value of a string-typed parameter.
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
     * Get the value of an integer-typed parameter.
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
     * Get the value of a long-typed parameter.
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
     * Determine whether a parameter has an associated value
     *
     * @param paramName name of the parameter
     *
     * @return true iff the named parameter has been set
     */
    bool isParamSet(std::string paramName) const;

    /**
     * Set an individual parameter value.
     *
     * @param paramName name of the parameter
     *
     * @param paramVal new value to set
     */
    void setStringParam(
        std::string paramName,
        std::string paramVal);

    /**
     * Clear all parameters.
     */
    void clear();
};

FENNEL_END_NAMESPACE

#endif

// End ConfigMap.h
