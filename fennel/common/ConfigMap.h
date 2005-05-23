/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 1999-2005 John V. Sichi
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
