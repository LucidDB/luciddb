/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 2004-2004 Disruptive Tech
// Copyright (C) 1999-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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

#ifndef Fennel_DynamicParam_Included
#define Fennel_DynamicParam_Included

#include "fennel/tuple/TupleData.h"
#include "fennel/tuple/TupleDescriptor.h"
#include <map>
#include <boost/scoped_array.hpp>


/**
 * DynamicParam defines parameters and methods to create and manage dynamic 
 * parameters.
 * Dynamic parameters are parameters (physically tuples) shared amongst streams.
 *
 * @author Wael Chatila
 * @version $Id$
 */

FENNEL_BEGIN_NAMESPACE

class DynamicParam
{
public:
    boost::scoped_array<FixedBuffer> pBuffer;
    TupleDatum datum;

    explicit DynamicParam(uint bufferSize);
    inline TupleDatum &getDatum();
};

typedef boost::shared_ptr<DynamicParam> SharedDynamicParam;

class DynamicParamManager
{
private:
    typedef std::map<std::string, SharedDynamicParam> ParamMap;
    typedef ParamMap::const_iterator ParamMapConstIter;

    ParamMap paramMap;
    
public:
    void createParam(const std::string &dynamicParamId, 
                     const TupleAttributeDescriptor &attrDesc);

    void removeParam(const std::string &dynamicParamId);

    void setParam(const std::string &dynamicParamId, const TupleDatum &src);

    DynamicParam &getParam(const std::string &dynamicParamId);

};

inline TupleDatum &DynamicParam::getDatum() 
{
    return datum;
}

FENNEL_END_NAMESPACE

#endif

// End DynamicParam.h
