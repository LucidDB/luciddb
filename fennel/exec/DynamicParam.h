/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2004-2007 Disruptive Tech
// Copyright (C) 2004-2007 LucidEra, Inc.
// Portions Copyright (C) 1999-2007 John V. Sichi
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

class DynamicParam
{
    friend class DynamicParamManager;

    boost::scoped_array<FixedBuffer> pBuffer;
    TupleAttributeDescriptor desc;
    TupleDatum datum;

public:
    explicit DynamicParam(TupleAttributeDescriptor const &desc);
    inline TupleDatum const &getDatum() const;
    inline TupleAttributeDescriptor const &getDesc() const;
};

/**
 * DynamicParamManager defines methods to allocate, access, and deallocate
 * dynamic parameters.  It is multi-thread safe (but see warning
 * on getParam).
 */
class DynamicParamManager
{
    typedef std::map<DynamicParamId, SharedDynamicParam> ParamMap;
    typedef ParamMap::const_iterator ParamMapConstIter;

    StrictMutex mutex;

    ParamMap paramMap;

    DynamicParam &getParamInternal(DynamicParamId dynamicParamId);

public:
    /**
     * Creates a new dynamic parameter.  Initially, a dynamic parameter
     * has value NULL.
     *
     * @param dynamicParamId unique ID of parameter within this manager; ID's
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
