/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2004-2007 John V. Sichi
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

#ifndef Fennel_TupleOverflowExcn_Included
#define Fennel_TupleOverflowExcn_Included

#include "fennel/common/FennelExcn.h"

FENNEL_BEGIN_NAMESPACE

class TupleDescriptor;
class TupleData;
    
/**
 * Exception class to be thrown when an oversized tuple is encountered.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class TupleOverflowExcn : public FennelExcn
{
public:
    /**
     * Constructs a new TupleOverflowExcn.
     *
     *<p>
     *
     * @param tupleDesc descriptor for the tuple
     *
     * @param tupleData data for the tuple
     *
     * @param cbActual actual number of bytes required to store tuple
     *
     * @param cbMax maximum number of bytes available to store tuple
     */
    explicit TupleOverflowExcn(
        TupleDescriptor const &tupleDesc,
        TupleData const &tupleData,
        uint cbActual,
        uint cbMax);
};

FENNEL_END_NAMESPACE

#endif

// End TupleOverflowExcn.h
