/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 1999-2009 John V. Sichi
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

#ifndef Fennel_TupleProjectionAccessor_Included
#define Fennel_TupleProjectionAccessor_Included

#include "fennel/tuple/TupleData.h"

FENNEL_BEGIN_NAMESPACE

class TupleAccessor;
class AttributeAccessor;
class TupleProjection;

/**
 * A TupleProjectionAccessor provides a way to efficiently unmarshal
 * selected attributes of a tuple, as explained in
 * the <a href="structTupleDesign.html#TupleProjection">design docs</a>.
 */
class FENNEL_TUPLE_EXPORT TupleProjectionAccessor
{
    TupleAccessor const *pTupleAccessor;
    std::vector<AttributeAccessor const *> ppAttributeAccessors;

public:
    explicit TupleProjectionAccessor();

    void bind(
        TupleAccessor const &tupleAccessor,
        TupleProjection const &tupleProjection);

    virtual ~TupleProjectionAccessor();

    void unmarshal(TupleData &tuple) const
    {
        unmarshal(tuple.begin());
    }

    void unmarshal(TupleData::iterator tupleIter) const;

    uint size() const
    {
        return ppAttributeAccessors.size();
    }
};

FENNEL_END_NAMESPACE

#endif

// End TupleProjectionAccessor.h
