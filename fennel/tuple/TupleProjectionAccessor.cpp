/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 1999-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later Eigenbase-approved version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307  USA
*/

#include "fennel/common/CommonPreamble.h"
#include "fennel/tuple/TupleAccessor.h"
#include "fennel/tuple/TupleProjectionAccessor.h"
#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/tuple/TupleData.h"
#include "fennel/tuple/AttributeAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");
TupleProjectionAccessor::TupleProjectionAccessor()
{
    pTupleAccessor = NULL;
}

TupleProjectionAccessor::~TupleProjectionAccessor()
{
}
    
void TupleProjectionAccessor::bind(
    TupleAccessor const &tupleAccessor,
    TupleProjection const &tupleProjection)
{
    pTupleAccessor = &tupleAccessor;
    ppAttributeAccessors.clear();
    for (uint i = 0; i < tupleProjection.size(); ++i) {
        ppAttributeAccessors.push_back(
            &(tupleAccessor.getAttributeAccessor(tupleProjection[i])));
    }
}

void TupleProjectionAccessor::unmarshal(TupleData::iterator tupleIter) const
{
    for (uint i = 0; i < ppAttributeAccessors.size(); ++i, ++tupleIter) {
        ppAttributeAccessors[i]->unmarshalValue(
            *pTupleAccessor,
            *tupleIter);
    }
}

FENNEL_END_CPPFILE("$Id$");

// End TupleProjectionAccessor.cpp
