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
#include "fennel/tuple/TupleData.h"
#include "fennel/tuple/TupleDescriptor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

TupleDatum::TupleDatum()
{
    cbData = 0;
    pData = NULL;
}

TupleData::TupleData()
{
}

TupleData::TupleData(TupleDescriptor const &tupleDesc)
{
    compute(tupleDesc);
}

void TupleData::compute(TupleDescriptor const &tupleDesc)
{
    clear();
    for (uint i = 0; i < tupleDesc.size(); ++i) {
        TupleDatum datum;
        datum.cbData = tupleDesc[i].cbStorage;
        push_back(datum);
    }
}

bool TupleData::containsNull() const
{
    for (uint i = 0; i < size(); ++i) {
        if (!(*this)[i].pData) {
            return true;
        }
    }
    return false;
}

void TupleData::projectFrom(
    TupleData const& src,
    TupleProjection const& projection)
{
    clear();
    for (uint i = 0; i < projection.size(); ++i) {
        push_back(src[projection[i]]);
    }
}

FENNEL_END_CPPFILE("$Id$");

// End TupleData.cpp
