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

FENNEL_END_CPPFILE("$Id$");

// End TupleData.cpp
