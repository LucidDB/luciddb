/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 2004-2004 Disruptive Tech
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
//
// An index and perhaps a pointer into a RegisterSet
// Optionally optimizes subsequent reads and writes
*/
#include "fennel/common/CommonPreamble.h"
#include "fennel/calc/RegisterReference.h"
#include "fennel/calc/Calculator.h"

#include "boost/format.hpp"
using boost::format;

FENNEL_BEGIN_CPPFILE("$Id$");

void 
RegisterReference::setCalc(Calculator* calcP) {
    mRegisterSetP = calcP->mRegisterTuple;
    mRegisterSetDescP = calcP->mRegisterSetDescriptor;
    mResetP = &(calcP->mRegisterReset);
}

void 
RegisterReference::cachePointer() {
    if (mProp & (EPropCachePointer|EPropPtrReset)) {
        assert(mRegisterSetP);
        assert(mSetIndex < ELastSet);
        assert(mRegisterSetP[mSetIndex]);
        assert(mRegisterSetDescP[mSetIndex]);

        TupleData* tupleDataP = mRegisterSetP[mSetIndex];
        TupleDatum* datumP = &((*tupleDataP)[mIndex]);
        mPData = const_cast<PBuffer>(datumP->pData);
        mCbData = datumP->cbData;

        // Next 3 lines clarify complex 4th line:
        // TupleDescriptor* tupleDescP = mRegisterSetDescP[mSetIndex];
        // TupleAttributeDescriptor* attrP = &((*tupleDescP)[mIndex]);
        // mCbStorage = attrP->cbStorage;
        mCbStorage = ((*(mRegisterSetDescP[mSetIndex]))[mIndex]).cbStorage;
        mCachePtrModified = false;
    }
}

string
RegisterReference::toString() const
{
    return boost::io::str( format("[S%d I%lu]") % mSetIndex % mIndex);
}

FENNEL_END_CPPFILE("$Id$");

// End RegisterReference.cpp
