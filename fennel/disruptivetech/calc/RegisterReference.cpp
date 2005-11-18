/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2004-2005 Disruptive Tech
// Copyright (C) 2005-2005 The Eigenbase Project
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
//
// An index and perhaps a pointer into a RegisterSet
// Optionally optimizes subsequent reads and writes
*/
#include "fennel/common/CommonPreamble.h"
#include "fennel/disruptivetech/calc/RegisterReference.h"
#include "fennel/disruptivetech/calc/Calculator.h"

#include "boost/format.hpp"
using boost::format;

FENNEL_BEGIN_CPPFILE("$Id$");

RegisterSetBinding::~RegisterSetBinding()
{
    if (ownTheBase)
        delete base;
    delete datumAddr;
}

RegisterSetBinding::RegisterSetBinding(TupleData* base, bool ownIt)
    :ownTheBase(ownIt), base(base)
{
    assert(base);
    ncols = base->size();
    datumAddr = new PConstBuffer[ncols];
    for (int i = 0; i < ncols; i++) {
        const TupleDatum& baseCol = (*base)[i];
        datumAddr[i] = baseCol.pData;
    }
}

RegisterSetBinding::RegisterSetBinding(TupleData* base, const TupleData* shadow, bool ownIt)
    :ownTheBase(ownIt), base(base)
{
    assert(base);
    ncols = base->size();
    assert(shadow->size() == ncols);
    datumAddr = new PConstBuffer[ncols];
    for (int i = 0; i < ncols; i++) {
        const TupleDatum& baseCol = (*base)[i];
        const TupleDatum& shadowCol = (*shadow)[i];
        datumAddr[i] = shadowCol.pData;
        // check that SHADOW coincides with BASE
        if (baseCol.pData)
            assert(baseCol.pData == shadowCol.pData);
        else
            assert(shadowCol.pData);
    }

}

void 
RegisterReference::setCalc(Calculator* calcP) {
    mRegisterSetP = calcP->mRegisterSetBinding;
    mRegisterSetDescP = calcP->mRegisterSetDescriptor;
    mResetP = &(calcP->mRegisterReset);
    mPDynamicParamManager = calcP->getDynamicParamManager();
    if (mSetIndex == EOutput && calcP->mOutputRegisterByReference) {
        //! See Calculator#outputRegisterByReference()
        mProp |= (EPropByRefOnly | EPropReadOnly);
    }
}

void 
RegisterReference::cachePointer() {
    if (mProp & (EPropCachePointer|EPropPtrReset)) {
        TupleDatum *bind = getBinding();
        mPData = const_cast<PBuffer>(bind->pData);
        mCbData = bind->cbData;

        // Next 3 lines clarify complex 4th line:
        // TupleDescriptor* tupleDescP = mRegisterSetDescP[mSetIndex];
        // TupleAttributeDescriptor* attrP = &((*tupleDescP)[mIndex]);
        // mCbStorage = attrP->cbStorage;
        mCbStorage = ((*(mRegisterSetDescP[mSetIndex]))[mIndex]).cbStorage;
        mCachePtrModified = false;
    }
}

void
RegisterReference::setDefaultProperties()
{
    switch(mSetIndex) {
    case ELiteral:
        mProp = KLiteralSetDefault;
        break;
    case EInput:
        mProp = KInputSetDefault;
        break;
    case EOutput:
        mProp = KOutputSetDefault;
        break;
    case ELocal:
        mProp = KLocalSetDefault;
        break;
    case EStatus:
        mProp = KStatusSetDefault;
        break;
    default:
        mProp = EPropNone;
    }
}



string
RegisterReference::toString() const
{
    return boost::io::str( format("[S%d I%lu]") % mSetIndex % mIndex);
}



FENNEL_END_CPPFILE("$Id: //open/dt/dev/fennel/disruptivetech/calc/RegisterReference.cpp#6 $");

// End RegisterReference.cpp
