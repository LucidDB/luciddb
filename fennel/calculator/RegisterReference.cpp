/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/
#include "fennel/common/CommonPreamble.h"
#include "fennel/calculator/RegisterReference.h"
#include "fennel/calculator/Calculator.h"

#include "boost/format.hpp"
using boost::format;

FENNEL_BEGIN_CPPFILE("$Id$");

RegisterSetBinding::~RegisterSetBinding()
{
    if (ownTheBase) {
        delete base;
    }
    delete[] datumAddr;
}

RegisterSetBinding::RegisterSetBinding(TupleData* base, bool ownIt)
    : ownTheBase(ownIt), base(base)
{
    assert(base);
    ncols = base->size();
    datumAddr = new PConstBuffer[ncols];
    for (int i = 0; i < ncols; i++) {
        const TupleDatum& baseCol = (*base)[i];
        datumAddr[i] = baseCol.pData;
    }
}

// Rebind to new Tuple as base. Make sure old base and newBase have the same
// size.
void RegisterSetBinding::rebind(
    TupleData* newBase, bool ownIt)
{
    assert(newBase);
    assert(ncols == newBase->size());
    if (ownTheBase) {
        // previous base owned by me.
        delete base;
    }
    base = newBase;
    ownTheBase = ownIt;
    ncols = base->size();
    for (int i = 0; i < ncols; i++) {
        const TupleDatum& baseCol = (*base)[i];
        datumAddr[i] = baseCol.pData;
    }
}

RegisterSetBinding::RegisterSetBinding(
    TupleData* base,
    const TupleData* shadow,
    bool ownIt)
    : ownTheBase(ownIt),
      base(base)
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
        if (baseCol.pData) {
            assert(baseCol.pData == shadowCol.pData);
        } else {
            assert(shadowCol.pData);
        }
    }
}

// Rebind to new Tuple as base. Make sure old base and newBase have the same
// size.
void RegisterSetBinding::rebind(
    TupleData* newBase, const TupleData* shadow, bool ownIt)
{
    assert(newBase);
    assert(ncols == newBase->size());
    if (ownTheBase) {
        // previous base owned by me.
        delete base;
    }
    base = newBase;
    ownTheBase = ownIt;
    ncols = base->size();
    assert(shadow->size() == ncols);
    for (int i = 0; i < ncols; i++) {
        const TupleDatum& baseCol = (*base)[i];
        const TupleDatum& shadowCol = (*shadow)[i];
        datumAddr[i] = shadowCol.pData;
        // check that SHADOW coincides with BASE
        if (baseCol.pData) {
            assert(baseCol.pData == shadowCol.pData);
        } else {
            assert(shadowCol.pData);
        }
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
    if (mProp & (EPropCachePointer | EPropPtrReset)) {
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
    switch (mSetIndex) {
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
    return boost::io::str(format("[S%d I%lu]") % mSetIndex % mIndex);
}

FENNEL_END_CPPFILE("$Id$");

// End RegisterReference.cpp
