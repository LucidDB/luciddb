/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2004 SQLstream, Inc.
// Copyright (C) 2004 Dynamo BI Corporation
// Portions Copyright (C) 1999 John V. Sichi
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

#include "fennel/common/CommonPreamble.h"
#include "fennel/exec/DynamicParam.h"
#include "fennel/tuple/TupleAccessor.h"
#include "fennel/tuple/TupleData.h"
#include "fennel/tuple/StandardTypeDescriptor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

DynamicParam::DynamicParam(
    TupleAttributeDescriptor const &descInit,
    bool isCounterInit)
    : desc(descInit), isCounter(isCounterInit)
{
    pBuffer.reset(new FixedBuffer[desc.cbStorage]);
}

void DynamicParamManager::createParam(
    DynamicParamId dynamicParamId,
    const TupleAttributeDescriptor &attrDesc,
    bool failIfExists)
{
    StrictMutexGuard mutexGuard(mutex);

    SharedDynamicParam param(new DynamicParam(attrDesc));
    createParam(dynamicParamId, param, failIfExists);
}

void DynamicParamManager::createParam(
    DynamicParamId dynamicParamId,
    SharedDynamicParam param,
    bool failIfExists)
{
    ParamMapConstIter pExisting = paramMap.find(dynamicParamId);
    if (pExisting != paramMap.end()) {
        permAssert(!failIfExists);
        assert(param->desc == pExisting->second->getDesc());
        return;
    }
    paramMap.insert(ParamMap::value_type(dynamicParamId, param));
}

void DynamicParamManager::createCounterParam(
    DynamicParamId dynamicParamId,
    bool failIfExists)
{
    StrictMutexGuard mutexGuard(mutex);

    StandardTypeDescriptorFactory stdTypeFactory;
    TupleAttributeDescriptor attrDesc(
        stdTypeFactory.newDataType(STANDARD_TYPE_INT_64));
    SharedDynamicParam param(new DynamicParam(attrDesc, true));
    createParam(dynamicParamId, param, failIfExists);

    // initialize the parameter to zero
    PBuffer buf = param->pBuffer.get();
    int64_t *pCounter = reinterpret_cast<int64_t *>(buf);
    *pCounter = 0;
    param->datum.pData = buf;
    param->datum.cbData = sizeof(int64_t);
}

void DynamicParamManager::deleteParam(DynamicParamId dynamicParamId)
{
    StrictMutexGuard mutexGuard(mutex);

    assert(paramMap.find(dynamicParamId) != paramMap.end());
    paramMap.erase(dynamicParamId);
    assert(paramMap.find(dynamicParamId) == paramMap.end());
}

void DynamicParamManager::writeParam(
    DynamicParamId dynamicParamId, const TupleDatum &src)
{
    StrictMutexGuard mutexGuard(mutex);

    DynamicParam &param = getParamInternal(dynamicParamId);
    if (src.pData) {
        assert(src.cbData <= param.getDesc().cbStorage);
    }
    param.datum.pData = param.pBuffer.get();
    param.datum.memCopyFrom(src);
}

DynamicParam const &DynamicParamManager::getParam(
    DynamicParamId dynamicParamId)
{
    StrictMutexGuard mutexGuard(mutex);
    return getParamInternal(dynamicParamId);
}

DynamicParam &DynamicParamManager::getParamInternal(
    DynamicParamId dynamicParamId)
{
    ParamMapConstIter pExisting = paramMap.find(dynamicParamId);
    assert(pExisting != paramMap.end());
    return *(pExisting->second.get());
}

void DynamicParamManager::readParam(
    DynamicParamId dynamicParamId, TupleDatum &dest)
{
    StrictMutexGuard mutexGuard(mutex);
    dest.memCopyFrom(getParamInternal(dynamicParamId).datum);
}

void DynamicParamManager::incrementCounterParam(DynamicParamId dynamicParamId)
{
    StrictMutexGuard mutexGuard(mutex);

    DynamicParam &param = getParamInternal(dynamicParamId);
    assert(param.isCounter);
    int64_t *pCounter = reinterpret_cast<int64_t *>(param.pBuffer.get());
    (*pCounter)++;
}

void DynamicParamManager::decrementCounterParam(DynamicParamId dynamicParamId)
{
    StrictMutexGuard mutexGuard(mutex);

    DynamicParam &param = getParamInternal(dynamicParamId);
    assert(param.isCounter);
    int64_t *pCounter = reinterpret_cast<int64_t *>(param.pBuffer.get());
    (*pCounter)--;
}

void DynamicParamManager::deleteAllParams()
{
    StrictMutexGuard mutexGuard(mutex);
    paramMap.erase(paramMap.begin(), paramMap.end());
}

FENNEL_END_CPPFILE("$Id$");

// End DynamicParam.cpp
