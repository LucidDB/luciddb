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
