/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2004-2005 Disruptive Tech
// Copyright (C) 2004-2005 LucidEra, Inc.
// Portions Copyright (C) 1999-2005 John V. Sichi
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

FENNEL_BEGIN_CPPFILE("$Id$");

DynamicParam::DynamicParam(TupleAttributeDescriptor const &descInit)
    : desc(descInit)
{
    pBuffer.reset(new FixedBuffer[desc.cbStorage]);
}

void DynamicParamManager::createParam(
    DynamicParamId dynamicParamId, 
    const TupleAttributeDescriptor &attrDesc,
    bool failIfExists)
{
    StrictMutexGuard mutexGuard(mutex);

    ParamMapConstIter pExisting = paramMap.find(dynamicParamId);
    if (pExisting != paramMap.end()) {
        permAssert(!failIfExists);
        assert(attrDesc == pExisting->second->getDesc());
        return;
    }
    SharedDynamicParam param(new DynamicParam(attrDesc));
    paramMap.insert(ParamMap::value_type(dynamicParamId, param));
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

FENNEL_END_CPPFILE("$Id$");

// End DynamicParam.cpp
