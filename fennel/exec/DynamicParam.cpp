/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 2004-2004 Disruptive Tech
// Copyright (C) 1999-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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

FENNEL_BEGIN_NAMESPACE



DynamicParam::DynamicParam(uint bufferSize)
{
    pBuffer.reset(new FixedBuffer[bufferSize]);
    datum.cbData = bufferSize;
    datum.pData = pBuffer.get();
}

void DynamicParamManager::createParam(
      const uint dynamicParamId, 
      const TupleAttributeDescriptor &attrDesc)
{
    assert(paramMap.find(dynamicParamId) == paramMap.end());
    SharedDynamicParam param(new DynamicParam(attrDesc.cbStorage));
    paramMap.insert(ParamMap::value_type(dynamicParamId, param));
}

void DynamicParamManager::removeParam(const uint dynamicParamId)
{
    assert(paramMap.find(dynamicParamId) != paramMap.end());
    paramMap.erase(dynamicParamId);
    assert(paramMap.find(dynamicParamId) == paramMap.end());
}

void DynamicParamManager::setParam(const uint dynamicParamId, const TupleDatum &src)
{
    DynamicParam &param = getParam(dynamicParamId);
    assert(param.getDatum().cbData = src.cbData);
    assert(param.getDatum().pData == param.pBuffer.get());

    memcpy(param.pBuffer.get(), src.pData, src.cbData);
}

DynamicParam &DynamicParamManager::getParam(const uint dynamicParamId)
{
    assert(paramMap.find(dynamicParamId) != paramMap.end());
    return *paramMap.find(dynamicParamId)->second.get();
}

FENNEL_END_NAMESPACE

// End DynamicParam.cpp
