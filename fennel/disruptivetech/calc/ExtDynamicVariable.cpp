/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2004-2005 Disruptive Tech
// Copyright (C) 2005-2005 The Eigenbase Project
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later Eigenbase-approved version.
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
#include "fennel/disruptivetech/calc/ExtDynamicVariable.h"
#include "fennel/disruptivetech/calc/ExtendedInstructionTable.h"
#include "fennel/disruptivetech/calc/DynamicParam.h"
#include "fennel/tuple/StandardTypeDescriptor.h"

FENNEL_BEGIN_NAMESPACE

PConstBuffer getData(RegisterRef<int>* id) {
    assert(!id->isNull());
    return id->getDynamicParamManager()->getParam(id->value()).getDatum().pData;
}

void
dynamicVariable(RegisterRef<int8_t>* result,
                RegisterRef<int32_t>* id)
{
    result->value(*reinterpret_cast<int8_t const *>(getData(id)));
}

void
dynamicVariable(RegisterRef<uint8_t>* result,
                RegisterRef<int32_t>* id)
{
    result->value(*reinterpret_cast<uint8_t const *>(getData(id)));
}

void
dynamicVariable(RegisterRef<int16_t>* result,
                RegisterRef<int32_t>* id)
{
    result->value(*reinterpret_cast<int16_t const *>(getData(id)));
}

void
dynamicVariable(RegisterRef<uint16_t>* result,
                RegisterRef<int32_t>* id)
{
    result->value(*reinterpret_cast<uint16_t const *>(getData(id)));
}

void
dynamicVariable(RegisterRef<int32_t>* result,
                RegisterRef<int32_t>* id)
{
    result->value(*reinterpret_cast<int32_t const *>(getData(id)));
}

void
dynamicVariable(RegisterRef<uint32_t>* result,
                RegisterRef<int32_t>* id)
{
    result->value(*reinterpret_cast<uint32_t const *>(getData(id)));
}

void
dynamicVariable(RegisterRef<int64_t>* result,
                RegisterRef<int32_t>* id)
{
    result->value(*reinterpret_cast<int64_t const *>(getData(id)));
}

void
dynamicVariable(RegisterRef<uint64_t>* result,
                RegisterRef<int32_t>* id)
{
    result->value(*reinterpret_cast<uint64_t const *>(getData(id)));
}

void
dynamicVariable(RegisterRef<float>* result,
                RegisterRef<int32_t>* id)
{
    result->value(*reinterpret_cast<float const *>(getData(id)));
}

void
dynamicVariable(RegisterRef<double>* result,
                RegisterRef<int32_t>* id)
{
    result->value(*reinterpret_cast<double const *>(getData(id)));
}

void
dynamicVariable(RegisterRef<bool>* result,
                RegisterRef<int32_t>* id)
{
    result->value(*reinterpret_cast<bool const *>(getData(id)));
}

void
dynamicVariable(RegisterRef<char*>* result,
                RegisterRef<int32_t>* id)
{
    result->value(*reinterpret_cast<char* const *>(getData(id)));
}

void
ExtDynamicVariableRegister(ExtendedInstructionTable* eit)
{
    assert(eit != NULL);

    vector<StandardTypeDescriptorOrdinal> params_s1;
    params_s1.push_back(STANDARD_TYPE_INT_8);
    params_s1.push_back(STANDARD_TYPE_INT_32);
    eit->add("dynamicVariable", params_s1,
             (ExtendedInstruction2<int8_t, int32_t>*) NULL,
             &dynamicVariable);

    vector<StandardTypeDescriptorOrdinal> params_u1;
    params_u1.push_back(STANDARD_TYPE_UINT_8);
    params_u1.push_back(STANDARD_TYPE_INT_32);
    eit->add("dynamicVariable", params_u1,
             (ExtendedInstruction2<uint8_t, int>*) NULL,
             &dynamicVariable);

    vector<StandardTypeDescriptorOrdinal> params_s2;
    params_s2.push_back(STANDARD_TYPE_INT_16);
    params_s2.push_back(STANDARD_TYPE_INT_32);
    eit->add("dynamicVariable", params_s2,
             (ExtendedInstruction2<int16_t, int32_t>*) NULL,
             &dynamicVariable);

    vector<StandardTypeDescriptorOrdinal> params_u2;
    params_u2.push_back(STANDARD_TYPE_UINT_16);
    params_u2.push_back(STANDARD_TYPE_INT_32);
    eit->add("dynamicVariable", params_u2,
             (ExtendedInstruction2<uint16_t, int32_t>*) NULL,
             &dynamicVariable);

    vector<StandardTypeDescriptorOrdinal> params_s4;
    params_s4.push_back(STANDARD_TYPE_INT_32);
    params_s4.push_back(STANDARD_TYPE_INT_32);
    eit->add("dynamicVariable", params_s4,
             (ExtendedInstruction2<int32_t, int32_t>*) NULL,
             &dynamicVariable);

    vector<StandardTypeDescriptorOrdinal> params_u4;
    params_u4.push_back(STANDARD_TYPE_UINT_32);
    params_u4.push_back(STANDARD_TYPE_INT_32);
    eit->add("dynamicVariable", params_u4,
             (ExtendedInstruction2<uint32_t, int32_t>*) NULL,
             &dynamicVariable);

    vector<StandardTypeDescriptorOrdinal> params_s8;
    params_s8.push_back(STANDARD_TYPE_INT_64);
    params_s8.push_back(STANDARD_TYPE_INT_32);
    eit->add("dynamicVariable", params_s8,
             (ExtendedInstruction2<int64_t, int32_t>*) NULL,
             &dynamicVariable);

    vector<StandardTypeDescriptorOrdinal> params_u8;
    params_u8.push_back(STANDARD_TYPE_UINT_64);
    params_u8.push_back(STANDARD_TYPE_INT_32);
    eit->add("dynamicVariable", params_u8,
             (ExtendedInstruction2<uint64_t, int32_t>*) NULL,
             &dynamicVariable);

    vector<StandardTypeDescriptorOrdinal> params_float;
    params_float.push_back(STANDARD_TYPE_REAL);
    params_float.push_back(STANDARD_TYPE_INT_32);
    eit->add("dynamicVariable", params_float,
             (ExtendedInstruction2<float, int32_t>*) NULL,
             &dynamicVariable);

    vector<StandardTypeDescriptorOrdinal> params_double;
    params_double.push_back(STANDARD_TYPE_DOUBLE);
    params_double.push_back(STANDARD_TYPE_INT_32);
    eit->add("dynamicVariable", params_double,
             (ExtendedInstruction2<double, int32_t>*) NULL,
             &dynamicVariable);

    vector<StandardTypeDescriptorOrdinal> params_bool;
    params_bool.push_back(STANDARD_TYPE_BOOL);
    params_bool.push_back(STANDARD_TYPE_INT_32);
    eit->add("dynamicVariable", params_bool,
             (ExtendedInstruction2<bool, int32_t>*) NULL,
             &dynamicVariable);

    vector<StandardTypeDescriptorOrdinal> params_char;
    params_char.push_back(STANDARD_TYPE_CHAR);
    params_char.push_back(STANDARD_TYPE_INT_32);
    eit->add("dynamicVariable", params_char,
             (ExtendedInstruction2<char*, int32_t>*) NULL,
             &dynamicVariable);

    vector<StandardTypeDescriptorOrdinal> params_varchar;
    params_varchar.push_back(STANDARD_TYPE_VARCHAR);
    params_varchar.push_back(STANDARD_TYPE_INT_32);
    eit->add("dynamicVariable", params_varchar,
             (ExtendedInstruction2<char*, int32_t>*) NULL,
             &dynamicVariable);

    vector<StandardTypeDescriptorOrdinal> params_binary;
    params_binary.push_back(STANDARD_TYPE_BINARY);
    params_binary.push_back(STANDARD_TYPE_INT_32);
    eit->add("dynamicVariable", params_binary,
             (ExtendedInstruction2<char*, int32_t>*) NULL,
             &dynamicVariable);

}


FENNEL_END_NAMESPACE

        
