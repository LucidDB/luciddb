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
// Test Calculator object directly by instantiating instruction objects,
// creating programs, running them, and checking the register set values.
*/

#include "fennel/common/CommonPreamble.h"
#include "fennel/calc/SqlString.h"
#include "fennel/calc/ExtendedInstructionTable.h"

FENNEL_BEGIN_NAMESPACE

void
castExactToStrA(RegisterRef<char*>* result,
                RegisterRef<int64_t>* src)
{
    assert(StandardTypeDescriptor::isTextArray(result->type()));

    if (src->isNull()) {
        result->toNull();
        result->length(0);
    } else {
 
        result->length(SqlStrCastFromExact<1,1>
                       (result->pointer(),
                        result->storage(),
                        src->value(),
                        (result->type() == STANDARD_TYPE_CHAR ?
                         true : false)));
    }
}

void
castApproxToStrA(RegisterRef<char*>* result,
                 RegisterRef<double>* src)
{
    assert(StandardTypeDescriptor::isTextArray(result->type()));

    if (src->isNull()) {
        result->toNull();
        result->length(0);
    } else {
        result->length(SqlStrCastFromApprox<1,1>
                       (result->pointer(),
                        result->storage(),
                        src->value(),
                        (result->type() == STANDARD_TYPE_CHAR ?
                         true : false)));
    }
}

void
castStrToExactA(RegisterRef<int64_t>* result,
                RegisterRef<char*>* src)
{
    assert(StandardTypeDescriptor::isTextArray(src->type()));

    if (src->isNull()) {
        result->toNull();
    } else {
        result->value(SqlStrCastToExact<1,1>
                      (src->pointer(),
                       src->stringLength()));
    }
}

void
castStrToApproxA(RegisterRef<double>* result,
                 RegisterRef<char*>* src)
{
    assert(StandardTypeDescriptor::isTextArray(src->type()));

    if (src->isNull()) {
        result->toNull();
    } else {
        result->value(SqlStrCastToApprox<1,1>
                      (src->pointer(),
                       src->stringLength()));
    }
}


void
ExtCastRegister(ExtendedInstructionTable* eit)
{
    assert(eit != NULL);
    
    vector<StandardTypeDescriptorOrdinal> params_1I_1C;
    params_1I_1C.push_back(STANDARD_TYPE_INT_64);
    params_1I_1C.push_back(STANDARD_TYPE_CHAR);

    vector<StandardTypeDescriptorOrdinal> params_1I_1V;
    params_1I_1V.push_back(STANDARD_TYPE_INT_64);
    params_1I_1V.push_back(STANDARD_TYPE_VARCHAR);

    vector<StandardTypeDescriptorOrdinal> params_1D_1C;
    params_1I_1C.push_back(STANDARD_TYPE_DOUBLE);
    params_1I_1C.push_back(STANDARD_TYPE_CHAR);

    vector<StandardTypeDescriptorOrdinal> params_1D_1V;
    params_1I_1V.push_back(STANDARD_TYPE_DOUBLE);
    params_1I_1V.push_back(STANDARD_TYPE_VARCHAR);

    vector<StandardTypeDescriptorOrdinal> params_1C_1I;
    params_1C_1I.push_back(STANDARD_TYPE_CHAR);
    params_1C_1I.push_back(STANDARD_TYPE_INT_64);

    vector<StandardTypeDescriptorOrdinal> params_1V_1I;
    params_1V_1I.push_back(STANDARD_TYPE_VARCHAR);
    params_1V_1I.push_back(STANDARD_TYPE_INT_64);

    vector<StandardTypeDescriptorOrdinal> params_1C_1D;
    params_1C_1I.push_back(STANDARD_TYPE_CHAR);
    params_1C_1I.push_back(STANDARD_TYPE_DOUBLE);

    vector<StandardTypeDescriptorOrdinal> params_1V_1D;
    params_1V_1I.push_back(STANDARD_TYPE_VARCHAR);
    params_1V_1I.push_back(STANDARD_TYPE_DOUBLE);

    eit->add("castA", params_1I_1C,
             (ExtendedInstruction2<int64_t, char*>*) NULL,
             &castStrToExactA);
    eit->add("castA", params_1I_1V,
             (ExtendedInstruction2<int64_t, char*>*) NULL,
             &castStrToExactA);

    eit->add("castA", params_1D_1C,
             (ExtendedInstruction2<double, char*>*) NULL,
             &castStrToApproxA);
    eit->add("castA", params_1D_1V,
             (ExtendedInstruction2<double, char*>*) NULL,
             &castStrToApproxA);

    eit->add("castA", params_1C_1I,
             (ExtendedInstruction2<char*, int64_t>*) NULL,
             &castExactToStrA);
    eit->add("castA", params_1V_1I,
             (ExtendedInstruction2<char*, int64_t>*) NULL,
             &castExactToStrA);

    eit->add("castA", params_1C_1D,
             (ExtendedInstruction2<char*, double>*) NULL,
             &castApproxToStrA);
    eit->add("castA", params_1V_1D,
             (ExtendedInstruction2<char*, double>*) NULL,
             &castApproxToStrA);
}


FENNEL_END_NAMESPACE

        
