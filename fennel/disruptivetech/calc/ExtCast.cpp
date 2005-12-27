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
*/

#include "fennel/common/CommonPreamble.h"
#include "fennel/disruptivetech/calc/SqlString.h"
#include "fennel/disruptivetech/calc/ExtendedInstructionTable.h"

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
castExactToStrA(RegisterRef<char*>* result,
                RegisterRef<int64_t>* src,
                RegisterRef<int32_t>* precision,
                RegisterRef<int32_t>* scale)
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
                        precision->value(),
                        scale->value(),
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
castStrToExactA(RegisterRef<int64_t>* result,
                RegisterRef<char*>* src,
                RegisterRef<int32_t>* precision,
                RegisterRef<int32_t>* scale)
{
    assert(StandardTypeDescriptor::isTextArray(src->type()));

    if (src->isNull()) {
        result->toNull();
    } else {
        result->value(SqlStrCastToExact<1,1>
                      (src->pointer(),
                       src->stringLength(),
                       precision->value(),
                       scale->value()));
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


// TODO: cases where the result is smaller than the input could
// probably benefit from operating by reference instead of by value
void
castStrToVarCharA(RegisterRef<char*>* result,
                  RegisterRef<char*>* src)
{
    assert(StandardTypeDescriptor::isTextArray(result->type()));
    assert(StandardTypeDescriptor::isTextArray(src->type()));
    
    if (src->isNull()) {
        result->toNull();
        result->length(0);
    } else {
        int rightTruncWarning = 0;
        result->length(SqlStrCastToVarChar<1,1>(result->pointer(),
                                                result->storage(),
                                                src->pointer(),
                                                src->stringLength(),
                                                &rightTruncWarning));
        if (rightTruncWarning) {
            // TODO: throw 22001 as a warning
//            throw "22001";
        }
    }
}

// TODO: cases where the result is smaller than the input could
// probably benefit from operating by reference instead of by value
void
castStrToCharA(RegisterRef<char*>* result,
               RegisterRef<char*>* src)
{
    assert(StandardTypeDescriptor::isTextArray(result->type()));
    assert(StandardTypeDescriptor::isTextArray(src->type()));
    
    if (src->isNull()) {
        result->toNull();
        result->length(0);
    } else {
        int rightTruncWarning = 0;
        result->length(SqlStrCastToChar<1,1>(result->pointer(),
                                             result->storage(),
                                             src->pointer(),
                                             src->stringLength(),
                                             &rightTruncWarning));

        if (rightTruncWarning) {
            // TODO: throw 22001 as a warning
//            throw "22001";
        }
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

    vector<StandardTypeDescriptorOrdinal> params_1I_1C_SP;
    params_1I_1C_SP.push_back(STANDARD_TYPE_INT_64);
    params_1I_1C_SP.push_back(STANDARD_TYPE_CHAR);
    params_1I_1C_SP.push_back(STANDARD_TYPE_INT_32);
    params_1I_1C_SP.push_back(STANDARD_TYPE_INT_32);

    vector<StandardTypeDescriptorOrdinal> params_1I_1V_SP;
    params_1I_1V_SP.push_back(STANDARD_TYPE_INT_64);
    params_1I_1V_SP.push_back(STANDARD_TYPE_VARCHAR);
    params_1I_1V_SP.push_back(STANDARD_TYPE_INT_32);
    params_1I_1V_SP.push_back(STANDARD_TYPE_INT_32);

    vector<StandardTypeDescriptorOrdinal> params_1D_1C;
    params_1D_1C.push_back(STANDARD_TYPE_DOUBLE);
    params_1D_1C.push_back(STANDARD_TYPE_CHAR);

    vector<StandardTypeDescriptorOrdinal> params_1D_1V;
    params_1D_1V.push_back(STANDARD_TYPE_DOUBLE);
    params_1D_1V.push_back(STANDARD_TYPE_VARCHAR);

    vector<StandardTypeDescriptorOrdinal> params_1C_1I;
    params_1C_1I.push_back(STANDARD_TYPE_CHAR);
    params_1C_1I.push_back(STANDARD_TYPE_INT_64);

    vector<StandardTypeDescriptorOrdinal> params_1V_1I;
    params_1V_1I.push_back(STANDARD_TYPE_VARCHAR);
    params_1V_1I.push_back(STANDARD_TYPE_INT_64);

    vector<StandardTypeDescriptorOrdinal> params_1C_1I_SP;
    params_1C_1I_SP.push_back(STANDARD_TYPE_CHAR);
    params_1C_1I_SP.push_back(STANDARD_TYPE_INT_64);
    params_1C_1I_SP.push_back(STANDARD_TYPE_INT_32);
    params_1C_1I_SP.push_back(STANDARD_TYPE_INT_32);

    vector<StandardTypeDescriptorOrdinal> params_1V_1I_SP;
    params_1V_1I_SP.push_back(STANDARD_TYPE_VARCHAR);
    params_1V_1I_SP.push_back(STANDARD_TYPE_INT_64);
    params_1V_1I_SP.push_back(STANDARD_TYPE_INT_32);
    params_1V_1I_SP.push_back(STANDARD_TYPE_INT_32);

    vector<StandardTypeDescriptorOrdinal> params_1C_1D;
    params_1C_1D.push_back(STANDARD_TYPE_CHAR);
    params_1C_1D.push_back(STANDARD_TYPE_DOUBLE);

    vector<StandardTypeDescriptorOrdinal> params_1V_1D;
    params_1V_1D.push_back(STANDARD_TYPE_VARCHAR);
    params_1V_1D.push_back(STANDARD_TYPE_DOUBLE);

    vector<StandardTypeDescriptorOrdinal> params_1V_1C;
    params_1V_1C.push_back(STANDARD_TYPE_VARCHAR);
    params_1V_1C.push_back(STANDARD_TYPE_CHAR);

    vector<StandardTypeDescriptorOrdinal> params_1C_1V;
    params_1C_1V.push_back(STANDARD_TYPE_CHAR);
    params_1C_1V.push_back(STANDARD_TYPE_VARCHAR);

    vector<StandardTypeDescriptorOrdinal> params_1V_1V;
    params_1V_1V.push_back(STANDARD_TYPE_VARCHAR);
    params_1V_1V.push_back(STANDARD_TYPE_VARCHAR);

    vector<StandardTypeDescriptorOrdinal> params_1C_1C;
    params_1C_1C.push_back(STANDARD_TYPE_CHAR);
    params_1C_1C.push_back(STANDARD_TYPE_CHAR);

    eit->add("castA", params_1I_1C,
             (ExtendedInstruction2<int64_t, char*>*) NULL,
             &castStrToExactA);
    eit->add("castA", params_1I_1V,
             (ExtendedInstruction2<int64_t, char*>*) NULL,
             &castStrToExactA);

    eit->add("castA", params_1I_1C_SP,
             (ExtendedInstruction4<int64_t, char*, int32_t, int32_t>*) NULL,
             &castStrToExactA);
    eit->add("castA", params_1I_1V_SP,
             (ExtendedInstruction4<int64_t, char*, int32_t, int32_t>*) NULL,
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

    eit->add("castA", params_1C_1I_SP,
             (ExtendedInstruction4<char*, int64_t, int32_t, int32_t>*) NULL,
             &castExactToStrA);
    eit->add("castA", params_1V_1I_SP,
             (ExtendedInstruction4<char*, int64_t, int32_t, int32_t>*) NULL,
             &castExactToStrA);

    eit->add("castA", params_1C_1D,
             (ExtendedInstruction2<char*, double>*) NULL,
             &castApproxToStrA);
    eit->add("castA", params_1V_1D,
             (ExtendedInstruction2<char*, double>*) NULL,
             &castApproxToStrA);

    eit->add("castA", params_1C_1V,
             (ExtendedInstruction2<char*, char*>*) NULL,
             &castStrToCharA);
    eit->add("castA", params_1C_1C,
             (ExtendedInstruction2<char*, char*>*) NULL,
             &castStrToCharA);

    eit->add("castA", params_1V_1C,
             (ExtendedInstruction2<char*, char*>*) NULL,
             &castStrToVarCharA);
    eit->add("castA", params_1V_1V,
             (ExtendedInstruction2<char*, char*>*) NULL,
             &castStrToVarCharA);
}


FENNEL_END_NAMESPACE

        
