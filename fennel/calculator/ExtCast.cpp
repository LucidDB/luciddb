/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2004-2010 SQLstream, Inc.
// Copyright (C) 2009-2009 LucidEra, Inc.
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
#include "fennel/calculator/SqlString.h"
#include "fennel/calculator/ExtendedInstructionTable.h"

FENNEL_BEGIN_NAMESPACE

void
castExactToStrA(
    RegisterRef<char*>* result,
    RegisterRef<int64_t>* src)
{
    assert(StandardTypeDescriptor::isTextArray(result->type()));

    if (src->isNull()) {
        result->toNull();
        result->length(0);
    } else {
        result->length(
            SqlStrCastFromExact<1, 1>(
                result->pointer(),
                result->storage(),
                src->value(),
                (result->type() == STANDARD_TYPE_CHAR ? true : false)));
    }
}

void
castExactToStrA(
    RegisterRef<char*>* result,
    RegisterRef<int64_t>* src,
    RegisterRef<int32_t>* precision,
    RegisterRef<int32_t>* scale)
{
    assert(StandardTypeDescriptor::isTextArray(result->type()));

    if (src->isNull()) {
        result->toNull();
        result->length(0);
    } else {
         result->length(
             SqlStrCastFromExact<1, 1>(
                 result->pointer(),
                 result->storage(),
                 src->value(),
                 precision->value(),
                 scale->value(),
                 (result->type() == STANDARD_TYPE_CHAR ? true : false)));
    }
}

void
castApproxToStrA(
    RegisterRef<char*>* result,
    RegisterRef<float>* src)
{
    assert(StandardTypeDescriptor::isTextArray(result->type()));

    if (src->isNull()) {
        result->toNull();
        result->length(0);
    } else {
        result->length(
            SqlStrCastFromApprox<1, 1>(
                result->pointer(),
                result->storage(),
                src->value(),
                true,
                (result->type() == STANDARD_TYPE_CHAR ? true : false)));
    }
}

void
castApproxToStrA(
    RegisterRef<char*>* result,
    RegisterRef<double>* src)
{
    assert(StandardTypeDescriptor::isTextArray(result->type()));

    if (src->isNull()) {
        result->toNull();
        result->length(0);
    } else {
        result->length(
            SqlStrCastFromApprox<1, 1>(
                result->pointer(),
                result->storage(),
                src->value(),
                false,
                (result->type() == STANDARD_TYPE_CHAR ? true : false)));
    }
}

void
castStrToExactA(
    RegisterRef<int64_t>* result,
    RegisterRef<char*>* src)
{
    assert(StandardTypeDescriptor::isTextArray(src->type()));

    if (src->isNull()) {
        result->toNull();
    } else {
        result->value(
            SqlStrCastToExact<1, 1>(
                src->pointer(),
                src->stringLength()));
    }
}


void
castStrToExactA(
    RegisterRef<int64_t>* result,
    RegisterRef<char*>* src,
    RegisterRef<int32_t>* precision,
    RegisterRef<int32_t>* scale)
{
    assert(StandardTypeDescriptor::isTextArray(src->type()));

    if (src->isNull()) {
        result->toNull();
    } else {
        result->value(
            SqlStrCastToExact<1, 1>(
                src->pointer(),
                src->stringLength(),
                precision->value(),
                scale->value()));
    }
}

void
castStrToApproxA(
    RegisterRef<double>* result,
    RegisterRef<char*>* src)
{
    assert(StandardTypeDescriptor::isTextArray(src->type()));

    if (src->isNull()) {
        result->toNull();
    } else {
        result->value(
            SqlStrCastToApprox<1, 1>(
                src->pointer(),
                src->stringLength()));
    }
}

void
castBooleanToStrA(
    RegisterRef<char*>* result,
    RegisterRef<bool>* src)
{
    assert(StandardTypeDescriptor::isTextArray(result->type()));

    if (src->isNull()) {
        result->toNull();
        result->length(0);
    } else {
        result->length(
            SqlStrCastFromBoolean<1, 1>(
                result->pointer(),
                result->storage(),
                src->value(),
                (result->type() == STANDARD_TYPE_CHAR ? true : false)));
    }
}

void
castStrToBooleanA(
    RegisterRef<bool>* result,
    RegisterRef<char*>* src)
{
    assert(StandardTypeDescriptor::isTextArray(src->type()));

    if (src->isNull()) {
        result->toNull();
    } else {
        result->value(
            SqlStrCastToBoolean<1, 1>(
                src->pointer(),
                src->stringLength()));
    }
}


// TODO: cases where the result is smaller than the input could
// probably benefit from operating by reference instead of by value
void
castStrToVarCharA(
    RegisterRef<char*>* result,
    RegisterRef<char*>* src)
{
    assert(StandardTypeDescriptor::isArray(result->type()));
    assert(StandardTypeDescriptor::isArray(src->type()));

    if (src->isNull()) {
        result->toNull();
        result->length(0);
    } else {
        int rightTruncWarning = 0;
        result->length(
            SqlStrCastToVarChar<1, 1>(
                result->pointer(),
                result->storage(),
                src->pointer(),
                src->stringLength(),
                &rightTruncWarning));
        if (rightTruncWarning) {
            // TODO: throw 22001 as a warning
//            throw SqlState::instance().code22001();
        }
    }
}

// TODO: cases where the result is smaller than the input could
// probably benefit from operating by reference instead of by value
void
castStrToCharA(
    RegisterRef<char*>* result,
    RegisterRef<char*>* src)
{
    assert(StandardTypeDescriptor::isArray(result->type()));
    assert(StandardTypeDescriptor::isArray(src->type()));

    if (src->isNull()) {
        result->toNull();
        result->length(0);
    } else {
        int rightTruncWarning = 0;
        result->length(
            SqlStrCastToChar<1, 1>(
                result->pointer(),
                result->storage(),
                src->pointer(),
                src->stringLength(),
                &rightTruncWarning));

        if (rightTruncWarning) {
            // TODO: throw 22001 as a warning
//            throw SqlState::instance().code22001();
        }
    }
}


// TODO: cases where the result is smaller than the input could
// probably benefit from operating by reference instead of by value
void
castStrToVarBinaryA(
    RegisterRef<char*>* result,
    RegisterRef<char*>* src)
{
    assert(StandardTypeDescriptor::isArray(result->type()));
    assert(StandardTypeDescriptor::isArray(src->type()));

    if (src->isNull()) {
        result->toNull();
        result->length(0);
    } else {
        int rightTruncWarning = 0;
        result->length(
            SqlStrCastToVarChar<1, 1>(
                result->pointer(),
                result->storage(),
                src->pointer(),
                src->stringLength(),
                &rightTruncWarning,
                0));
        if (rightTruncWarning) {
            // TODO: throw 22001 as a warning
//            throw SqlState::instance().code22001();
        }
    }
}

// TODO: cases where the result is smaller than the input could
// probably benefit from operating by reference instead of by value
void
castStrToBinaryA(
    RegisterRef<char*>* result,
    RegisterRef<char*>* src)
{
    assert(StandardTypeDescriptor::isArray(result->type()));
    assert(StandardTypeDescriptor::isArray(src->type()));

    if (src->isNull()) {
        result->toNull();
        result->length(0);
    } else {
        int rightTruncWarning = 0;
        result->length(
            SqlStrCastToChar<1, 1>(
                result->pointer(),
                result->storage(),
                src->pointer(),
                src->stringLength(),
                &rightTruncWarning,
                0));

        if (rightTruncWarning) {
            // TODO: throw 22001 as a warning
//            throw SqlState::instance().code22001();
        }
    }
}


void
ExtCastRegister(ExtendedInstructionTable* eit)
{
    assert(eit != NULL);

    vector<StandardTypeDescriptorOrdinal> params_1bo_1c;
    params_1bo_1c.push_back(STANDARD_TYPE_BOOL);
    params_1bo_1c.push_back(STANDARD_TYPE_CHAR);

    vector<StandardTypeDescriptorOrdinal> params_1bo_1vc;
    params_1bo_1vc.push_back(STANDARD_TYPE_BOOL);
    params_1bo_1vc.push_back(STANDARD_TYPE_VARCHAR);

    vector<StandardTypeDescriptorOrdinal> params_1s8_1c;
    params_1s8_1c.push_back(STANDARD_TYPE_INT_64);
    params_1s8_1c.push_back(STANDARD_TYPE_CHAR);

    vector<StandardTypeDescriptorOrdinal> params_1s8_1vc;
    params_1s8_1vc.push_back(STANDARD_TYPE_INT_64);
    params_1s8_1vc.push_back(STANDARD_TYPE_VARCHAR);

    vector<StandardTypeDescriptorOrdinal> params_1s8_1c_PS;
    params_1s8_1c_PS.push_back(STANDARD_TYPE_INT_64);
    params_1s8_1c_PS.push_back(STANDARD_TYPE_CHAR);
    params_1s8_1c_PS.push_back(STANDARD_TYPE_INT_32);
    params_1s8_1c_PS.push_back(STANDARD_TYPE_INT_32);

    vector<StandardTypeDescriptorOrdinal> params_1s8_1vc_PS;
    params_1s8_1vc_PS.push_back(STANDARD_TYPE_INT_64);
    params_1s8_1vc_PS.push_back(STANDARD_TYPE_VARCHAR);
    params_1s8_1vc_PS.push_back(STANDARD_TYPE_INT_32);
    params_1s8_1vc_PS.push_back(STANDARD_TYPE_INT_32);

    vector<StandardTypeDescriptorOrdinal> params_1d_1c;
    params_1d_1c.push_back(STANDARD_TYPE_DOUBLE);
    params_1d_1c.push_back(STANDARD_TYPE_CHAR);

    vector<StandardTypeDescriptorOrdinal> params_1d_1vc;
    params_1d_1vc.push_back(STANDARD_TYPE_DOUBLE);
    params_1d_1vc.push_back(STANDARD_TYPE_VARCHAR);

    vector<StandardTypeDescriptorOrdinal> params_1c_1bo;
    params_1c_1bo.push_back(STANDARD_TYPE_CHAR);
    params_1c_1bo.push_back(STANDARD_TYPE_BOOL);

    vector<StandardTypeDescriptorOrdinal> params_1vc_1bo;
    params_1vc_1bo.push_back(STANDARD_TYPE_VARCHAR);
    params_1vc_1bo.push_back(STANDARD_TYPE_BOOL);

    vector<StandardTypeDescriptorOrdinal> params_1c_1s8;
    params_1c_1s8.push_back(STANDARD_TYPE_CHAR);
    params_1c_1s8.push_back(STANDARD_TYPE_INT_64);

    vector<StandardTypeDescriptorOrdinal> params_1vc_1s8;
    params_1vc_1s8.push_back(STANDARD_TYPE_VARCHAR);
    params_1vc_1s8.push_back(STANDARD_TYPE_INT_64);

    vector<StandardTypeDescriptorOrdinal> params_1c_1s8_PS;
    params_1c_1s8_PS.push_back(STANDARD_TYPE_CHAR);
    params_1c_1s8_PS.push_back(STANDARD_TYPE_INT_64);
    params_1c_1s8_PS.push_back(STANDARD_TYPE_INT_32);
    params_1c_1s8_PS.push_back(STANDARD_TYPE_INT_32);

    vector<StandardTypeDescriptorOrdinal> params_1vc_1s8_PS;
    params_1vc_1s8_PS.push_back(STANDARD_TYPE_VARCHAR);
    params_1vc_1s8_PS.push_back(STANDARD_TYPE_INT_64);
    params_1vc_1s8_PS.push_back(STANDARD_TYPE_INT_32);
    params_1vc_1s8_PS.push_back(STANDARD_TYPE_INT_32);

    vector<StandardTypeDescriptorOrdinal> params_1c_1d;
    params_1c_1d.push_back(STANDARD_TYPE_CHAR);
    params_1c_1d.push_back(STANDARD_TYPE_DOUBLE);

    vector<StandardTypeDescriptorOrdinal> params_1vc_1d;
    params_1vc_1d.push_back(STANDARD_TYPE_VARCHAR);
    params_1vc_1d.push_back(STANDARD_TYPE_DOUBLE);

    vector<StandardTypeDescriptorOrdinal> params_1c_1r;
    params_1c_1r.push_back(STANDARD_TYPE_CHAR);
    params_1c_1r.push_back(STANDARD_TYPE_REAL);

    vector<StandardTypeDescriptorOrdinal> params_1vc_1r;
    params_1vc_1r.push_back(STANDARD_TYPE_VARCHAR);
    params_1vc_1r.push_back(STANDARD_TYPE_REAL);

    vector<StandardTypeDescriptorOrdinal> params_1vc_1c;
    params_1vc_1c.push_back(STANDARD_TYPE_VARCHAR);
    params_1vc_1c.push_back(STANDARD_TYPE_CHAR);

    vector<StandardTypeDescriptorOrdinal> params_1c_1vc;
    params_1c_1vc.push_back(STANDARD_TYPE_CHAR);
    params_1c_1vc.push_back(STANDARD_TYPE_VARCHAR);

    vector<StandardTypeDescriptorOrdinal> params_1vc_1vc;
    params_1vc_1vc.push_back(STANDARD_TYPE_VARCHAR);
    params_1vc_1vc.push_back(STANDARD_TYPE_VARCHAR);

    vector<StandardTypeDescriptorOrdinal> params_1c_1c;
    params_1c_1c.push_back(STANDARD_TYPE_CHAR);
    params_1c_1c.push_back(STANDARD_TYPE_CHAR);

    vector<StandardTypeDescriptorOrdinal> params_1vb_1b;
    params_1vb_1b.push_back(STANDARD_TYPE_VARBINARY);
    params_1vb_1b.push_back(STANDARD_TYPE_BINARY);

    vector<StandardTypeDescriptorOrdinal> params_1b_1vb;
    params_1b_1vb.push_back(STANDARD_TYPE_BINARY);
    params_1b_1vb.push_back(STANDARD_TYPE_VARBINARY);

    vector<StandardTypeDescriptorOrdinal> params_1vb_1vb;
    params_1vb_1vb.push_back(STANDARD_TYPE_VARBINARY);
    params_1vb_1vb.push_back(STANDARD_TYPE_VARBINARY);

    vector<StandardTypeDescriptorOrdinal> params_1b_1b;
    params_1b_1b.push_back(STANDARD_TYPE_BINARY);
    params_1b_1b.push_back(STANDARD_TYPE_BINARY);

    eit->add(
        "castA", params_1bo_1c,
        (ExtendedInstruction2<bool, char*>*) NULL,
        &castStrToBooleanA);
    eit->add(
        "castA", params_1bo_1vc,
        (ExtendedInstruction2<bool, char*>*) NULL,
        &castStrToBooleanA);

    eit->add(
        "castA", params_1s8_1c,
        (ExtendedInstruction2<int64_t, char*>*) NULL,
        &castStrToExactA);
    eit->add(
        "castA", params_1s8_1vc,
        (ExtendedInstruction2<int64_t, char*>*) NULL,
        &castStrToExactA);

    eit->add(
        "castA", params_1s8_1c_PS,
        (ExtendedInstruction4<int64_t, char*, int32_t, int32_t>*) NULL,
        &castStrToExactA);
    eit->add(
        "castA", params_1s8_1vc_PS,
        (ExtendedInstruction4<int64_t, char*, int32_t, int32_t>*) NULL,
        &castStrToExactA);

    eit->add(
        "castA", params_1d_1c,
        (ExtendedInstruction2<double, char*>*) NULL,
        &castStrToApproxA);
    eit->add(
        "castA", params_1d_1vc,
        (ExtendedInstruction2<double, char*>*) NULL,
        &castStrToApproxA);

    eit->add(
        "castA", params_1c_1bo,
        (ExtendedInstruction2<char*, bool>*) NULL,
        &castBooleanToStrA);
    eit->add(
        "castA", params_1vc_1bo,
        (ExtendedInstruction2<char*, bool>*) NULL,
        &castBooleanToStrA);

    eit->add(
        "castA", params_1c_1s8,
        (ExtendedInstruction2<char*, int64_t>*) NULL,
        &castExactToStrA);
    eit->add(
        "castA", params_1vc_1s8,
        (ExtendedInstruction2<char*, int64_t>*) NULL,
        &castExactToStrA);

    eit->add(
        "castA", params_1c_1s8_PS,
        (ExtendedInstruction4<char*, int64_t, int32_t, int32_t>*) NULL,
        &castExactToStrA);
    eit->add(
        "castA", params_1vc_1s8_PS,
        (ExtendedInstruction4<char*, int64_t, int32_t, int32_t>*) NULL,
        &castExactToStrA);

    eit->add(
        "castA", params_1c_1d,
        (ExtendedInstruction2<char*, double>*) NULL,
        &castApproxToStrA);
    eit->add(
        "castA", params_1vc_1d,
        (ExtendedInstruction2<char*, double>*) NULL,
        &castApproxToStrA);

    eit->add(
        "castA", params_1c_1r,
        (ExtendedInstruction2<char*, float>*) NULL,
        &castApproxToStrA);
    eit->add(
        "castA", params_1vc_1r,
        (ExtendedInstruction2<char*, float>*) NULL,
        &castApproxToStrA);

    eit->add(
        "castA", params_1c_1vc,
        (ExtendedInstruction2<char*, char*>*) NULL,
        &castStrToCharA);
    eit->add(
        "castA", params_1c_1c,
        (ExtendedInstruction2<char*, char*>*) NULL,
        &castStrToCharA);

    eit->add(
        "castA", params_1vc_1c,
        (ExtendedInstruction2<char*, char*>*) NULL,
        &castStrToVarCharA);
    eit->add(
        "castA", params_1vc_1vc,
        (ExtendedInstruction2<char*, char*>*) NULL,
        &castStrToVarCharA);

    eit->add(
        "castA", params_1b_1vb,
        (ExtendedInstruction2<char*, char*>*) NULL,
        &castStrToBinaryA);
    eit->add(
        "castA", params_1b_1b,
        (ExtendedInstruction2<char*, char*>*) NULL,
        &castStrToBinaryA);

    eit->add(
        "castA", params_1vb_1b,
        (ExtendedInstruction2<char*, char*>*) NULL,
        &castStrToVarBinaryA);
    eit->add(
        "castA", params_1vb_1vb,
        (ExtendedInstruction2<char*, char*>*) NULL,
        &castStrToVarBinaryA);
}


FENNEL_END_NAMESPACE

// End ExtCast.cpp
