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
#include "fennel/calc/ExtendedInstructionTable.h"
#include "fennel/calc/SqlDate.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void
CastDateToStrA(
        RegisterRef<char*>* result,
        RegisterRef<int64_t>* date)
{
    assert(date->type() == STANDARD_TYPE_INT_64);
    assert(result->type() == STANDARD_TYPE_VARCHAR);

    if (date->isNull()) {
        result->toNull();
        result->length(0);
    } else {
        // Produce a result like "2004-05-12"
        int64_t v = date->value() * 1000;
        int len = SqlDateToStr<1,1,SQLDATE>(result->pointer(), result->storage(),v);
        result->length(len);
    }
}

void
CastTimeToStrA(
        RegisterRef<char*>* result,
        RegisterRef<int64_t>* time)
{
    assert(time->type() == STANDARD_TYPE_INT_64);
    assert(result->type() == STANDARD_TYPE_VARCHAR);

    if (time->isNull()) {
        result->toNull();
        result->length(0);
    } else {
        int64_t v = time->value() * 1000;
        int len = SqlDateToStr<1,1,SQLTIME>(result->pointer(), result->storage(),v);
        result->length(len);
    }
}

void
CastTimestampToStrA(
        RegisterRef<char*>* result,
        RegisterRef<int64_t>* tstamp)
{
    assert(tstamp->type() == STANDARD_TYPE_INT_64);
    assert(result->type() == STANDARD_TYPE_VARCHAR);

    if (tstamp->isNull()) {
        result->toNull();
        result->length(0);
    } else {
        int64_t v = tstamp->value() * 1000;
        int len = SqlDateToStr<1,1,SQLTIMESTAMP>(result->pointer(), result->storage(),v);
        result->length(len);
    }
}


void
ExtDateTimeRegister(ExtendedInstructionTable* eit)
{
    assert(eit != NULL);

    vector<StandardTypeDescriptorOrdinal> params_V_I64;
    params_V_I64.push_back(STANDARD_TYPE_VARCHAR);
    params_V_I64.push_back(STANDARD_TYPE_INT_64);

    eit->add("CastDateToStrA", params_V_I64,
             (ExtendedInstruction2<char*, int64_t>*) NULL,
             &CastDateToStrA);

    eit->add("CastTimeToStrA", params_V_I64,
             (ExtendedInstruction2<char*, int64_t>*) NULL,
             &CastTimeToStrA);

    eit->add("CastTimestampToStrA", params_V_I64,
             (ExtendedInstruction2<char*, int64_t>*) NULL,
             &CastTimestampToStrA);


}


FENNEL_END_CPPFILE("$Id$");
        
