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

FENNEL_BEGIN_CPPFILE("$Id$");

void
ConvertDateToString(
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
        char * ptr = result->pointer(); // preserve old value if possible
        // FIXME use real date library
        char buf[11];           // extra byte for NUL
        int64_t d = date->value();
        int64_t daysSinceEpoch = d / (24 * 60 * 60 * 1000);
        int year = (daysSinceEpoch / 365) + 1970;
        int month = daysSinceEpoch / 30 + 1;
        int day = daysSinceEpoch % 30 + 1;
        int len = sprintf(buf, "%04d-%02d-%02d", year, month, day);
        assert(len == 10);
        memcpy(ptr, buf, len);
        result->pointer(ptr, len);
    }
}
void foo(char c) 
{}

void
ExtDateTimeRegister(ExtendedInstructionTable* eit)
{
    assert(eit != NULL);

    vector<StandardTypeDescriptorOrdinal> params_V_I64;
    params_V_I64.push_back(STANDARD_TYPE_VARCHAR);
    params_V_I64.push_back(STANDARD_TYPE_INT_64);

    eit->add("ConvertDateToString", params_V_I64,
             (ExtendedInstruction2<char*, int64_t>*) NULL,
             &ConvertDateToString);
}


FENNEL_END_CPPFILE("$Id$");
        
