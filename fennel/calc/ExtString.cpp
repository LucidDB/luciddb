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
strCatA2(RegisterRef<char*>* result,
         RegisterRef<char*>* str1)
{
    assert(StandardTypeDescriptor::isTextArray(str1->type()));

    if (str1->isNull()) {
        result->toNull();
        result->length(0);
    } else {
        result->length(SqlStrCat(result->pointer(),
                                 result->storage(),
                                 result->length(),
                                 str1->pointer(),
                                 str1->stringLength()));
    }
}

void
strCatA3(RegisterRef<char*>* result,
         RegisterRef<char*>* str1,
         RegisterRef<char*>* str2)
{
    assert(StandardTypeDescriptor::isTextArray(str1->type()));

    if (str1->isNull() || str2->isNull()) {
        result->toNull();
        result->length(0);
    } else {
        result->length(SqlStrCat(result->pointer(),
                                 result->storage(),
                                 str1->pointer(),
                                 str1->stringLength(),
                                 str2->pointer(),
                                 str2->stringLength()));
    }
}

void
strCmpA(RegisterRef<int32_t>* result,
        RegisterRef<char*>* str1,
        RegisterRef<char*>* str2)
{
    assert(str1->type() == str2->type());
    assert(StandardTypeDescriptor::isTextArray(str1->type()));

    if (str1->isNull() || str2->isNull()) {
        result->toNull();
    } else {
        if (str1->type() == STANDARD_TYPE_CHAR) {
            result->value(SqlStrCmp_Fix<1,1>
                          (str1->pointer(),
                           str1->storage(),
                           str2->pointer(),
                           str2->storage()));
        } else {
            assert(str1->type()== STANDARD_TYPE_VARCHAR);
            result->value(SqlStrCmp_Var<1,1>
                          (str1->pointer(),
                           str1->length(),
                           str2->pointer(),
                           str2->length()));
        }
    }
}

void
strCpyA(RegisterRef<char*>* result,
        RegisterRef<char*>* str)
{
    assert(result->type() == str->type());
    assert(StandardTypeDescriptor::isTextArray(str->type()));

    if (str->isNull()) {
        result->toNull();
        result->length(0);
    } else {
        if (str->type() == STANDARD_TYPE_CHAR) {
            result->length(SqlStrCpy_Fix<1,1>(result->pointer(),
                                              result->storage(),
                                              str->pointer(),
                                              str->stringLength()));
        } else {
            result->length(SqlStrCpy_Var(result->pointer(),
                                         result->storage(),
                                         str->pointer(),
                                         str->stringLength()));
        }
    }
}


void
strLenBitA(RegisterRef<int32_t>* result,
           RegisterRef<char*>* str)
{
    assert(StandardTypeDescriptor::isTextArray(str->type()));

    if (str->isNull()) {
        result->toNull();
    } else {
        result->value(SqlStrLenBit(str->stringLength()));
    }
}
    
void
strLenCharA(RegisterRef<int32_t>* result,
            RegisterRef<char*>* str)
{
    assert(StandardTypeDescriptor::isTextArray(str->type()));

    if (str->isNull()) {
        result->toNull();
    } else {
        result->value(SqlStrLenChar<1,1>
                      (str->pointer(),
                       str->stringLength()));
    }
}

void
strLenOctA(RegisterRef<int32_t>* result,
           RegisterRef<char*>* str)
{
    assert(StandardTypeDescriptor::isTextArray(str->type()));

    if (str->isNull()) {
        result->toNull();
    } else {
        result->value(SqlStrLenOct(str->stringLength()));
    }
}

void
strOverlayA4(RegisterRef<char*>* result,
             RegisterRef<char*>* str,
             RegisterRef<char*>* overlay,
             RegisterRef<int32_t>* start)
{
    assert(result->type() == STANDARD_TYPE_VARCHAR);
    assert(str->type() == overlay->type());
    assert(StandardTypeDescriptor::isTextArray(str->type()));

    if (str->isNull() || overlay->isNull() || start->isNull()) {
        result->toNull();
        result->length(0);
    } else {
        result->length(SqlStrOverlay<1,1>
                       (result->pointer(),
                        result->storage(),
                        str->pointer(),
                        str->stringLength(),
                        overlay->pointer(),
                        overlay->stringLength(),
                        start->value(),
                        0,
                        false));
    }
}

void
strOverlayA5(RegisterRef<char*>* result,
             RegisterRef<char*>* str,
             RegisterRef<char*>* overlay,
             RegisterRef<int32_t>* start,
             RegisterRef<int32_t>* len)
{
    assert(result->type() == STANDARD_TYPE_VARCHAR);
    assert(str->type() == overlay->type());
    assert(StandardTypeDescriptor::isTextArray(str->type()));

    if (str->isNull() || overlay->isNull() || start->isNull() || len->isNull()) {
        result->toNull();
        result->length(0);
    } else {
        result->length(SqlStrOverlay<1,1>(result->pointer(),
                                          result->storage(),
                                          str->pointer(),
                                          str->stringLength(),
                                          overlay->pointer(),
                                          overlay->stringLength(),
                                          start->value(),
                                          len->value(),
                                          true));
    }
}

void
strPosA(RegisterRef<int32_t>* result,
        RegisterRef<char*>* str,
        RegisterRef<char*>* find)
{
    assert(str->type() == find->type());
    assert(StandardTypeDescriptor::isTextArray(str->type()));

    if (str->isNull() || find->isNull()) {
        result->toNull();
    } else {
        result->value(SqlStrPos<1,1>(str->pointer(),
                                     str->stringLength(),
                                     find->pointer(),
                                     find->stringLength()));
    }
}

void
strSubStringA3(RegisterRef<char*>* result,
               RegisterRef<char*>* str,
               RegisterRef<int32_t>* start)
{
    assert(result->type() == STANDARD_TYPE_VARCHAR);
    assert(StandardTypeDescriptor::isTextArray(str->type()));

    if (str->isNull() || start->isNull()) {
        result->toNull();
        result->length(0);
    } else {
        // Don't try anything fancy with RegisterRef accessors. KISS.
        char * ptr = result->pointer(); // preserve old value if possible
        // TODO: Not sure why cast from char* to char const * is required below.
        int32_t newLen = SqlStrSubStr<1,1>(const_cast<char const **>(&ptr),
                                           result->storage(),
                                           str->pointer(),
                                           str->stringLength(),
                                           start->value(),
                                           0,
                                           false);
        result->pointer(ptr, newLen);
    }
}

void
strSubStringA4(RegisterRef<char*>* result,
               RegisterRef<char*>* str,
               RegisterRef<int32_t>* start,
               RegisterRef<int32_t>* len)
{
    assert(result->type() == STANDARD_TYPE_VARCHAR);
    assert(StandardTypeDescriptor::isTextArray(str->type()));

    if (str->isNull() || start->isNull() || len->isNull()) {
        result->toNull();
        result->length(0);
    } else {
        // Don't try anything fancy with RegisterRef accessors. KISS.
        char * ptr = result->pointer(); // preserve old value if possible
        // TODO: Not sure why cast from char* to char const * is required below.
        int32_t newLen = SqlStrSubStr<1,1>(const_cast<char const **>(&ptr),
                                           result->storage(),
                                           str->pointer(),
                                           str->stringLength(),
                                           start->value(),
                                           len->value(),
                                           true);
        result->pointer(ptr, newLen);
    }
}

void
strToLowerA(RegisterRef<char*>* result,
            RegisterRef<char*>* str)
{
    assert(StandardTypeDescriptor::isTextArray(str->type()));
    assert(str->type() == result->type());
    assert(str->type() == STANDARD_TYPE_CHAR ? (result->storage() == str->storage()) : true);

    if (str->isNull()) {
        result->toNull();
        result->length(0);
    } else {
        // fixed width case: length should be harmlessly reset to same value
        result->length(SqlStrAlterCase
                       <1,1,AlterCaseLower>
                       (result->pointer(),
                        result->storage(),
                        str->pointer(),
                        str->stringLength()));
    }
}

void
strToUpperA(RegisterRef<char*>* result,
            RegisterRef<char*>* str)
{
    assert(StandardTypeDescriptor::isTextArray(str->type()));
    assert(str->type() == result->type());
    assert(str->type() == STANDARD_TYPE_CHAR ? (result->storage() == str->storage()) : true);

    if (str->isNull()) {
        result->toNull();
        result->length(0);
    } else {
        // fixed width case: length should be harmlessly reset to same value
        result->length(SqlStrAlterCase
                       <1,1,AlterCaseUpper>
                       (result->pointer(),
                        result->storage(),
                        str->pointer(),
                        str->stringLength()));
    }
}


void
strTrimA(RegisterRef<char*>* result,
         RegisterRef<char*>* str,
         RegisterRef<int32_t>* trimLeft,
         RegisterRef<int32_t>* trimRight)
{
    assert(StandardTypeDescriptor::isTextArray(str->type()));
    assert(result->type() == STANDARD_TYPE_VARCHAR);

    if (str->isNull() || trimLeft->isNull() || trimRight->isNull()) {
        result->toNull();
        result->length(0);
    } else {
        // Don't try anything fancy with RegisterRef accessors. KISS.
        char * ptr = result->pointer(); // preserve old value if possible
        // TODO: Not sure why cast from char* to char const * is required below.
        // use trim by reference function
        int32_t newLen = SqlStrTrim<1,1>(const_cast<char const **>(&ptr),
                                         str->pointer(),
                                         str->stringLength(),
                                         trimLeft->value(),
                                         trimRight->value());
        result->pointer(ptr, newLen);
    }
}


void
ExtStringRegister(ExtendedInstructionTable* eit)
{
    assert(eit != NULL);
    
    vector<StandardTypeDescriptorOrdinal> params_2F;
    params_2F.push_back(STANDARD_TYPE_CHAR);
    params_2F.push_back(STANDARD_TYPE_CHAR);

    vector<StandardTypeDescriptorOrdinal> params_2V;
    params_2V.push_back(STANDARD_TYPE_VARCHAR);
    params_2V.push_back(STANDARD_TYPE_VARCHAR);

    eit->add("strCatA2", params_2F,
             (ExtendedInstruction2<char*, char*>*) NULL,
             &strCatA2);
    eit->add("strCatA2", params_2V,
             (ExtendedInstruction2<char*, char*>*) NULL,
             &strCatA2);


    vector<StandardTypeDescriptorOrdinal> params_3F;
    params_3F.push_back(STANDARD_TYPE_CHAR);
    params_3F.push_back(STANDARD_TYPE_CHAR);
    params_3F.push_back(STANDARD_TYPE_CHAR);

    vector<StandardTypeDescriptorOrdinal> params_3V;
    params_3V.push_back(STANDARD_TYPE_VARCHAR);
    params_3V.push_back(STANDARD_TYPE_VARCHAR);
    params_3V.push_back(STANDARD_TYPE_VARCHAR);

    eit->add("strCatA3", params_3F,
             (ExtendedInstruction3<char*, char*, char*>*) NULL,
             &strCatA3);

    eit->add("strCatA3", params_3V,
             (ExtendedInstruction3<char*, char*, char*>*) NULL,
             &strCatA3);

    vector<StandardTypeDescriptorOrdinal> params_1N_2F;
    params_1N_2F.push_back(STANDARD_TYPE_INT_32);
    params_1N_2F.push_back(STANDARD_TYPE_CHAR);
    params_1N_2F.push_back(STANDARD_TYPE_CHAR);

    vector<StandardTypeDescriptorOrdinal> params_1N_2V;
    params_1N_2V.push_back(STANDARD_TYPE_INT_32);
    params_1N_2V.push_back(STANDARD_TYPE_VARCHAR);
    params_1N_2V.push_back(STANDARD_TYPE_VARCHAR);

    eit->add("strCmpA", params_1N_2F,
             (ExtendedInstruction3<int32_t, char*, char*>*) NULL,
             &strCmpA);

    eit->add("strCmpA", params_1N_2V,
             (ExtendedInstruction3<int32_t, char*, char*>*) NULL,
             &strCmpA);

    eit->add("strCpyA", params_2V,
             (ExtendedInstruction2<char*, char*>*) NULL,
             &strCpyA);

    eit->add("strCpyA", params_2F,
             (ExtendedInstruction2<char*, char*>*) NULL,
             &strCpyA);

    vector<StandardTypeDescriptorOrdinal> params_1N_1F;
    params_1N_1F.push_back(STANDARD_TYPE_INT_32);
    params_1N_1F.push_back(STANDARD_TYPE_CHAR);

    vector<StandardTypeDescriptorOrdinal> params_1N_1V;
    params_1N_1V.push_back(STANDARD_TYPE_INT_32);
    params_1N_1V.push_back(STANDARD_TYPE_VARCHAR);

    eit->add("strLenBitA", params_1N_1F,
             (ExtendedInstruction2<int32_t, char*>*) NULL,
             &strLenBitA);
    eit->add("strLenBitA", params_1N_1V,
             (ExtendedInstruction2<int32_t, char*>*) NULL,
             &strLenBitA);

    eit->add("strLenCharA", params_1N_1F,
             (ExtendedInstruction2<int32_t, char*>*) NULL,
             &strLenCharA);
    eit->add("strLenCharA", params_1N_1V,
             (ExtendedInstruction2<int32_t, char*>*) NULL,
             &strLenCharA);

    eit->add("strLenOctA", params_1N_1F,
             (ExtendedInstruction2<int32_t, char*>*) NULL,
             &strLenOctA);
    eit->add("strLenOctA", params_1N_1V,
             (ExtendedInstruction2<int32_t, char*>*) NULL,
             &strLenOctA);

    vector<StandardTypeDescriptorOrdinal> params_1V_2F_2I;
    params_1V_2F_2I.push_back(STANDARD_TYPE_VARCHAR);
    params_1V_2F_2I.push_back(STANDARD_TYPE_CHAR);
    params_1V_2F_2I.push_back(STANDARD_TYPE_CHAR);
    params_1V_2F_2I.push_back(STANDARD_TYPE_INT_32);
    params_1V_2F_2I.push_back(STANDARD_TYPE_INT_32);

    vector<StandardTypeDescriptorOrdinal> params_3V_2I;
    params_3V_2I.push_back(STANDARD_TYPE_VARCHAR);
    params_3V_2I.push_back(STANDARD_TYPE_VARCHAR);
    params_3V_2I.push_back(STANDARD_TYPE_VARCHAR);
    params_3V_2I.push_back(STANDARD_TYPE_INT_32);
    params_3V_2I.push_back(STANDARD_TYPE_INT_32);

    eit->add("strOverlayA5", params_1V_2F_2I,
             (ExtendedInstruction5<char*, char*, char*, int32_t, int32_t>*) NULL,
             &strOverlayA5);

    eit->add("strOverlayA5", params_3V_2I,
             (ExtendedInstruction5<char*, char*, char*, int32_t, int32_t>*) NULL,
             &strOverlayA5);

    vector<StandardTypeDescriptorOrdinal> params_1V_2F_1I;
    params_1V_2F_1I.push_back(STANDARD_TYPE_VARCHAR);
    params_1V_2F_1I.push_back(STANDARD_TYPE_CHAR);
    params_1V_2F_1I.push_back(STANDARD_TYPE_CHAR);
    params_1V_2F_1I.push_back(STANDARD_TYPE_INT_32);

    vector<StandardTypeDescriptorOrdinal> params_3V_1I;
    params_3V_1I.push_back(STANDARD_TYPE_VARCHAR);
    params_3V_1I.push_back(STANDARD_TYPE_VARCHAR);
    params_3V_1I.push_back(STANDARD_TYPE_VARCHAR);
    params_3V_1I.push_back(STANDARD_TYPE_INT_32);

    eit->add("strOverlayA4", params_1V_2F_1I,
             (ExtendedInstruction4<char*, char*, char*, int32_t>*) NULL,
             &strOverlayA4);

    eit->add("strOverlayA4", params_3V_1I,
             (ExtendedInstruction4<char*, char*, char*, int32_t>*) NULL,
             &strOverlayA4);


    eit->add("strPosA", params_1N_2F,
             (ExtendedInstruction3<int32_t, char*, char*>*) NULL,
             &strPosA);

    eit->add("strPosA", params_1N_2V,
             (ExtendedInstruction3<int32_t, char*, char*>*) NULL,
             &strPosA);

    vector<StandardTypeDescriptorOrdinal> params_1V_1F_1N;
    params_1V_1F_1N.push_back(STANDARD_TYPE_VARCHAR);
    params_1V_1F_1N.push_back(STANDARD_TYPE_CHAR);
    params_1V_1F_1N.push_back(STANDARD_TYPE_INT_32);

    vector<StandardTypeDescriptorOrdinal> params_2V_1N;
    params_2V_1N.push_back(STANDARD_TYPE_VARCHAR);
    params_2V_1N.push_back(STANDARD_TYPE_VARCHAR);
    params_2V_1N.push_back(STANDARD_TYPE_INT_32);

    eit->add("strSubStringA3", params_1V_1F_1N,
             (ExtendedInstruction3<char*, char*, int32_t>*) NULL,
             &strSubStringA3);
    
    eit->add("strSubStringA3", params_2V_1N,
             (ExtendedInstruction3<char*, char*, int32_t>*) NULL,
             &strSubStringA3);

    vector<StandardTypeDescriptorOrdinal> params_1V_1F_2N;
    params_1V_1F_2N.push_back(STANDARD_TYPE_VARCHAR);
    params_1V_1F_2N.push_back(STANDARD_TYPE_CHAR);
    params_1V_1F_2N.push_back(STANDARD_TYPE_INT_32);
    params_1V_1F_2N.push_back(STANDARD_TYPE_INT_32);

    vector<StandardTypeDescriptorOrdinal> params_2V_2N;
    params_2V_2N.push_back(STANDARD_TYPE_VARCHAR);
    params_2V_2N.push_back(STANDARD_TYPE_VARCHAR);
    params_2V_2N.push_back(STANDARD_TYPE_INT_32);
    params_2V_2N.push_back(STANDARD_TYPE_INT_32);

    eit->add("strSubStringA4", params_1V_1F_2N,
             (ExtendedInstruction4<char*, char*, int32_t, int32_t>*) NULL,
             &strSubStringA4);
    
    eit->add("strSubStringA4", params_2V_2N,
             (ExtendedInstruction4<char*, char*, int32_t, int32_t>*) NULL,
             &strSubStringA4);
    

    eit->add("strToLowerA", params_2F,
             (ExtendedInstruction2<char*, char*>*) NULL,
             &strToLowerA);

    eit->add("strToLowerA", params_2V,
             (ExtendedInstruction2<char*, char*>*) NULL,
             &strToLowerA);

    eit->add("strToUpperA", params_2F,
             (ExtendedInstruction2<char*, char*>*) NULL,
             &strToUpperA);

    eit->add("strToUpperA", params_2V,
             (ExtendedInstruction2<char*, char*>*) NULL,
             &strToUpperA);

    vector<StandardTypeDescriptorOrdinal> params_1V_1F_2I;
    params_1V_1F_2I.push_back(STANDARD_TYPE_VARCHAR);
    params_1V_1F_2I.push_back(STANDARD_TYPE_CHAR);
    params_1V_1F_2I.push_back(STANDARD_TYPE_INT_32);
    params_1V_1F_2I.push_back(STANDARD_TYPE_INT_32);

    eit->add("strTrimA", params_1V_1F_2I,
             (ExtendedInstruction4<char*, char*, int32_t, int32_t>*) NULL,
             &strTrimA);

    vector<StandardTypeDescriptorOrdinal> params_2V_2I;
    params_2V_2I.push_back(STANDARD_TYPE_VARCHAR);
    params_2V_2I.push_back(STANDARD_TYPE_VARCHAR);
    params_2V_2I.push_back(STANDARD_TYPE_INT_32);
    params_2V_2I.push_back(STANDARD_TYPE_INT_32);

    eit->add("strTrimA", params_2V_2I,
             (ExtendedInstruction4<char*, char*, int32_t, int32_t>*) NULL,
             &strTrimA);

}


FENNEL_END_NAMESPACE

        
