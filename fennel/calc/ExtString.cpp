/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 2004-2004 Disruptive Technologies, Inc.
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

#include "fennel/calc/ExtString.h"
#include "fennel/calc/SqlString.h"
#include "fennel/calc/ExtendedInstructionTable.h"

FENNEL_BEGIN_NAMESPACE


void
strCatA2(Calculator *pCalc,
         RegisterRef<char*>* result,
         RegisterRef<char*>* op1)
{
    assert(op1->type() == STANDARD_TYPE_CHAR || op1->type() == STANDARD_TYPE_VARCHAR);

    if (op1->isNull()) {
        result->toNull();
        result->putS(0);
    } else {
        result->putS(SqlStrAsciiCat(result->getP(),
                                    result->getSMax(),
                                    result->getS(),
                                    op1->getP(),
                                    op1->getSStr()));
    }
}

void
strCatA3(Calculator *pCalc,
         RegisterRef<char*>* result,
         RegisterRef<char*>* op1,
         RegisterRef<char*>* op2)
{
    assert(op1->type() == STANDARD_TYPE_CHAR || op1->type() == STANDARD_TYPE_VARCHAR);

    if (op1->isNull() || op2->isNull()) {
        result->toNull();
        result->putS(0);
    } else {
        result->putS(SqlStrAsciiCat(result->getP(),
                                    result->getSMax(),
                                    op1->getP(),
                                    op1->getSStr(),
                                    op2->getP(),
                                    op2->getSStr()));
    }
}

void
strCmpA(Calculator *pCalc,
        RegisterRef<int32_t>* result,
        RegisterRef<char*>* op1,
        RegisterRef<char*>* op2)
{
    assert(op1->type() == op2->type());
    assert(op1->type() == STANDARD_TYPE_CHAR || op1->type() == STANDARD_TYPE_VARCHAR);

    if (op1->isNull() || op2->isNull()) {
        result->toNull();
    } else {
        if (op1->type() == STANDARD_TYPE_CHAR) {
            result->putV(SqlStrAsciiCmpF(op1->getP(),
                                         op1->getSMax(),
                                         op2->getP(),
                                         op2->getSMax()));
        } else {
            assert(op1->type()== STANDARD_TYPE_VARCHAR);
            result->putV(SqlStrAsciiCmpV(op1->getP(),
                                         op1->getS(),
                                         op2->getP(),
                                         op2->getS()));
        }
    }
}

void
strLenBitA(Calculator *pCalc,
           RegisterRef<int32_t>* result,
           RegisterRef<char*>* op1)
{
    assert(op1->type() == STANDARD_TYPE_CHAR || op1->type() == STANDARD_TYPE_VARCHAR);

    if (op1->isNull()) {
        result->toNull();
    } else {
        result->putV(SqlStrAsciiLenBit(op1->getP(),
                                       op1->getSStr()));
    }
}
    
void
strLenCharA(Calculator *pCalc,
            RegisterRef<int32_t>* result,
            RegisterRef<char*>* op1)
{
    assert(op1->type() == STANDARD_TYPE_CHAR || op1->type() == STANDARD_TYPE_VARCHAR);

    if (op1->isNull()) {
        result->toNull();
    } else {
        printf("strLenCharA S=%d SMax=%d SStr=%d\n",
               op1->getS(), op1->getSMax(), op1->getSStr());
        result->putV(SqlStrAsciiLenChar(op1->getP(),
                                        op1->getSStr()));
    }
}

void
strLenOctA(Calculator *pCalc,
           RegisterRef<int32_t>* result,
           RegisterRef<char*>* op1)
{
    assert(op1->type() == STANDARD_TYPE_CHAR || op1->type() == STANDARD_TYPE_VARCHAR);

    if (op1->isNull()) {
        result->toNull();
    } else {
        result->putV(SqlStrAsciiLenOct(op1->getP(),
                                       op1->getSStr()));
    }
}

void
strOverlayA5(Calculator *pCalc,
             RegisterRef<char*> *result,
             RegisterRef<char*> *str,
             RegisterRef<char*> *overlay,
             RegisterRef<int32_t> *start,
             RegisterRef<int32_t> *len)
{
    assert(result->type() == STANDARD_TYPE_VARCHAR);
    assert(str->type() == overlay->type());
    assert(str->type() == STANDARD_TYPE_CHAR || str->type() == STANDARD_TYPE_VARCHAR);

    if (str->isNull() || overlay->isNull() || start->isNull() || len->isNull()) {
        result->toNull();
        result->putS(0);
    } else {
        result->putS(SqlStrAsciiOverlay(result->getP(),
                                        result->getSMax(),
                                        str->getP(),
                                        str->getSStr(),
                                        overlay->getP(),
                                        overlay->getSStr(),
                                        start->getV(),
                                        len->getV(),
                                        true));
    }
}

void
strOverlayA4(Calculator *pCalc,
             RegisterRef<char*> *result,
             RegisterRef<char*> *str,
             RegisterRef<char*> *overlay,
             RegisterRef<int32_t> *start)
{
    assert(result->type() == STANDARD_TYPE_VARCHAR);
    assert(str->type() == overlay->type());
    assert(str->type() == STANDARD_TYPE_CHAR || str->type() == STANDARD_TYPE_VARCHAR);

    if (str->isNull() || overlay->isNull() || start->isNull()) {
        result->toNull();
        result->putS(0);
    } else {
        result->putS(SqlStrAsciiOverlay(result->getP(),
                                        result->getSMax(),
                                        str->getP(),
                                        str->getSStr(),
                                        overlay->getP(),
                                        overlay->getSStr(),
                                        start->getV(),
                                        0,
                                        false));
    }
}


void
strRegister()
{
    ExtendedInstructionTable* eit = ExtendedInstructionTable::instance();
    
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
    
}


FENNEL_END_NAMESPACE

        
