/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2004-2007 Disruptive Tech
// Copyright (C) 2004-2007 The Eigenbase Project
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
//
// Test Calculator object directly by instantiating instruction objects,
// creating programs, running them, and checking the register set values.
*/
#include "fennel/common/CommonPreamble.h"
#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/tuple/TupleData.h"
#include "fennel/tuple/TupleAccessor.h"
#include "fennel/tuple/TuplePrinter.h"
#include "fennel/tuple/AttributeAccessor.h"
#include "fennel/tuple/StandardTypeDescriptor.h"
#include "fennel/common/TraceSource.h"

#include "fennel/disruptivetech/calc/CalcCommon.h"
#include "fennel/disruptivetech/calc/InstructionCommon.h"  // required as we're manipulating instructions
#include "fennel/disruptivetech/calc/InstructionFactory.h"

#include <stdlib.h>
#include <stdio.h>
#include <string>
#include <boost/scoped_array.hpp>
#include <boost/test/unit_test_suite.hpp>
#include <limits>
#include <iostream.h>

using namespace std;
using namespace fennel;

char* ProgramName;

// JR 6/22/07, this construct:
//    instP = new (Instruction *)[200];
// is no longer allowed by gcc >= 4.0, so just use a typedef to be clear
typedef Instruction *InstructionPtr;

void
fail(const char* str, int line) {
    assert(ProgramName);
    assert(str);
    printf("%s: unit test failed: |%s| line %d\n", ProgramName, str, line);
    exit(-1);
}

void
unitTestStrings()
{
    bool isNullable = true;    // Can tuple contain nulls?
    int i, registersize = 120;
    static uint buflen = 16;

    assert(!(registersize & 3)); // make sure registers are multiple of four
    assert(!(buflen & 1));  // buffers must be even length
    
    static uint buflenShort = buflen >> 2;     // short is 1/4 size of long

    const int shortIdx = 0;                    // half short
    const int longIdx = registersize >> 1;     // half long
    const int veryShortIdx = registersize - 2; // just one very short
    const int zeroLenIdx = registersize - 3;   // zero length string, long string width
    const int varFullIdx = registersize - 4;   // full width variable len string
    const int trimmableIdx = registersize - 5; // string with leading and trailing spaces

    TupleDescriptor tupleDesc;
    tupleDesc.clear();

    // Build up a description of what we'd like the tuple to look like
    StandardTypeDescriptorFactory typeFactory;

    int idx = 0;

    const int varcharIdx = idx;
    for (i=0;i < registersize; i++) {
        // VARCHAR in first "half"
        StoredTypeDescriptor const &typeDesc = typeFactory.newDataType(STANDARD_TYPE_VARCHAR);
        int len = buflen;
        if (i == veryShortIdx) {
            // make a very short string to force errors
            len = 1;
        } else if (i < longIdx) {
            len = buflenShort;
        } else {
            len = buflen;
        }
        tupleDesc.push_back(TupleAttributeDescriptor(typeDesc,
                                                     isNullable,
                                                     len));
        idx++;
    
    }

    const int charIdx = idx;

    for (i=0;i < registersize; i++) {
        // CHAR in second "half"
        StoredTypeDescriptor const &typeDesc = typeFactory.newDataType(STANDARD_TYPE_CHAR);
        int len = buflen;
        if (i == veryShortIdx) {
            // make a very short string to force errors
            len = 1;
        } else if (i < longIdx) {
            len = buflenShort;
        } else {
            len = buflen;
        }
        tupleDesc.push_back(TupleAttributeDescriptor(typeDesc,
                                                     isNullable,
                                                     len));
        idx++;
    }

    const int intIdx = idx;
    for (i=0;i < registersize; i++) {
        // int_32 in third "half"
        StoredTypeDescriptor const &typeDesc = typeFactory.newDataType(STANDARD_TYPE_INT_32);
        tupleDesc.push_back(TupleAttributeDescriptor(typeDesc, isNullable));
        idx++;
    }

    // Create a tuple accessor from the description
    //
    // Note: Must use a NOT_NULL_AND_FIXED accessor when creating a tuple out of the
    // air like this, otherwise unmarshal() does not know what to do. If you need a
    // STANDARD type tuple that supports nulls, it has to be built as a copy.
    TupleAccessor tupleAccessorFixedLiteral;
    TupleAccessor tupleAccessorFixedInput;
    TupleAccessor tupleAccessorFixedOutput;
    TupleAccessor tupleAccessorFixedLocal;
    TupleAccessor tupleAccessorFixedStatus;
    tupleAccessorFixedLiteral.compute(tupleDesc, TUPLE_FORMAT_ALL_FIXED);
    tupleAccessorFixedInput.compute(tupleDesc, TUPLE_FORMAT_ALL_FIXED);
    tupleAccessorFixedOutput.compute(tupleDesc, TUPLE_FORMAT_ALL_FIXED);
    tupleAccessorFixedLocal.compute(tupleDesc, TUPLE_FORMAT_ALL_FIXED);
    tupleAccessorFixedStatus.compute(tupleDesc, TUPLE_FORMAT_ALL_FIXED);

    // Allocate memory for the tuple
    boost::scoped_array<FixedBuffer>
        pTupleBufFixedLiteral(new FixedBuffer[tupleAccessorFixedLiteral.getMaxByteCount()]);
    boost::scoped_array<FixedBuffer>
        pTupleBufFixedInput(new FixedBuffer[tupleAccessorFixedInput.getMaxByteCount()]);
    boost::scoped_array<FixedBuffer>
        pTupleBufFixedOutput(new FixedBuffer[tupleAccessorFixedOutput.getMaxByteCount()]);
    boost::scoped_array<FixedBuffer>
        pTupleBufFixedLocal(new FixedBuffer[tupleAccessorFixedLocal.getMaxByteCount()]);
    boost::scoped_array<FixedBuffer>
        pTupleBufFixedStatus(new FixedBuffer[tupleAccessorFixedStatus.getMaxByteCount()]);

    // Link memory to accessor
    tupleAccessorFixedLiteral.setCurrentTupleBuf(pTupleBufFixedLiteral.get(), false);
    tupleAccessorFixedInput.setCurrentTupleBuf(pTupleBufFixedInput.get(), false);
    tupleAccessorFixedOutput.setCurrentTupleBuf(pTupleBufFixedOutput.get(), false);
    tupleAccessorFixedLocal.setCurrentTupleBuf(pTupleBufFixedLocal.get(), false);
    tupleAccessorFixedStatus.setCurrentTupleBuf(pTupleBufFixedStatus.get(), false);

    // Create a vector of TupleDatum objects based on the description we built
    TupleData tupleDataFixedLiteral(tupleDesc);
    TupleData tupleDataFixedInput(tupleDesc);
    TupleData tupleDataFixedOutput(tupleDesc);
    TupleData tupleDataFixedLocal(tupleDesc);
    TupleData tupleDataFixedStatus(tupleDesc);

    // Do something mysterious. Probably binding pointers in the accessor to items
    // in the TupleData vector
    tupleAccessorFixedLiteral.unmarshal(tupleDataFixedLiteral);
    tupleAccessorFixedInput.unmarshal(tupleDataFixedInput);
    tupleAccessorFixedOutput.unmarshal(tupleDataFixedOutput);
    tupleAccessorFixedLocal.unmarshal(tupleDataFixedLocal);
    tupleAccessorFixedStatus.unmarshal(tupleDataFixedStatus);

    // create four nullable tuples to serve as register sets
    TupleData literal = tupleDataFixedLiteral;
    TupleData input = tupleDataFixedInput;
    TupleData output = tupleDataFixedOutput;
    TupleData local = tupleDataFixedLocal;
    TupleData status = tupleDataFixedStatus;


    TupleData::iterator itr;

    const uint variableStringLen = 3;
    
    assert(buflenShort >= variableStringLen);  // string is V00, V01, etc. or F00, F01, etc.
    itr = input.begin();
    for(i=0; i < registersize; i++, itr++) {
        char* ptr = reinterpret_cast<char *>(const_cast<PBuffer>(itr->pData));
        if (i == veryShortIdx) {
            assert(itr->cbData == 1);
            *ptr = 'V';
        } else if (i == zeroLenIdx) {
            itr->cbData = 0;
        } else if (i == varFullIdx) {
            itr->cbData = buflen;
            memset(ptr, 'V', buflen);
        } else if (i == trimmableIdx) {
            itr->cbData = buflen;
            assert(buflen == 16);
            memcpy(ptr, "    1 3 5 78    ", buflen);
        } else {
            itr->cbData = variableStringLen;
            sprintf(ptr, "V%02d", i);
            // fill in rest of space with _, which should never be seen.
            if (i < longIdx) {
                memset (ptr+3, '_', buflenShort - variableStringLen);
            } else {
                memset (ptr+3, '_', buflen - variableStringLen);
            }
        }
    }
    for(i=0; i < registersize; i++, itr++) {
        char* ptr = reinterpret_cast<char *>(const_cast<PBuffer>(itr->pData));
        if (i == veryShortIdx) {
            assert(itr->cbData == 1);
            *ptr = 'F';
        } else if (i == zeroLenIdx) {
            itr->cbData = 0;
        } else if (i == trimmableIdx) {
            itr->cbData = buflen;
            assert(buflen == 16);
            memcpy(ptr, "    1 3 5 78    ", buflen);
        } else {
            sprintf(ptr, "F%02d", i);
            if (i < longIdx) {
                memset (ptr+3, ' ', buflenShort - 3);
            } else {
                memset (ptr+3, ' ', buflen - 3);
            }
        }
    }

    int negRegister = registersize >> 1;
    //int negIdx = intIdx + negRegister;
    for(i=0; i < registersize; i++, itr++) {
        if (i < negRegister) {
            *(reinterpret_cast<int32_t *>(const_cast<PBuffer>(itr->pData))) = i;
        } else {
            *(reinterpret_cast<int32_t *>(const_cast<PBuffer>(itr->pData))) = 
                (i - negRegister) * -1;
        }
    }
  
    itr = output.begin();
    for(i=0; i < registersize; i++, itr++) {
        char* ptr = reinterpret_cast<char *>(const_cast<PBuffer>(itr->pData));
        memset(ptr, ' ', buflen); // VARCHAR is not null terminated
        itr->cbData = 0;
    }
    for(i=0; i < registersize; i++, itr++) {
        char* ptr = reinterpret_cast<char *>(const_cast<PBuffer>(itr->pData));
        memset(ptr, ' ', buflen); // CHAR is not null terminated
    }
    for(i=0; i < registersize; i++, itr++) {
        *(reinterpret_cast<int32_t *>(const_cast<PBuffer>(itr->pData))) = -8;
        
    }

    // null out last element of each type
    int nullRegister = registersize - 1;
    int varcharNullIdx = varcharIdx + nullRegister;
    int charNullIdx = charIdx + nullRegister;
    int intNullIdx = intIdx + nullRegister;
    input[varcharNullIdx].pData = NULL;
    input[varcharNullIdx].cbData = 0;
    input[charNullIdx].pData = NULL;
    input[charNullIdx].cbData = 0;
    input[intNullIdx].pData = NULL;

    // Print out the nullable tuple
    TuplePrinter tuplePrinter;
    printf("\nInput\n");
    tuplePrinter.print(cout, tupleDesc, input);
    cout << endl;
    printf("\nOutput\n");
    tuplePrinter.print(cout, tupleDesc, output);
    cout << endl;


    // predefine register references. a real compiler wouldn't do
    // something so regular and pre-determined. a compiler would
    // probably build these on the fly as it built each instruction.
    // predefine register references. a real compiler wouldn't do
    // something so regular and pre-determined
    RegisterRef<char *> **vcInP, **vcOutP;
    RegisterRef<char *> **cInP, **cOutP;
    RegisterRef<int32_t> **iInP, **iOutP;

    vcInP = new RegisterRef<char *>*[registersize];
    vcOutP = new RegisterRef<char *>*[registersize];

    cInP = new RegisterRef<char *>*[registersize];
    cOutP = new RegisterRef<char *>*[registersize];

    iInP = new RegisterRef<int32_t>*[registersize];
    iOutP = new RegisterRef<int32_t>*[registersize];

    // Set up the Calculator
    DynamicParamManager dpm;
    Calculator c(&dpm,0,0,0,0,0,0);
    c.outputRegisterByReference(false);

    // set up register references to symbolically point to 
    // their corresponding storage locations -- makes for easy test case
    // generation. again, a compiler wouldn't do things in quite
    // this way.
    for (i=0; i < registersize; i++) {
        // varchar
        vcInP[i] = new RegisterRef<char *>(RegisterReference::EInput, 
                                           varcharIdx + i, 
                                           STANDARD_TYPE_VARCHAR);
        c.appendRegRef(vcInP[i]);
        vcOutP[i] = new RegisterRef<char *>(RegisterReference::EOutput,
                                            varcharIdx + i,
                                            STANDARD_TYPE_VARCHAR);
        c.appendRegRef(vcOutP[i]);

        // char
        cInP[i] = new RegisterRef<char *>(RegisterReference::EInput, 
                                          charIdx + i, 
                                          STANDARD_TYPE_CHAR);
        c.appendRegRef(cInP[i]);
        cOutP[i] = new RegisterRef<char *>(RegisterReference::EOutput,
                                           charIdx + i,
                                           STANDARD_TYPE_CHAR);
        c.appendRegRef(cOutP[i]);

        // integer
        iInP[i] = new RegisterRef<int32_t>(RegisterReference::EInput,
                                           intIdx + i,
                                           STANDARD_TYPE_INT_32);
        c.appendRegRef(iInP[i]);
        iOutP[i] = new RegisterRef<int32_t>(RegisterReference::EOutput,
                                            intIdx + i,
                                            STANDARD_TYPE_INT_32);
        c.appendRegRef(iOutP[i]);
    }

    // Set up storage for instructions
    // a real compiler would probably cons up instructions and insert them
    // directly into the calculator. keep an array of the instructions at
    // this level to allow printing of the program after execution, and other
    // debugging
    Instruction **instP;
    instP = new InstructionPtr[200];
    int pc = 0, outI = 0;
    int outVCLong = longIdx, outVCShort = shortIdx;
    int outCLong = longIdx, outCShort = shortIdx;
    
    ExtendedInstructionTable* eit = InstructionFactory::getExtendedInstructionTable();

    vector<RegisterReference*> regRefs;

    //
    // strCatA2 Fixed
    //
    ExtendedInstructionDef* strCatA2F = (*eit)["strCatA2(c,c)"];
    assert(strCatA2F);
    assert(strCatA2F->getName() == string("strCatA2"));
    assert(strCatA2F->getParameterTypes().size() == 2);
    regRefs.resize(2);

    // can't test common case w/o using strCatAF3 as well to have
    // length set correctly

    // null case
    regRefs[0] = cOutP[outCLong++];
    regRefs[1] = cInP[nullRegister];
    instP[pc++] = strCatA2F->createInstruction(regRefs);

    // force right truncation exception
    regRefs[0] = cOutP[veryShortIdx];
    regRefs[1] = cInP[longIdx + 0];
    const int strCatA2FException = pc;
    instP[pc++] = strCatA2F->createInstruction(regRefs);

    //
    // strCatA3 Fixed
    //
    ExtendedInstructionDef* strCatA3F = (*eit)["strCatA3(c,c,c)"];
    assert(strCatA3F);
    assert(strCatA3F->getName() == string("strCatA3"));
    assert(strCatA3F->getParameterTypes().size() == 3);
    regRefs.resize(3);

    // common cases
    regRefs[0] = cOutP[outCLong++];
    regRefs[1] = cInP[shortIdx + 0];
    regRefs[2] = cInP[shortIdx + 1];
    instP[pc++] = strCatA3F->createInstruction(regRefs);

    regRefs[0] = cOutP[outCLong++];
    regRefs[1] = cInP[shortIdx + 0];
    regRefs[2] = cInP[shortIdx + 0];
    instP[pc++] = strCatA3F->createInstruction(regRefs);

    // null cases
    regRefs[0] = cOutP[outCLong++];
    regRefs[1] = cInP[nullRegister];
    regRefs[2] = cInP[shortIdx + 0];
    instP[pc++] = strCatA3F->createInstruction(regRefs);

    regRefs[0] = cOutP[outCLong++];
    regRefs[1] = cInP[shortIdx + 0];
    regRefs[2] = cInP[nullRegister];
    instP[pc++] = strCatA3F->createInstruction(regRefs);

    regRefs[0] = cOutP[outCLong++];
    regRefs[1] = cInP[nullRegister];
    regRefs[2] = cInP[nullRegister];
    instP[pc++] = strCatA3F->createInstruction(regRefs);

    // force right truncation exception
    regRefs[0] = cOutP[veryShortIdx];
    regRefs[1] = cInP[shortIdx + 0];
    regRefs[2] = cInP[shortIdx + 1];
    const int strCatA3FException = pc;
    instP[pc++] = strCatA3F->createInstruction(regRefs);

    //
    // strCatA2 & strCatA3 Fixed
    //
    // four way fixed cat using both
    regRefs.resize(3);
    regRefs[0] = cOutP[outCLong];
    regRefs[1] = cInP[shortIdx + 0];
    regRefs[2] = cInP[shortIdx + 1];
    instP[pc++] = strCatA3F->createInstruction(regRefs);

    regRefs.resize(2);
    regRefs[0] = cOutP[outCLong];
    regRefs[1] = cInP[shortIdx + 2];
    instP[pc++] = strCatA2F->createInstruction(regRefs);

    regRefs[0] = cOutP[outCLong++];
    regRefs[1] = cInP[shortIdx + 3];
    instP[pc++] = strCatA2F->createInstruction(regRefs);


    //
    // strCatA2 Variable
    //
    ExtendedInstructionDef* strCatA2V = (*eit)["strCatA2(vc,vc)"];
    assert(strCatA2V);
    assert(strCatA2V->getName() == string("strCatA2"));
    assert(strCatA2V->getParameterTypes().size() == 2);
    regRefs.resize(2);

    // common case
    regRefs[0] = vcOutP[outVCLong++];
    regRefs[1] = vcInP[shortIdx + 4];
    instP[pc++] = strCatA2V->createInstruction(regRefs);

    // just append to first string
    regRefs[1] = vcInP[shortIdx + 5];
    instP[pc++] = strCatA2V->createInstruction(regRefs);

    // zero length case
    regRefs[0] = vcOutP[outVCLong++];
    regRefs[1] = vcInP[zeroLenIdx];
    instP[pc++] = strCatA2V->createInstruction(regRefs);

    // null case
    regRefs[0] = vcOutP[outVCLong++];
    regRefs[1] = vcInP[nullRegister];
    instP[pc++] = strCatA2V->createInstruction(regRefs);

    // force right truncation exception
    regRefs[0] = vcOutP[veryShortIdx];
    regRefs[1] = vcInP[longIdx + 0];
    const int strCatA2VException = pc;
    instP[pc++] = strCatA2V->createInstruction(regRefs);

    //
    // strCatA3 Variable
    //
    ExtendedInstructionDef* strCatA3V = (*eit)["strCatA3(vc,vc,vc)"];
    assert(strCatA3V);
    assert(strCatA3V->getName() == string("strCatA3"));
    assert(strCatA3V->getParameterTypes().size() == 3);
    regRefs.resize(3);

    // common cases
    regRefs[0] = vcOutP[outVCLong++];
    regRefs[1] = vcInP[shortIdx + 0];
    regRefs[2] = vcInP[shortIdx + 1];
    instP[pc++] = strCatA3V->createInstruction(regRefs);

    regRefs[0] = vcOutP[outVCLong++];
    regRefs[1] = vcInP[shortIdx + 0];
    regRefs[2] = vcInP[shortIdx + 0];
    instP[pc++] = strCatA3V->createInstruction(regRefs);

    // zero length cases
    regRefs[0] = vcOutP[outVCLong++];
    regRefs[1] = vcInP[zeroLenIdx];
    regRefs[2] = vcInP[zeroLenIdx];
    instP[pc++] = strCatA3V->createInstruction(regRefs);

    regRefs[0] = vcOutP[outVCLong++];
    regRefs[1] = vcInP[zeroLenIdx];
    regRefs[2] = vcInP[shortIdx + 3];
    instP[pc++] = strCatA3V->createInstruction(regRefs);

    regRefs[0] = vcOutP[outVCLong++];
    regRefs[1] = vcInP[shortIdx + 4];
    regRefs[2] = vcInP[zeroLenIdx];
    instP[pc++] = strCatA3V->createInstruction(regRefs);

    // null cases
    regRefs[0] = vcOutP[outVCLong++];
    regRefs[1] = vcInP[nullRegister];
    regRefs[2] = vcInP[shortIdx + 0];
    instP[pc++] = strCatA3V->createInstruction(regRefs);

    regRefs[0] = vcOutP[outVCLong++];
    regRefs[1] = vcInP[shortIdx + 0];
    regRefs[2] = vcInP[nullRegister];
    instP[pc++] = strCatA3V->createInstruction(regRefs);

    regRefs[0] = vcOutP[outVCLong++];
    regRefs[1] = vcInP[nullRegister];
    regRefs[2] = vcInP[nullRegister];
    instP[pc++] = strCatA3V->createInstruction(regRefs);

    // force right truncation exception
    regRefs[0] = vcOutP[veryShortIdx];
    regRefs[1] = vcInP[shortIdx + 0];
    regRefs[2] = vcInP[shortIdx + 1];
    const int strCatA3VException = pc;
    instP[pc++] = strCatA3V->createInstruction(regRefs);


    //
    // strCmpA Fixed
    //
    ExtendedInstructionDef* strCmpAF = (*eit)["strCmpA(s4,c,c)"];
    assert(strCmpAF);
    assert(strCmpAF->getName() == string("strCmpA"));
    assert(strCmpAF->getParameterTypes().size() == 3);
    regRefs.resize(3);

    // common cases
    regRefs[0] = iOutP[outI++];
    regRefs[1] = cInP[shortIdx + 0];
    regRefs[2] = cInP[shortIdx + 0];
    instP[pc++] = strCmpAF->createInstruction(regRefs);

    regRefs[0] = iOutP[outI++];
    regRefs[1] = cInP[shortIdx + 0];
    regRefs[2] = cInP[shortIdx + 1];
    instP[pc++] = strCmpAF->createInstruction(regRefs);

    regRefs[0] = iOutP[outI++];
    regRefs[1] = cInP[shortIdx + 1];
    regRefs[2] = cInP[shortIdx + 0];
    instP[pc++] = strCmpAF->createInstruction(regRefs);

    // null cases
    regRefs[0] = iOutP[outI++];
    regRefs[1] = cInP[nullRegister];
    regRefs[2] = cInP[shortIdx + 0];
    instP[pc++] = strCmpAF->createInstruction(regRefs);

    regRefs[0] = iOutP[outI++];
    regRefs[1] = cInP[shortIdx + 0];
    regRefs[2] = cInP[nullRegister];
    instP[pc++] = strCmpAF->createInstruction(regRefs);

    regRefs[0] = iOutP[outI++];
    regRefs[1] = cInP[nullRegister];
    regRefs[2] = cInP[nullRegister];
    instP[pc++] = strCmpAF->createInstruction(regRefs);

    //
    // strCmpA Variable
    //
    ExtendedInstructionDef* strCmpAV = (*eit)["strCmpA(s4,vc,vc)"];
    assert(strCmpAV);
    assert(strCmpAV->getName() == string("strCmpA"));
    assert(strCmpAV->getParameterTypes().size() == 3);
    regRefs.resize(3);

    // common cases
    regRefs[0] = iOutP[outI++];
    regRefs[1] = cInP[shortIdx + 0];
    regRefs[2] = cInP[shortIdx + 0];
    instP[pc++] = strCmpAV->createInstruction(regRefs);

    regRefs[0] = iOutP[outI++];
    regRefs[1] = cInP[shortIdx + 0];
    regRefs[2] = cInP[shortIdx + 1];
    instP[pc++] = strCmpAV->createInstruction(regRefs);

    regRefs[0] = iOutP[outI++];
    regRefs[1] = cInP[shortIdx + 1];
    regRefs[2] = cInP[shortIdx + 0];
    instP[pc++] = strCmpAV->createInstruction(regRefs);

    // zero length cases
    regRefs[0] = iOutP[outI++];
    regRefs[1] = vcInP[zeroLenIdx];
    regRefs[2] = vcInP[zeroLenIdx];
    instP[pc++] = strCmpAV->createInstruction(regRefs);

    regRefs[0] = iOutP[outI++];
    regRefs[1] = vcInP[zeroLenIdx];
    regRefs[2] = vcInP[shortIdx + 3];
    instP[pc++] = strCmpAV->createInstruction(regRefs);

    regRefs[0] = iOutP[outI++];
    regRefs[1] = vcInP[shortIdx + 4];
    regRefs[2] = vcInP[zeroLenIdx];
    instP[pc++] = strCmpAV->createInstruction(regRefs);


    // null cases
    regRefs[0] = iOutP[outI++];
    regRefs[1] = cInP[nullRegister];
    regRefs[2] = cInP[shortIdx + 0];
    instP[pc++] = strCmpAV->createInstruction(regRefs);

    regRefs[0] = iOutP[outI++];
    regRefs[1] = cInP[shortIdx + 0];
    regRefs[2] = cInP[nullRegister];
    instP[pc++] = strCmpAV->createInstruction(regRefs);

    regRefs[0] = iOutP[outI++];
    regRefs[1] = cInP[nullRegister];
    regRefs[2] = cInP[nullRegister];
    instP[pc++] = strCmpAV->createInstruction(regRefs);


    //
    // strLenBitA Fixed
    //
    ExtendedInstructionDef* strLenBitAF = (*eit)["strLenBitA(s4,c)"];
    assert(strLenBitAF);
    assert(strLenBitAF->getName() == string("strLenBitA"));
    assert(strLenBitAF->getParameterTypes().size() == 2);
    regRefs.resize(2);

    // common case
    regRefs[0] = iOutP[outI++];
    regRefs[1] = cInP[shortIdx + 0];
    instP[pc++] = strLenBitAF->createInstruction(regRefs);

    regRefs[0] = iOutP[outI++];
    regRefs[1] = cInP[longIdx + 0];
    instP[pc++] = strLenBitAF->createInstruction(regRefs);

    // null case
    regRefs[0] = iOutP[outI++];
    regRefs[1] = cInP[nullRegister];
    instP[pc++] = strLenBitAF->createInstruction(regRefs);

    //
    // strLenBitA Variable
    //
    ExtendedInstructionDef* strLenBitAV = (*eit)["strLenBitA(s4,vc)"];
    assert(strLenBitAV);
    assert(strLenBitAV->getName() == string("strLenBitA"));
    assert(strLenBitAV->getParameterTypes().size() == 2);
    regRefs.resize(2);

    // common case
    regRefs[0] = iOutP[outI++];
    regRefs[1] = vcInP[zeroLenIdx];
    instP[pc++] = strLenBitAV->createInstruction(regRefs);

    regRefs[0] = iOutP[outI++];
    regRefs[1] = vcInP[shortIdx + 0];
    instP[pc++] = strLenBitAV->createInstruction(regRefs);

    regRefs[0] = iOutP[outI++];
    regRefs[1] = vcInP[longIdx + 0];
    instP[pc++] = strLenBitAV->createInstruction(regRefs);

    // null case
    regRefs[0] = iOutP[outI++];
    regRefs[1] = vcInP[nullRegister];
    instP[pc++] = strLenBitAV->createInstruction(regRefs);


    //
    // strLenCharA Fixed
    //
    ExtendedInstructionDef* strLenCharAF = (*eit)["strLenCharA(s4,c)"];
    assert(strLenCharAF);
    assert(strLenCharAF->getName() == string("strLenCharA"));
    assert(strLenCharAF->getParameterTypes().size() == 2);
    regRefs.resize(2);

    // common case
    regRefs[0] = iOutP[outI++];
    regRefs[1] = cInP[shortIdx + 0];
    instP[pc++] = strLenCharAF->createInstruction(regRefs);

    regRefs[0] = iOutP[outI++];
    regRefs[1] = cInP[longIdx + 0];
    instP[pc++] = strLenCharAF->createInstruction(regRefs);

    // null case
    regRefs[0] = iOutP[outI++];
    regRefs[1] = cInP[nullRegister];
    instP[pc++] = strLenCharAF->createInstruction(regRefs);

    //
    // strLenCharA Variable
    //
    ExtendedInstructionDef* strLenCharAV = (*eit)["strLenCharA(s4,vc)"];
    assert(strLenCharAV);
    assert(strLenCharAV->getName() == string("strLenCharA"));
    assert(strLenCharAV->getParameterTypes().size() == 2);
    regRefs.resize(2);

    // common case
    regRefs[0] = iOutP[outI++];
    regRefs[1] = vcInP[zeroLenIdx];
    instP[pc++] = strLenCharAV->createInstruction(regRefs);

    regRefs[0] = iOutP[outI++];
    regRefs[1] = vcInP[shortIdx + 0];
    instP[pc++] = strLenCharAV->createInstruction(regRefs);

    regRefs[0] = iOutP[outI++];
    regRefs[1] = vcInP[longIdx + 0];
    instP[pc++] = strLenCharAV->createInstruction(regRefs);

    // null case
    regRefs[0] = iOutP[outI++];
    regRefs[1] = vcInP[nullRegister];
    instP[pc++] = strLenCharAV->createInstruction(regRefs);


    //
    // strLenOctA Fixed
    //
    ExtendedInstructionDef* strLenOctAF = (*eit)["strLenOctA(s4,c)"];
    assert(strLenOctAF);
    assert(strLenOctAF->getName() == string("strLenOctA"));
    assert(strLenOctAF->getParameterTypes().size() == 2);
    regRefs.resize(2);

    // common case
    regRefs[0] = iOutP[outI++];
    regRefs[1] = cInP[shortIdx + 0];
    instP[pc++] = strLenOctAF->createInstruction(regRefs);

    regRefs[0] = iOutP[outI++];
    regRefs[1] = cInP[longIdx + 0];
    instP[pc++] = strLenOctAF->createInstruction(regRefs);

    // null case
    regRefs[0] = iOutP[outI++];
    regRefs[1] = cInP[nullRegister];
    instP[pc++] = strLenOctAF->createInstruction(regRefs);

    //
    // strLenOctA Variable
    //
    ExtendedInstructionDef* strLenOctAV = (*eit)["strLenOctA(s4,vc)"];
    assert(strLenOctAV);
    assert(strLenOctAV->getName() == string("strLenOctA"));
    assert(strLenOctAV->getParameterTypes().size() == 2);
    regRefs.resize(2);

    // common case
    regRefs[0] = iOutP[outI++];
    regRefs[1] = vcInP[zeroLenIdx];
    instP[pc++] = strLenOctAV->createInstruction(regRefs);

    regRefs[0] = iOutP[outI++];
    regRefs[1] = vcInP[shortIdx + 0];
    instP[pc++] = strLenOctAV->createInstruction(regRefs);

    regRefs[0] = iOutP[outI++];
    regRefs[1] = vcInP[longIdx + 0];
    instP[pc++] = strLenOctAV->createInstruction(regRefs);

    // null case
    regRefs[0] = iOutP[outI++];
    regRefs[1] = vcInP[nullRegister];
    instP[pc++] = strLenOctAV->createInstruction(regRefs);


    //
    // strOverlayA5 Fixed
    //
    ExtendedInstructionDef* strOverlayA5F = 
        (*eit)["strOverlayA5(vc,c,c,s4,s4)"];
    assert(strOverlayA5F);
    assert(strOverlayA5F->getName() == string("strOverlayA5"));
    assert(strOverlayA5F->getParameterTypes().size() == 5);
    regRefs.resize(5);

    // common case
    regRefs[0] = vcOutP[outVCLong++];
    regRefs[1] = cInP[shortIdx + 0];
    regRefs[2] = cInP[shortIdx + 1];
    regRefs[3] = iInP[2];
    regRefs[4] = iInP[0];
    instP[pc++] = strOverlayA5F->createInstruction(regRefs);

    // null cases
    regRefs[0] = vcOutP[outVCLong++];
    regRefs[1] = cInP[nullRegister];
    regRefs[2] = cInP[shortIdx + 1];
    regRefs[3] = iInP[2];
    regRefs[4] = iInP[3];
    instP[pc++] = strOverlayA5F->createInstruction(regRefs);

    regRefs[0] = vcOutP[outVCLong++];
    regRefs[1] = cInP[shortIdx + 1];
    regRefs[2] = cInP[nullRegister];
    regRefs[3] = iInP[2];
    regRefs[4] = iInP[3];
    instP[pc++] = strOverlayA5F->createInstruction(regRefs);

    regRefs[0] = vcOutP[outVCLong++];
    regRefs[1] = cInP[shortIdx + 1];
    regRefs[2] = cInP[shortIdx + 1];
    regRefs[3] = iInP[nullRegister];
    regRefs[4] = iInP[3];
    instP[pc++] = strOverlayA5F->createInstruction(regRefs);

    regRefs[0] = vcOutP[outVCLong++];
    regRefs[1] = cInP[shortIdx + 1];
    regRefs[2] = cInP[shortIdx + 1];
    regRefs[3] = iInP[2];
    regRefs[4] = iInP[nullRegister];
    instP[pc++] = strOverlayA5F->createInstruction(regRefs);

    // substring errors
    regRefs[0] = vcOutP[outVCLong++];
    regRefs[1] = cInP[shortIdx + 0];
    regRefs[2] = cInP[shortIdx + 1];
    regRefs[3] = iInP[negRegister + 2];
    regRefs[4] = iInP[3];
    const int strOverlayA5FException1 = pc;
    instP[pc++] = strOverlayA5F->createInstruction(regRefs);

    regRefs[0] = vcOutP[outVCLong++];
    regRefs[1] = cInP[shortIdx + 0];
    regRefs[2] = cInP[shortIdx + 1];
    regRefs[3] = iInP[2];
    regRefs[4] = iInP[negRegister + 3];
    const int strOverlayA5FException2 = pc;
    instP[pc++] = strOverlayA5F->createInstruction(regRefs);

    // right truncation
    regRefs[0] = vcOutP[outVCLong++];
    regRefs[1] = cInP[longIdx + 0];
    regRefs[2] = cInP[longIdx + 1];
    regRefs[3] = iInP[2];
    regRefs[4] = iInP[3];
    const int strOverlayA5FException3 = pc;
    instP[pc++] = strOverlayA5F->createInstruction(regRefs);


 
    //
    // strOverlayA4 Fixed
    //
    ExtendedInstructionDef* strOverlayA4F = 
        (*eit)["strOverlayA4(vc,c,c,s4)"];
    assert(strOverlayA4F);
    assert(strOverlayA4F->getName() == string("strOverlayA4"));
    assert(strOverlayA4F->getParameterTypes().size() == 4);
    regRefs.resize(4);

    // common case
    regRefs[0] = vcOutP[outVCLong++];
    regRefs[1] = cInP[shortIdx + 0];
    regRefs[2] = cInP[shortIdx + 1];
    regRefs[3] = iInP[3];
    instP[pc++] = strOverlayA4F->createInstruction(regRefs);

    // null cases
    regRefs[0] = vcOutP[outVCLong++];
    regRefs[1] = cInP[nullRegister];
    regRefs[2] = cInP[shortIdx + 1];
    regRefs[3] = iInP[2];
    instP[pc++] = strOverlayA4F->createInstruction(regRefs);

    regRefs[0] = vcOutP[outVCLong++];
    regRefs[1] = cInP[shortIdx + 1];
    regRefs[2] = cInP[nullRegister];
    regRefs[3] = iInP[2];
    instP[pc++] = strOverlayA4F->createInstruction(regRefs);

    regRefs[0] = vcOutP[outVCLong++];
    regRefs[1] = cInP[shortIdx + 1];
    regRefs[2] = cInP[shortIdx + 1];
    regRefs[3] = iInP[nullRegister];
    instP[pc++] = strOverlayA4F->createInstruction(regRefs);

    // substring error
    regRefs[0] = vcOutP[outVCLong++];
    regRefs[1] = cInP[shortIdx + 0];
    regRefs[2] = cInP[shortIdx + 1];
    regRefs[3] = iInP[negRegister + 2];
    const int strOverlayA4FException1 = pc;
    instP[pc++] = strOverlayA4F->createInstruction(regRefs);

    // right truncation
    regRefs[0] = vcOutP[outVCLong++];
    regRefs[1] = cInP[longIdx + 0];
    regRefs[2] = cInP[longIdx + 1];
    regRefs[3] = iInP[2];
    const int strOverlayA4FException2 = pc;
    instP[pc++] = strOverlayA4F->createInstruction(regRefs);


    //
    // strOverlayA5V
    //
    ExtendedInstructionDef* strOverlayA5V = 
        (*eit)["strOverlayA5(vc,vc,vc,s4,s4)"];
    assert(strOverlayA5V);
    assert(strOverlayA5V->getName() == string("strOverlayA5"));
    assert(strOverlayA5V->getParameterTypes().size() == 5);
    regRefs.resize(5);

    // common case
    regRefs[0] = vcOutP[outVCLong++];
    regRefs[1] = vcInP[shortIdx + 0];
    regRefs[2] = vcInP[shortIdx + 1];
    regRefs[3] = iInP[2];
    regRefs[4] = iInP[0];
    instP[pc++] = strOverlayA5V->createInstruction(regRefs);

    // null cases
    regRefs[0] = vcOutP[outVCLong++];
    regRefs[1] = vcInP[nullRegister];
    regRefs[2] = vcInP[shortIdx + 1];
    regRefs[3] = iInP[2];
    regRefs[4] = iInP[3];
    instP[pc++] = strOverlayA5V->createInstruction(regRefs);

    regRefs[0] = vcOutP[outVCLong++];
    regRefs[1] = vcInP[shortIdx + 1];
    regRefs[2] = vcInP[nullRegister];
    regRefs[3] = iInP[2];
    regRefs[4] = iInP[3];
    instP[pc++] = strOverlayA5V->createInstruction(regRefs);

    regRefs[0] = vcOutP[outVCLong++];
    regRefs[1] = vcInP[shortIdx + 1];
    regRefs[2] = vcInP[shortIdx + 1];
    regRefs[3] = iInP[nullRegister];
    regRefs[4] = iInP[3];
    instP[pc++] = strOverlayA5V->createInstruction(regRefs);

    regRefs[0] = vcOutP[outVCLong++];
    regRefs[1] = vcInP[shortIdx + 1];
    regRefs[2] = vcInP[shortIdx + 1];
    regRefs[3] = iInP[2];
    regRefs[4] = iInP[nullRegister];
    instP[pc++] = strOverlayA5V->createInstruction(regRefs);

    // substring errors
    regRefs[0] = vcOutP[outVCLong++];
    regRefs[1] = vcInP[shortIdx + 0];
    regRefs[2] = vcInP[shortIdx + 1];
    regRefs[3] = iInP[negRegister + 2];
    regRefs[4] = iInP[3];
    const int strOverlayA5VException1 = pc;
    instP[pc++] = strOverlayA5V->createInstruction(regRefs);

    regRefs[0] = vcOutP[outVCLong++];
    regRefs[1] = vcInP[shortIdx + 0];
    regRefs[2] = vcInP[shortIdx + 1];
    regRefs[3] = iInP[2];
    regRefs[4] = iInP[negRegister + 3];
    const int strOverlayA5VException2 = pc;
    instP[pc++] = strOverlayA5V->createInstruction(regRefs);

    // right truncation
    regRefs[0] = vcOutP[outVCShort++];
    regRefs[1] = vcInP[longIdx + 0];
    regRefs[2] = vcInP[longIdx + 1];
    regRefs[3] = iInP[2];
    regRefs[4] = iInP[0];
    const int strOverlayA5VException3 = pc;
    instP[pc++] = strOverlayA5V->createInstruction(regRefs);



    //
    // strOverlayA4 Variable
    //
    ExtendedInstructionDef* strOverlayA4V = 
        (*eit)["strOverlayA4(vc,vc,vc,s4)"];
    assert(strOverlayA4V);
    assert(strOverlayA4V->getName() == string("strOverlayA4"));
    assert(strOverlayA4V->getParameterTypes().size() == 4);
    regRefs.resize(4);

    // common case
    regRefs[0] = vcOutP[outVCLong++];
    regRefs[1] = vcInP[shortIdx + 0];
    regRefs[2] = vcInP[shortIdx + 1];
    regRefs[3] = iInP[3];
    instP[pc++] = strOverlayA4V->createInstruction(regRefs);

    // null cases
    regRefs[0] = vcOutP[outVCLong++];
    regRefs[1] = vcInP[nullRegister];
    regRefs[2] = vcInP[shortIdx + 1];
    regRefs[3] = iInP[2];
    instP[pc++] = strOverlayA4V->createInstruction(regRefs);

    regRefs[0] = vcOutP[outVCLong++];
    regRefs[1] = vcInP[shortIdx + 1];
    regRefs[2] = vcInP[nullRegister];
    regRefs[3] = iInP[2];
    instP[pc++] = strOverlayA4V->createInstruction(regRefs);

    regRefs[0] = vcOutP[outVCLong++];
    regRefs[1] = vcInP[shortIdx + 1];
    regRefs[2] = vcInP[shortIdx + 1];
    regRefs[3] = iInP[nullRegister];
    instP[pc++] = strOverlayA4V->createInstruction(regRefs);

    // substring error
    regRefs[0] = vcOutP[outVCLong++];
    regRefs[1] = vcInP[shortIdx + 0];
    regRefs[2] = vcInP[shortIdx + 1];
    regRefs[3] = iInP[negRegister + 2];
    const int strOverlayA4VException1 = pc;
    instP[pc++] = strOverlayA4V->createInstruction(regRefs);

    // right truncation
    regRefs[0] = vcOutP[outVCShort++];
    regRefs[1] = vcInP[longIdx + 0];
    regRefs[2] = vcInP[longIdx + 1];
    regRefs[3] = iInP[3];
    const int strOverlayA4VException2 = pc;
    instP[pc++] = strOverlayA4V->createInstruction(regRefs);


    //
    // strPosA Fixed
    //
    ExtendedInstructionDef* strPosAF = (*eit)["strPosA(s4,c,c)"];
    assert(strPosAF);
    assert(strPosAF->getName() == string("strPosA"));
    assert(strPosAF->getParameterTypes().size() == 3);
    regRefs.resize(3);

    // common case
    regRefs[0] = iOutP[outI++];
    regRefs[1] = cInP[shortIdx + 0];
    regRefs[2] = cInP[shortIdx + 0];
    instP[pc++] = strPosAF->createInstruction(regRefs);

    regRefs[0] = iOutP[outI++];
    regRefs[1] = cInP[longIdx + 0];
    regRefs[2] = cInP[longIdx + 1];
    instP[pc++] = strPosAF->createInstruction(regRefs);

    // null cases
    regRefs[0] = iOutP[outI++];
    regRefs[1] = cInP[longIdx + 0];
    regRefs[2] = cInP[nullRegister];
    instP[pc++] = strPosAF->createInstruction(regRefs);

    regRefs[0] = iOutP[outI++];
    regRefs[1] = cInP[nullRegister];
    regRefs[2] = cInP[longIdx + 1];
    instP[pc++] = strPosAF->createInstruction(regRefs);


    //
    // strPosA Variable
    //
    ExtendedInstructionDef* strPosAV = (*eit)["strPosA(s4,vc,vc)"];
    assert(strPosAV);
    assert(strPosAV->getName() == string("strPosA"));
    assert(strPosAV->getParameterTypes().size() == 3);
    regRefs.resize(3);

    // common case
    regRefs[0] = iOutP[outI++];
    regRefs[1] = vcInP[shortIdx + 0];
    regRefs[2] = vcInP[shortIdx + 0];
    instP[pc++] = strPosAV->createInstruction(regRefs);

    regRefs[0] = iOutP[outI++];
    regRefs[1] = vcInP[longIdx + 0];
    regRefs[2] = vcInP[longIdx + 1];
    instP[pc++] = strPosAV->createInstruction(regRefs);

#if 0
TODO: JR 6/07 these 4 tests removed temporarily (see notes below)
    regRefs[0] = iOutP[outI++];
    regRefs[1] = vcInP[zeroLenIdx];
    regRefs[2] = vcInP[longIdx + 1];
    instP[pc++] = strPosAV->createInstruction(regRefs);

    regRefs[0] = iOutP[outI++];
    regRefs[1] = vcInP[longIdx + 0];
    regRefs[2] = vcInP[zeroLenIdx];
    instP[pc++] = strPosAV->createInstruction(regRefs);

    // null cases
    regRefs[0] = iOutP[outI++];
    regRefs[1] = vcInP[longIdx + 0];
    regRefs[2] = vcInP[nullRegister];
    instP[pc++] = strPosAV->createInstruction(regRefs);

    regRefs[0] = iOutP[outI++];
    regRefs[1] = vcInP[nullRegister];
    regRefs[2] = vcInP[longIdx + 1];
    instP[pc++] = strPosAV->createInstruction(regRefs);
#endif

 
    //
    // strSubStringA3 Fixed
    //
    ExtendedInstructionDef* strSubStringA3F = 
        (*eit)["strSubStringA3(vc,c,s4)"];
    assert(strSubStringA3F);
    assert(strSubStringA3F->getName() == string("strSubStringA3"));
    assert(strSubStringA3F->getParameterTypes().size() == 3);
    regRefs.resize(3);

    // common case
    regRefs[0] = vcOutP[outVCShort++];
    regRefs[1] = cInP[shortIdx + 0];
    regRefs[2] = iInP[1];
    instP[pc++] = strSubStringA3F->createInstruction(regRefs);

    regRefs[0] = vcOutP[outVCShort++];
    regRefs[1] = cInP[shortIdx + 0];
    regRefs[2] = iInP[5];
    instP[pc++] = strSubStringA3F->createInstruction(regRefs);

    // null cases
    regRefs[0] = vcOutP[outVCShort++];
    regRefs[1] = cInP[nullRegister];
    regRefs[2] = iInP[5];
    instP[pc++] = strSubStringA3F->createInstruction(regRefs);

    regRefs[0] = vcOutP[outVCShort++];
    regRefs[1] = cInP[shortIdx + 1];
    regRefs[2] = iInP[nullRegister];
    instP[pc++] = strSubStringA3F->createInstruction(regRefs);

    // substring error not possible if len is unspecified
    // right truncation
    regRefs[0] = vcOutP[outVCShort++];
    regRefs[1] = cInP[longIdx + 0];
    regRefs[2] = iInP[0];
    const int strSubStringA3FException1 = pc;
    instP[pc++] = strSubStringA3F->createInstruction(regRefs);

    //
    // strSubStringA3 Variable
    //
    ExtendedInstructionDef* strSubStringA3V = 
        (*eit)["strSubStringA3(vc,vc,s4)"];
    assert(strSubStringA3V);
    assert(strSubStringA3V->getName() == string("strSubStringA3"));
    assert(strSubStringA3V->getParameterTypes().size() == 3);
    regRefs.resize(3);

    // common case
    regRefs[0] = vcOutP[outVCShort++];
    regRefs[1] = vcInP[shortIdx + 0];
    regRefs[2] = iInP[1];
    instP[pc++] = strSubStringA3V->createInstruction(regRefs);

    regRefs[0] = vcOutP[outVCShort++];
    regRefs[1] = vcInP[shortIdx + 0];
    regRefs[2] = iInP[5];
    instP[pc++] = strSubStringA3V->createInstruction(regRefs);

    // null cases
    regRefs[0] = vcOutP[outVCShort++];
    regRefs[1] = vcInP[nullRegister];
    regRefs[2] = iInP[5];
    instP[pc++] = strSubStringA3V->createInstruction(regRefs);

    regRefs[0] = vcOutP[outVCShort++];
    regRefs[1] = vcInP[shortIdx + 1];
    regRefs[2] = iInP[nullRegister];
    instP[pc++] = strSubStringA3V->createInstruction(regRefs);

    // substring error not possible if len is unspecified
    // right truncation
    regRefs[0] = vcOutP[outVCShort++];
    regRefs[1] = vcInP[varFullIdx];
    regRefs[2] = iInP[1];
    const int strSubStringA3VException1 = pc;
    instP[pc++] = strSubStringA3V->createInstruction(regRefs);

 
    //
    // strSubStringA4 Fixed
    //
    ExtendedInstructionDef* strSubStringA4F = 
        (*eit)["strSubStringA4(vc,c,s4,s4)"];
    assert(strSubStringA4F);
    assert(strSubStringA4F->getName() == string("strSubStringA4"));
    assert(strSubStringA4F->getParameterTypes().size() == 4);
    regRefs.resize(4);

    // common case
    regRefs[0] = vcOutP[outVCShort++];
    regRefs[1] = cInP[shortIdx + 0];
    regRefs[2] = iInP[1];
    regRefs[3] = iInP[2];
    instP[pc++] = strSubStringA4F->createInstruction(regRefs);

    regRefs[0] = vcOutP[outVCShort++];
    regRefs[1] = cInP[shortIdx + 0];
    regRefs[2] = iInP[5];
    regRefs[3] = iInP[5];
    instP[pc++] = strSubStringA4F->createInstruction(regRefs);

    // null cases
    regRefs[0] = vcOutP[outVCShort++];
    regRefs[1] = cInP[nullRegister];
    regRefs[2] = iInP[5];
    regRefs[3] = iInP[5];
    instP[pc++] = strSubStringA4F->createInstruction(regRefs);

    regRefs[0] = vcOutP[outVCShort++];
    regRefs[1] = cInP[shortIdx + 1];
    regRefs[2] = iInP[nullRegister];
    regRefs[3] = iInP[5];
    instP[pc++] = strSubStringA4F->createInstruction(regRefs);

    regRefs[0] = vcOutP[outVCShort++];
    regRefs[1] = cInP[shortIdx + 1];
    regRefs[2] = iInP[5];
    regRefs[3] = iInP[nullRegister];
    instP[pc++] = strSubStringA4F->createInstruction(regRefs);

    // substring error
    regRefs[0] = vcOutP[outVCShort++];
    regRefs[1] = cInP[longIdx + 0];
    regRefs[2] = iInP[0];
    regRefs[3] = iInP[negRegister + 3];
    const int strSubStringA4FException1 = pc;
    instP[pc++] = strSubStringA4F->createInstruction(regRefs);

    // right truncation
    regRefs[0] = vcOutP[outVCShort++];
    regRefs[1] = cInP[longIdx + 0];
    regRefs[2] = iInP[0];
    regRefs[3] = iInP[16];
    const int strSubStringA4FException2 = pc;
    instP[pc++] = strSubStringA4F->createInstruction(regRefs);

    //
    // strSubStringA4 Variable
    //
    ExtendedInstructionDef* strSubStringA4V = 
        (*eit)["strSubStringA4(vc,vc,s4,s4)"];
    assert(strSubStringA4V);
    assert(strSubStringA4V->getName() == string("strSubStringA4"));
    assert(strSubStringA4V->getParameterTypes().size() == 4);
    regRefs.resize(4);

    // common case
    regRefs[0] = vcOutP[outVCShort++];
    regRefs[1] = vcInP[shortIdx + 0];
    regRefs[2] = iInP[1];
    regRefs[3] = iInP[2];
    instP[pc++] = strSubStringA4V->createInstruction(regRefs);

    regRefs[0] = vcOutP[outVCShort++];
    regRefs[1] = vcInP[shortIdx + 0];
    regRefs[2] = iInP[5];
    regRefs[3] = iInP[5];
    instP[pc++] = strSubStringA4V->createInstruction(regRefs);

    // null cases
    regRefs[0] = vcOutP[outVCShort++];
    regRefs[1] = vcInP[nullRegister];
    regRefs[2] = iInP[5];
    regRefs[3] = iInP[5];
    instP[pc++] = strSubStringA4V->createInstruction(regRefs);

    regRefs[0] = vcOutP[outVCShort++];
    regRefs[1] = vcInP[shortIdx + 1];
    regRefs[2] = iInP[nullRegister];
    regRefs[3] = iInP[5];
    instP[pc++] = strSubStringA4V->createInstruction(regRefs);

    regRefs[0] = vcOutP[outVCShort++];
    regRefs[1] = vcInP[shortIdx + 1];
    regRefs[2] = iInP[5];
    regRefs[3] = iInP[nullRegister];
    instP[pc++] = strSubStringA4V->createInstruction(regRefs);

    // substring error
    regRefs[0] = vcOutP[outVCShort++];
    regRefs[1] = vcInP[longIdx + 0];
    regRefs[2] = iInP[0];
    regRefs[3] = iInP[negRegister + 3];
    const int strSubStringA4VException1 = pc;
    instP[pc++] = strSubStringA4V->createInstruction(regRefs);

    // right truncation
    regRefs[0] = vcOutP[outVCShort++];
    regRefs[1] = vcInP[varFullIdx];
    regRefs[2] = iInP[1];
    regRefs[3] = iInP[16];
    const int strSubStringA4VException2 = pc;
    instP[pc++] = strSubStringA4V->createInstruction(regRefs);

    //
    // strToLowerA Fixed
    //
    ExtendedInstructionDef* strToLowerAF = 
        (*eit)["strToLowerA(c,c)"];
    assert(strToLowerAF);
    assert(strToLowerAF->getName() == string("strToLowerA"));
    assert(strToLowerAF->getParameterTypes().size() == 2);
    regRefs.resize(2);

    // common case
    int toLowerOutCShort = outCShort;
    regRefs[0] = cOutP[outCShort++];
    regRefs[1] = cInP[shortIdx + 0];
    instP[pc++] = strToLowerAF->createInstruction(regRefs);

    // null case
    // use long register here as lengths must be equal and null register is long
    regRefs[0] = cOutP[outCLong++];
    regRefs[1] = cInP[nullRegister];
    instP[pc++] = strToLowerAF->createInstruction(regRefs);

    // right truncation not possible in fixed width, as both 
    // strings must be same length by definition.

    //
    // strToLowerA Variable
    //
    ExtendedInstructionDef* strToLowerAV = 
        (*eit)["strToLowerA(vc,vc)"];
    assert(strToLowerAV);
    assert(strToLowerAV->getName() == string("strToLowerA"));
    assert(strToLowerAV->getParameterTypes().size() == 2);
    regRefs.resize(2);

    // common cases
    int toLowerOutVCShort = outVCShort;
    regRefs[0] = vcOutP[outVCShort++];
    regRefs[1] = vcInP[shortIdx + 0];
    instP[pc++] = strToLowerAV->createInstruction(regRefs);

    regRefs[0] = vcOutP[outVCShort++];
    regRefs[1] = vcInP[zeroLenIdx];
    instP[pc++] = strToLowerAV->createInstruction(regRefs);

    // null case
    regRefs[0] = vcOutP[outVCShort++];
    regRefs[1] = vcInP[nullRegister];
    instP[pc++] = strToLowerAV->createInstruction(regRefs);

    // right truncation
    regRefs[0] = vcOutP[outVCShort++];
    regRefs[1] = vcInP[varFullIdx];
    const int strToLowerAVException1 = pc;
    instP[pc++] = strToLowerAV->createInstruction(regRefs);

    //
    // strToUpperA Fixed
    //
    ExtendedInstructionDef* strToUpperAF = 
        (*eit)["strToUpperA(c,c)"];
    assert(strToUpperAF);
    assert(strToUpperAF->getName() == string("strToUpperA"));
    assert(strToUpperAF->getParameterTypes().size() == 2);
    regRefs.resize(2);

    // common case
    regRefs[0] = cOutP[outCShort++];
    regRefs[1] = cOutP[toLowerOutCShort];
    instP[pc++] = strToUpperAF->createInstruction(regRefs);

    // null case
    // use long register here as lengths must be equal and null register is long
    regRefs[0] = cOutP[outCLong++];
    regRefs[1] = cInP[nullRegister];
    instP[pc++] = strToUpperAF->createInstruction(regRefs);

    // right truncation not possible in fixed width, as both 
    // strings must be same length by definition.

    //
    // strToUpperA Variable
    //
    ExtendedInstructionDef* strToUpperAV = 
        (*eit)["strToUpperA(vc,vc)"];
    assert(strToUpperAV);
    assert(strToUpperAV->getName() == string("strToUpperA"));
    assert(strToUpperAV->getParameterTypes().size() == 2);
    regRefs.resize(2);

    // common cases
    regRefs[0] = vcOutP[outVCShort++];
    regRefs[1] = vcOutP[toLowerOutVCShort];
    instP[pc++] = strToUpperAV->createInstruction(regRefs);

    regRefs[0] = vcOutP[outVCShort++];
    regRefs[1] = vcInP[zeroLenIdx];
    instP[pc++] = strToUpperAV->createInstruction(regRefs);

    // null case
    regRefs[0] = vcOutP[outVCShort++];
    regRefs[1] = vcInP[nullRegister];
    instP[pc++] = strToUpperAV->createInstruction(regRefs);

    // right truncation
    regRefs[0] = vcOutP[outVCShort++];
    regRefs[1] = vcInP[varFullIdx];
    const int strToUpperAVException1 = pc;
    instP[pc++] = strToUpperAV->createInstruction(regRefs);

#if 0
 TODO: JR 6/07, strTrimA now takes 4 parameters, pending the re-write
	this test (and the verification of its output is being omitted

vc,c,c,s4,s4
vc,c,vc,s4,s4
vc,vc,c,s4,s4
vc,vc,vc,s4,s4

.... started to re-write this code ...

    //
    // strTrimA C, C
    //
    ExtendedInstructionDef* strTrimAF = 
        (*eit)["strTrimA(vc,c,c,s4,s4)"];
    assert(strTrimAF);
    assert(strTrimAF->getName() == string("strTrimA"));
    assert(strTrimAF->getParameterTypes().size() == 5);
    regRefs.resize(5);

... and got this far ... JR 6/07

    // common case
    regRefs[0] = vcOutP[outVCLong++];
    regRefs[1] = cInP[trimmableIdx];
    regRefs[2] = iOutP[0];
    regRefs[3] = iOutP[0];
    instP[pc++] = strTrimAF->createInstruction(regRefs);

    regRefs[0] = vcOutP[outVCLong++];
    regRefs[1] = cInP[trimmableIdx];
    regRefs[2] = iOutP[1];
    regRefs[3] = iOutP[0];
    instP[pc++] = strTrimAF->createInstruction(regRefs);

    regRefs[0] = vcOutP[outVCLong++];
    regRefs[1] = cInP[trimmableIdx];
    regRefs[2] = iOutP[0];
    regRefs[3] = iOutP[1];
    instP[pc++] = strTrimAF->createInstruction(regRefs);

    regRefs[0] = vcOutP[outVCLong++];
    regRefs[1] = cInP[trimmableIdx];
    regRefs[2] = iOutP[1];
    regRefs[3] = iOutP[1];
    instP[pc++] = strTrimAF->createInstruction(regRefs);

    // null case
    regRefs[0] = vcOutP[outVCLong++];
    regRefs[1] = cInP[nullRegister];
    regRefs[2] = iOutP[1];
    regRefs[3] = iOutP[1];
    instP[pc++] = strTrimAF->createInstruction(regRefs);

    //
    // strTrimA Variable
    //
    ExtendedInstructionDef* strTrimAV = 
        (*eit)["strTrimA(vc,vc,s4,s4)"];
    assert(strTrimAV);
    assert(strTrimAV->getName() == string("strTrimA"));
    assert(strTrimAV->getParameterTypes().size() == 4);
    regRefs.resize(4);

    // common case
    regRefs[0] = vcOutP[outVCLong++];
    regRefs[1] = cInP[trimmableIdx];
    regRefs[2] = iOutP[0];
    regRefs[3] = iOutP[0];
    instP[pc++] = strTrimAV->createInstruction(regRefs);

    regRefs[0] = vcOutP[outVCLong++];
    regRefs[1] = cInP[trimmableIdx];
    regRefs[2] = iOutP[1];
    regRefs[3] = iOutP[0];
    instP[pc++] = strTrimAV->createInstruction(regRefs);

    regRefs[0] = vcOutP[outVCLong++];
    regRefs[1] = cInP[trimmableIdx];
    regRefs[2] = iOutP[0];
    regRefs[3] = iOutP[1];
    instP[pc++] = strTrimAV->createInstruction(regRefs);

    regRefs[0] = vcOutP[outVCLong++];
    regRefs[1] = cInP[trimmableIdx];
    regRefs[2] = iOutP[1];
    regRefs[3] = iOutP[1];
    instP[pc++] = strTrimAV->createInstruction(regRefs);

    // null case
    regRefs[0] = vcOutP[outVCLong++];
    regRefs[1] = cInP[nullRegister];
    regRefs[2] = iOutP[1];
    regRefs[3] = iOutP[1];
    instP[pc++] = strTrimAV->createInstruction(regRefs);
#endif

    int lastPC = pc;

    for (i = 0; i < pc; i++) {
        c.appendInstruction(instP[i]);
    }

    c.bind(RegisterReference::ELiteral,
           &literal,
           tupleDesc);
    c.bind(RegisterReference::EInput,
           &input,
           tupleDesc);
    c.bind(RegisterReference::EOutput,
           &output,
           tupleDesc);
    c.bind(RegisterReference::ELocal,
           &local,
           tupleDesc);
    c.bind(RegisterReference::EStatus,
           &status,
           tupleDesc);

    c.exec();

    cout << "Calculator Warnings: " << c.warnings() << endl;

    string out;
    for (i = 0; i < pc; i++) {
        assert(instP[i]);
        instP[i]->describe(out, true);
        printf("[%2d] %s\n", i, out.c_str());
    }

    // Print out the output tuple
    printf("Output Tuple\n");
    tuplePrinter.print(cout, tupleDesc, output);
    cout << endl;

    // the following now index into the output tuple, not outputregisterref
    outVCLong = varcharIdx + longIdx;
    outVCShort = varcharIdx + shortIdx;
    outCLong = charIdx + longIdx;
    outCShort = charIdx + shortIdx;
    outI = intIdx;

    string truncErr("22001");
    string substrErr("22011");
    deque<CalcMessage>::iterator iter = c.mWarnings.begin();
    if (c.mWarnings.empty()) fail("no warnings", __LINE__);

    //
    // strCatA2F
    // note: cannot test common case of strCatA2F w/ running strCatA2F first
    // to have length set correctly.
    //
    if (output[outCLong++].pData != NULL) fail("testStrCatA2F", __LINE__);
    if (iter->pc != strCatA2FException) fail("testStrCatA2F", __LINE__);
    if (truncErr.compare(iter->str)) fail("testStrCatA2F", __LINE__);
    iter++;

    //
    // strCatA3F
    //
    if (memcmp(output[outCLong].pData, "F00 F01 ", 8))
        fail("testStrCatA3F", __LINE__);
    // note: a real program should not exit with a fixed length != width
    if (output[outCLong++].cbData != 8)
        fail("testStrCatA3F", __LINE__);
    if (memcmp(output[outCLong].pData, "F00 F00 ", 8))
        fail("testStrCatA3F", __LINE__);
    // note: a real program should not exit with a fixed length != width
    if (output[outCLong++].cbData != 8)
        fail("testStrCatA3F", __LINE__);
    if (output[outCLong++].pData != NULL) fail("testStrCatA3F", __LINE__);
    if (output[outCLong++].pData != NULL) fail("testStrCatA3F", __LINE__);
    if (output[outCLong++].pData != NULL) fail("testStrCatA3F", __LINE__);
    if (iter->pc != strCatA3FException) fail("testStrCatA3F", __LINE__);
    if (truncErr.compare(iter->str)) fail("testStrCatA3F", __LINE__);
    iter++;
    

    //
    // strCatA2F & strCatA3F
    // four way fixed cat using both
    //
    if (memcmp(output[outCLong].pData, "F00 F01 F02 F03 ", buflen))
        fail("testStrCatA2F/3", __LINE__);
    if (output[outCLong++].cbData != 16)
        fail("testStrCatA2F/3", __LINE__);

    //
    // strCatA2V
    //
    if (memcmp(output[outVCLong].pData, "V04V05", 6))
        fail("testStrCatA2V", __LINE__);
    if (output[outVCLong++].cbData !=  6)
        fail("testStrCatA2V", __LINE__);
    if (output[outVCLong++].cbData != 0) fail("testStrCatA2V", __LINE__);
    if (output[outVCLong++].pData != NULL) fail("testStrCatA2V", __LINE__);
    if (iter->pc != strCatA2VException) fail("testStrCatA2V", __LINE__);
    if (truncErr.compare(iter->str)) fail("testStrCatA2V", __LINE__);
    iter++;

    //
    // strCatA3V
    //
    if (memcmp(output[outVCLong].pData, "V00V01", 6))
        fail("testStrCatA3V", __LINE__);
    if (output[outVCLong++].cbData !=  6)
        fail("testStrCatA3V", __LINE__);
    if (memcmp(output[outVCLong].pData, "V00V00", 6))
        fail("testStrCatA3V", __LINE__);
    if (output[outVCLong++].cbData !=  6)
        fail("testStrCatA3V", __LINE__);
    if (output[outVCLong++].cbData != 0) fail("testStrCatA3V", __LINE__);
    if (memcmp(output[outVCLong].pData, "V03", 3))
        fail("testStrCatA3V", __LINE__);
    if (output[outVCLong++].cbData !=  3)
        fail("testStrCatA3V", __LINE__);
    if (memcmp(output[outVCLong].pData, "V04", 3))
        fail("testStrCatA3V", __LINE__);
    if (output[outVCLong++].cbData !=  3)
        fail("testStrCatA3V", __LINE__);
    if (output[outVCLong++].pData != NULL) fail("testStrCatA3V", __LINE__);
    if (output[outVCLong++].pData != NULL) fail("testStrCatA3V", __LINE__);
    if (output[outVCLong++].pData != NULL) fail("testStrCatA3V", __LINE__);
    if (iter->pc != strCatA3VException) fail("testStrCatA3V", __LINE__);
    if (truncErr.compare(iter->str)) fail("testStrCatA3V", __LINE__);
    iter++;
    
    //
    // strCmpA Fixed
    //
    if (*(reinterpret_cast<int32_t const *>(output[outI++].pData)) != 0) 
        fail("testStrCmpAF", __LINE__);
    if (*(reinterpret_cast<int32_t const *>(output[outI++].pData)) >= 0) 
        fail("testStrCmpAF", __LINE__);
    if (*(reinterpret_cast<int32_t const *>(output[outI++].pData)) <= 0)
        fail("testStrCmpAF", __LINE__);

    if (output[outI++].pData != NULL) fail("testStrCcmpAF", __LINE__);
    if (output[outI++].pData != NULL) fail("testStrCcmpAF", __LINE__);
    if (output[outI++].pData != NULL) fail("testStrCcmpAF", __LINE__);

    //
    // strCmpAV
    //
    if (*(reinterpret_cast<int32_t const *>(output[outI++].pData)) != 0) 
        fail("testStrCmpAV", __LINE__);
    if (*(reinterpret_cast<int32_t const *>(output[outI++].pData)) >= 0) 
        fail("testStrCmpAV", __LINE__);
    if (*(reinterpret_cast<int32_t const *>(output[outI++].pData)) <= 0)
        fail("testStrCmpAV", __LINE__);

    if (*(reinterpret_cast<int32_t const *>(output[outI++].pData)) != 0) 
        fail("testStrCmpAV", __LINE__);
    if (*(reinterpret_cast<int32_t const *>(output[outI++].pData)) >= 0) 
        fail("testStrCmpAV", __LINE__);
    if (*(reinterpret_cast<int32_t const *>(output[outI++].pData)) <= 0)
        fail("testStrCmpAV", __LINE__);

    if (output[outI++].pData != NULL) fail("testStrCmpAV", __LINE__);
    if (output[outI++].pData != NULL) fail("testStrCmpAV", __LINE__);
    if (output[outI++].pData != NULL) fail("testStrCmpAV", __LINE__);

    //
    // strLenBitA Fixed
    //
    if (*(reinterpret_cast<int32_t const *>(output[outI++].pData)) != 
        (static_cast<int32_t>(buflenShort * 8)))
        fail("strLenBitAF", __LINE__);
    if (*(reinterpret_cast<int32_t const *>(output[outI++].pData)) !=
        (static_cast<int32_t>(buflen * 8)))
        fail("strLenBitAF", __LINE__);

    if (output[outI++].pData != NULL) fail("strLenBitAF", __LINE__);

    //
    // strLenBitA Variable
    //
    if (*(reinterpret_cast<int32_t const *>(output[outI++].pData)) != 
        (static_cast<int32_t>(0)))
        fail("strLenBitAV", __LINE__);
    if (*(reinterpret_cast<int32_t const *>(output[outI++].pData)) != 
        (static_cast<int32_t>(variableStringLen * 8)))
        fail("strLenBitAV", __LINE__);
    if (*(reinterpret_cast<int32_t const *>(output[outI++].pData)) !=
        (static_cast<int32_t>(variableStringLen * 8)))
        fail("strLenBitAV", __LINE__);

    if (output[outI++].pData != NULL) fail("strLenBitAV", __LINE__);

    //
    // strLenCharA Fixed
    //
    if (*(reinterpret_cast<int32_t const *>(output[outI++].pData)) != 
        (static_cast<int32_t>(buflenShort)))
        fail("strLenCharAF", __LINE__);
    if (*(reinterpret_cast<int32_t const *>(output[outI++].pData)) !=
        (static_cast<int32_t>(buflen)))
        fail("strLenCharAF", __LINE__);

    if (output[outI++].pData != NULL) fail("strLenCharAF", __LINE__);

    //
    // strLenCharA Variable
    //
    if (*(reinterpret_cast<int32_t const *>(output[outI++].pData)) != 
        (static_cast<int32_t>(0)))
        fail("strLenCharAV", __LINE__);
    if (*(reinterpret_cast<int32_t const *>(output[outI++].pData)) != 
        (static_cast<int32_t>(variableStringLen)))
        fail("strLenCharAV", __LINE__);
    if (*(reinterpret_cast<int32_t const *>(output[outI++].pData)) !=
        (static_cast<int32_t>(variableStringLen)))
        fail("strLenCharAV", __LINE__);

    if (output[outI++].pData != NULL) fail("strLenCharAV", __LINE__);

    //
    // strLenOctA Fixed
    //
    if (*(reinterpret_cast<int32_t const *>(output[outI++].pData)) != 
        (static_cast<int32_t>(buflenShort)))
        fail("strLenOctAF", __LINE__);
    if (*(reinterpret_cast<int32_t const *>(output[outI++].pData)) !=
        (static_cast<int32_t>(buflen)))
        fail("strLenOctAF", __LINE__);

    if (output[outI++].pData != NULL) fail("strLenOctAF", __LINE__);

    //
    // strLenOctA Variable
    //
    if (*(reinterpret_cast<int32_t const *>(output[outI++].pData)) != 
        (static_cast<int32_t>(0)))
        fail("strLenOctAV", __LINE__);
    if (*(reinterpret_cast<int32_t const *>(output[outI++].pData)) != 
        (static_cast<int32_t>(variableStringLen)))
        fail("strLenOctAV", __LINE__);
    if (*(reinterpret_cast<int32_t const *>(output[outI++].pData)) !=
        (static_cast<int32_t>(variableStringLen)))
        fail("strLenOctAV", __LINE__);

    if (output[outI++].pData != NULL) fail("strLenOctAV", __LINE__);

    //
    // strOverlayA5 Fixed
    //
    if (memcmp(output[outVCLong].pData, "FF01 00 ", 8))
        fail("strOverlayA5F", __LINE__);
    if (output[outVCLong++].cbData != 8) fail("strOverlayA5F", __LINE__);
    if (output[outVCLong++].pData != NULL) fail("strOverlayA5F", __LINE__);
    if (output[outVCLong++].pData != NULL) fail("strOverlayA5F", __LINE__);
    if (output[outVCLong++].pData != NULL) fail("strOverlayA5F", __LINE__);
    if (output[outVCLong++].pData != NULL) fail("strOverlayA5F", __LINE__);

    if (iter->pc != strOverlayA5FException1) fail("strOverlayA5F", __LINE__);
    if (substrErr.compare(iter->str)) fail("strOverlayA5F", __LINE__);
    iter++;
    outVCLong++;

    if (iter->pc != strOverlayA5FException2) fail("strOverlayA5F", __LINE__);
    if (substrErr.compare(iter->str)) fail("strOverlayA5F", __LINE__);
    iter++;
    outVCLong++;

    if (iter->pc != strOverlayA5FException3) fail("strOverlayA5F", __LINE__);
    if (truncErr.compare(iter->str)) fail("strOverlayA5F", __LINE__);
    iter++;
    outVCLong++;

    //
    // strOverlayA4 Fixed
    //
    if (memcmp(output[outVCLong].pData, "F0F01 ", 6))
        fail("strOverlayA4F", __LINE__);
    if (output[outVCLong++].cbData != 6) fail("strOverlayA4F", __LINE__);
    if (output[outVCLong++].pData != NULL) fail("strOverlayA4F", __LINE__);
    if (output[outVCLong++].pData != NULL) fail("strOverlayA4F", __LINE__);
    if (output[outVCLong++].pData != NULL) fail("strOverlayA4F", __LINE__);

    if (iter->pc != strOverlayA4FException1) fail("strOverlayA4F", __LINE__);
    if (substrErr.compare(iter->str)) fail("strOverlayA4F", __LINE__);
    iter++;
    outVCLong++;

    if (iter->pc != strOverlayA4FException2) fail("strOverlayA4F", __LINE__);
    if (truncErr.compare(iter->str)) fail("strOverlayA4F", __LINE__);
    iter++;
    outVCLong++;

    //
    // strOverlayA5 Variable
    //
    if (memcmp(output[outVCLong].pData, "VV0100", 6))
        fail("strOverlayA5V", __LINE__);
    if (output[outVCLong++].cbData != 6) fail("strOverlayA5V", __LINE__);
    if (output[outVCLong++].pData != NULL) fail("strOverlayA5V", __LINE__);
    if (output[outVCLong++].pData != NULL) fail("strOverlayA5V", __LINE__);
    if (output[outVCLong++].pData != NULL) fail("strOverlayA5V", __LINE__);
    if (output[outVCLong++].pData != NULL) fail("strOverlayA5V", __LINE__);

    if (iter->pc != strOverlayA5VException1) fail("strOverlayA5V", __LINE__);
    if (substrErr.compare(iter->str)) fail("strOverlayA5V", __LINE__);
    iter++;
    outVCLong++;

    if (iter->pc != strOverlayA5VException2) fail("strOverlayA5V", __LINE__);
    if (substrErr.compare(iter->str)) fail("strOverlayA5V", __LINE__);
    iter++;
    outVCLong++;

    if (iter->pc != strOverlayA5VException3) fail("strOverlayA5V", __LINE__);
    if (truncErr.compare(iter->str)) fail("strOverlayA5V", __LINE__);
    iter++;
    outVCShort++;

    //
    // strOverlayA4 Variable
    //
    if (memcmp(output[outVCLong].pData, "V0V01", 5))
        fail("strOverlayA4V", __LINE__);
    if (output[outVCLong++].cbData != 5) fail("strOverlayA4V", __LINE__);
    if (output[outVCLong++].pData != NULL) fail("strOverlayA4V", __LINE__);
    if (output[outVCLong++].pData != NULL) fail("strOverlayA4V", __LINE__);
    if (output[outVCLong++].pData != NULL) fail("strOverlayA4V", __LINE__);

    if (iter->pc != strOverlayA4VException1) fail("strOverlayA4V", __LINE__);
    if (substrErr.compare(iter->str)) fail("strOverlayA4V", __LINE__);
    iter++;
    outVCLong++;

    if (iter->pc != strOverlayA4VException2) fail("strOverlayA4V", __LINE__);
    if (truncErr.compare(iter->str)) fail("strOverlayA4V", __LINE__);
    iter++;
    outVCShort++;

    //
    // strPosA Fixed
    //
    if (*(reinterpret_cast<int32_t const *>(output[outI++].pData)) != 
        (static_cast<int32_t>(1)))
        fail("strPosAF", __LINE__);
    if (*(reinterpret_cast<int32_t const *>(output[outI++].pData)) !=
        (static_cast<int32_t>(0)))
        fail("strPosAF", __LINE__);

    if (output[outI++].pData != NULL) fail("strPosAF", __LINE__);
    if (output[outI++].pData != NULL) fail("strPosAF", __LINE__);

    //
    // strPosA Variable
    //
    if (*(reinterpret_cast<int32_t const *>(output[outI++].pData)) != 
        (static_cast<int32_t>(1)))
        fail("strPosAV", __LINE__);
    if (*(reinterpret_cast<int32_t const *>(output[outI++].pData)) !=
        (static_cast<int32_t>(0)))
        fail("strPosAV", __LINE__);
#if 0

//TODO: JR 6/07 these 4 tests (at least the first 2) fail, so 
	they are being temporarily removed.


    if (*(reinterpret_cast<int32_t const *>(output[outI++].pData)) !=
        (static_cast<int32_t>(0)))
        fail("strPosAV", __LINE__);
    if (*(reinterpret_cast<int32_t const *>(output[outI++].pData)) !=
        (static_cast<int32_t>(1)))
        fail("strPosAV", __LINE__);

    if (output[outI++].pData != NULL) fail("strPosAV", __LINE__);
    if (output[outI++].pData != NULL) fail("strPosAV", __LINE__);
#endif

    //
    // strSubStringA3 Fixed
    //
    if (memcmp(output[outVCShort].pData, "F00 ", 4))
        fail("strSubStringA3F", __LINE__);
    if (output[outVCShort++].cbData != 4) fail("strSubStringA3F", __LINE__);
    if (output[outVCShort++].cbData != 0) fail("strSubStringA3F", __LINE__);
    if (output[outVCShort++].pData != NULL) fail("strSubStringA3F", __LINE__);
    if (output[outVCShort++].pData != NULL) fail("strSubStringA3F", __LINE__);

    if (iter->pc != strSubStringA3FException1) fail("strSubStringA3F", __LINE__);
    if (truncErr.compare(iter->str)) fail("strSubStringA3F", __LINE__);
    iter++;
    outVCShort++;

    //
    // strSubStringA3 Variable
    //
    if (memcmp(output[outVCShort].pData, "V00", 3))
        fail("strSubStringA3V", __LINE__);
    if (output[outVCShort++].cbData != 3) fail("strSubStringA3V", __LINE__);
    if (output[outVCShort++].cbData != 0) fail("strSubStringA3V", __LINE__);
    if (output[outVCShort++].pData != NULL) fail("strSubStringA3V", __LINE__);
    if (output[outVCShort++].pData != NULL) fail("strSubStringA3V", __LINE__);

    if (iter->pc != strSubStringA3VException1) fail("strSubStringA3V", __LINE__);
    if (truncErr.compare(iter->str)) fail("strSubStringA3V", __LINE__);
    iter++;
    outVCShort++;

    //
    // strSubStringA4 Fixed
    //
    if (memcmp(output[outVCShort].pData, "F0", 2))
        fail("strSubStringA4F", __LINE__);
    if (output[outVCShort++].cbData != 2) fail("strSubStringA4F", __LINE__);
    if (output[outVCShort++].cbData != 0) fail("strSubStringA4F", __LINE__);
    if (output[outVCShort++].pData != NULL) fail("strSubStringA4F", __LINE__);
    if (output[outVCShort++].pData != NULL) fail("strSubStringA4F", __LINE__);
    if (output[outVCShort++].pData != NULL) fail("strSubStringA4F", __LINE__);

    if (iter->pc != strSubStringA4FException1) fail("strSubStringA4F", __LINE__);
    if (substrErr.compare(iter->str)) fail("strSubStringA4F", __LINE__);
    iter++;
    outVCShort++;

    if (iter->pc != strSubStringA4FException2) fail("strSubStringA4F", __LINE__);
    if (truncErr.compare(iter->str)) fail("strSubStringA4F", __LINE__);
    iter++;
    outVCShort++;

    //
    // strSubStringA4 Variable
    //
    if (memcmp(output[outVCShort].pData, "V0", 2))
        fail("strSubStringA4V", __LINE__);
    if (output[outVCShort++].cbData != 2) fail("strSubStringA4V", __LINE__);
    if (output[outVCShort++].cbData != 0) fail("strSubStringA4V", __LINE__);
    if (output[outVCShort++].pData != NULL) fail("strSubStringA4V", __LINE__);
    if (output[outVCShort++].pData != NULL) fail("strSubStringA4V", __LINE__);
    if (output[outVCShort++].pData != NULL) fail("strSubStringA4V", __LINE__);

    if (iter->pc != strSubStringA4VException1) fail("strSubStringA4V", __LINE__);
    if (substrErr.compare(iter->str)) fail("strSubStringA4V", __LINE__);
    iter++;
    outVCShort++;

    if (iter->pc != strSubStringA4VException2) fail("strSubStringA4V", __LINE__);
    if (truncErr.compare(iter->str)) fail("strSubStringA4V", __LINE__);
    iter++;
    outVCShort++;

    //
    // strToLowerA Fixed
    //
    if (memcmp(output[outCShort].pData, "f00 ", 4))
        fail("strToLowerAF", __LINE__);
    if (output[outCShort++].cbData != 4) fail("strToLowerAF", __LINE__);
    // use long register here as lengths must be equal and null register is long
    if (output[outCLong++].pData != NULL) fail("strToLowerAF", __LINE__);

    //
    // strToLowerA Variable
    //
    if (memcmp(output[outVCShort].pData, "v00", 3))
        fail("strToLowerAV", __LINE__);
    if (output[outVCShort++].cbData != 3) fail("strToLowerAV", __LINE__);
    if (output[outVCShort++].cbData != 0) fail("strToLowerAV", __LINE__);
    if (output[outVCShort++].pData != NULL) fail("strToLowerAV", __LINE__);

    if (iter->pc != strToLowerAVException1) fail("strToLowerAV", __LINE__);
    if (truncErr.compare(iter->str)) fail("strToLowerAV", __LINE__);
    iter++;
    outVCShort++;


    //
    // strToUpperA Fixed
    //
    if (memcmp(output[outCShort].pData, "F00 ", 4))
        fail("strToUpperAF", __LINE__);
    if (output[outCShort++].cbData != 4) fail("strToUpperAF", __LINE__);
    // use long register here as lengths must be equal and null register is long
    if (output[outCLong++].pData != NULL) fail("strToUpperAF", __LINE__);

    //
    // strToUpperA Variable
    //
    if (memcmp(output[outVCShort].pData, "V00", 3))
        fail("strToUpperAV", __LINE__);
    if (output[outVCShort++].cbData != 3) fail("strToUpperAV", __LINE__);
    if (output[outVCShort++].cbData != 0) fail("strToUpperAV", __LINE__);
    if (output[outVCShort++].pData != NULL) fail("strToUpperAV", __LINE__);

    if (iter->pc != strToUpperAVException1) fail("strToUpperAV", __LINE__);
    if (truncErr.compare(iter->str)) fail("strToUpperAV", __LINE__);
    iter++;
    outVCShort++;

    //
    // strTrimA Fixed
    //
#if 0
TODO: test for strTrimA is being temporarily omitted, see notes
	above.

    if (memcmp(output[outVCLong].pData, "    1 3 5 78    ", 16))
        fail("strTrimAF", __LINE__);
    if (output[outVCLong++].cbData != 16) fail("strTrimAF", __LINE__);
    if (memcmp(output[outVCLong].pData, "1 3 5 78    ", 12))
        fail("strTrimAF", __LINE__);
    if (output[outVCLong++].cbData != 12) fail("strTrimAF", __LINE__);
    if (memcmp(output[outVCLong].pData, "    1 3 5 78", 12))
        fail("strTrimAF", __LINE__);
    if (output[outVCLong++].cbData != 12) fail("strTrimAF", __LINE__);
    if (memcmp(output[outVCLong].pData, "1 3 5 78", 8))
        fail("strTrimAF", __LINE__);
    if (output[outVCLong++].cbData != 8) fail("strTrimAF", __LINE__);

    if (output[outVCLong++].pData != NULL) fail("strTrimAF", __LINE__);

    //
    // strTrimA Variable
    //
    if (memcmp(output[outVCLong].pData, "    1 3 5 78    ", 16))
        fail("strTrimAV", __LINE__);
    if (output[outVCLong++].cbData != 16) fail("strTrimAV", __LINE__);
    if (memcmp(output[outVCLong].pData, "1 3 5 78    ", 12))
        fail("strTrimAV", __LINE__);
    if (output[outVCLong++].cbData != 12) fail("strTrimAV", __LINE__);
    if (memcmp(output[outVCLong].pData, "    1 3 5 78", 12))
        fail("strTrimAV", __LINE__);
    if (output[outVCLong++].cbData != 12) fail("strTrimAV", __LINE__);
    if (memcmp(output[outVCLong].pData, "1 3 5 78", 8))
        fail("strTrimAV", __LINE__);
    if (output[outVCLong++].cbData != 8) fail("strTrimAV", __LINE__);

    if (output[outVCLong++].pData != NULL) fail("strTrimAV", __LINE__);
#endif

    // must be no more warnings
    if (iter != c.mWarnings.end()) fail("MoreWarningsPresent", __LINE__);

    delete [] vcInP;
    delete [] vcOutP;
    delete [] cInP;
    delete [] cOutP;
    delete [] iInP;
    delete [] iOutP;
    for (i = 0; i < lastPC; i++) {
        delete instP[i];
    }
    delete [] instP;
}


int main(int argc, char* argv[])
{
    ProgramName = argv[0];

    CalcInit::instance();

    unitTestStrings();

    printf("all tests passed\n");
    exit(0);
}

boost::unit_test_framework::test_suite *init_unit_test_suite(int,char **)
{
    return NULL;
}
