/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 2004-2004 Disruptive Tech
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

#include "fennel/calc/CalcCommon.h"
#include "fennel/calc/InstructionCommon.h"  // required as we're manipulating instructions

#include <stdlib.h>
#include <stdio.h>
#include <string>
#include <boost/scoped_array.hpp>
#include <limits>
#include <iostream.h>

using namespace std;
using namespace fennel;

char* ProgramName;

void
fail(const char* str, int line) {
    assert(ProgramName);
    assert(str);
    printf("%s: unit test failed: |%s| line %d\n", ProgramName, str, line);
    exit(-1);
}

void
unitTestBool()
{
    printf("=========================================================\n");
    printf("=========================================================\n");
    printf("=====\n");
    printf("=====     unitTestBool()\n");
    printf("=====\n");
    printf("=========================================================\n");
    printf("=========================================================\n");
    bool isNullable = true;    // Can tuple contain nulls?
    int i, registersize = 125;

    TupleDescriptor tupleDesc;
    tupleDesc.clear();

    // Build up a description of what we'd like the tuple to look like
    StandardTypeDescriptorFactory typeFactory;
    for (i=0;i < registersize; i++) {
        StoredTypeDescriptor const &typeDesc = typeFactory.newDataType(STANDARD_TYPE_BOOL);
        tupleDesc.push_back(TupleAttributeDescriptor(typeDesc, isNullable));
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
    tupleAccessorFixedLiteral.compute(tupleDesc, TUPLE_FORMAT_ALL_NOT_NULL_AND_FIXED);
    tupleAccessorFixedInput.compute(tupleDesc, TUPLE_FORMAT_ALL_NOT_NULL_AND_FIXED);
    tupleAccessorFixedOutput.compute(tupleDesc, TUPLE_FORMAT_ALL_NOT_NULL_AND_FIXED);
    tupleAccessorFixedLocal.compute(tupleDesc, TUPLE_FORMAT_ALL_NOT_NULL_AND_FIXED);
    tupleAccessorFixedStatus.compute(tupleDesc, TUPLE_FORMAT_ALL_NOT_NULL_AND_FIXED);

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
    tupleAccessorFixedLiteral.setCurrentTupleBuf(pTupleBufFixedLiteral.get());
    tupleAccessorFixedInput.setCurrentTupleBuf(pTupleBufFixedInput.get());
    tupleAccessorFixedOutput.setCurrentTupleBuf(pTupleBufFixedOutput.get());
    tupleAccessorFixedLocal.setCurrentTupleBuf(pTupleBufFixedLocal.get());
    tupleAccessorFixedStatus.setCurrentTupleBuf(pTupleBufFixedStatus.get());

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

    TupleData::iterator itr = tupleDataFixedLiteral.begin();
    for(i=0; i < registersize; i++, itr++) {
        *(reinterpret_cast<bool *>(const_cast<PBuffer>(itr->pData))) = false;
    }
    itr = tupleDataFixedInput.begin();
    for(i=0; i < registersize; i++, itr++) {
        *(reinterpret_cast<bool *>(const_cast<PBuffer>(itr->pData))) = false;
    }
    itr = tupleDataFixedOutput.begin();
    for(i=0; i < registersize; i++, itr++) {
        *(reinterpret_cast<bool *>(const_cast<PBuffer>(itr->pData))) = false;
    }
    itr = tupleDataFixedLocal.begin();
    for(i=0; i < registersize; i++, itr++) {
        *(reinterpret_cast<bool *>(const_cast<PBuffer>(itr->pData))) = false;
    }

    // create four nullable tuples to serve as register sets
    TupleData literal = tupleDataFixedLiteral;
    TupleData input = tupleDataFixedInput;
    TupleData output = tupleDataFixedOutput;
    TupleData local = tupleDataFixedLocal;
    TupleData status = tupleDataFixedStatus;

    // null out last element of each type
    int nullidx = registersize-1;
    literal[nullidx].pData = NULL;
    input[nullidx].pData = NULL;
    output[nullidx].pData = NULL;
    local[nullidx].pData = NULL;

    // Print out the nullable tuple
    TuplePrinter tuplePrinter;
    tuplePrinter.print(cout, tupleDesc, literal);
    cout << endl;
    tuplePrinter.print(cout, tupleDesc, input);
    cout << endl;
    tuplePrinter.print(cout, tupleDesc, output);
    cout << endl;
    tuplePrinter.print(cout, tupleDesc, local);
    cout << endl;

    // set up some nice literals for tests
    *(reinterpret_cast<bool *>(const_cast<PBuffer>((literal[0].pData)))) = false;
    *(reinterpret_cast<bool *>(const_cast<PBuffer>((literal[1].pData)))) = true;

    // predefine register references. a real compiler wouldn't do
    // something so regular and pre-determined. a compiler would
    // probably build these on the fly as it built each instruction.
    RegisterRef<bool> **bInP, **bOutP, **bLoP, **bLiP;
    bInP = new RegisterRef<bool>*[registersize];
    bOutP = new RegisterRef<bool>*[registersize];
    bLoP = new RegisterRef<bool>*[registersize];
    bLiP = new RegisterRef<bool>*[registersize];

    // Set up the Calculator
    Calculator c(0,0,0,0,0,0);
    c.outputRegisterByReference(false);

    // set up register references to symbolically point to 
    // their corresponding storage locations -- makes for easy test case
    // generation. again, a compiler wouldn't do things in quite
    // this way
    for (i=0; i < registersize; i++) {
        bInP[i] = new RegisterRef<bool>(RegisterReference::EInput,
                                        i,
                                        STANDARD_TYPE_BOOL);
        c.appendRegRef(bInP[i]);
        bOutP[i] = new RegisterRef<bool>(RegisterReference::EOutput,
                                         i,
                                         STANDARD_TYPE_BOOL);
        c.appendRegRef(bOutP[i]);
        bLoP[i] = new RegisterRef<bool>(RegisterReference::ELocal, 
                                        i,
                                        STANDARD_TYPE_BOOL);
        c.appendRegRef(bLoP[i]);
        bLiP[i] = new RegisterRef<bool>(RegisterReference::ELiteral,
                                        i,
                                        STANDARD_TYPE_BOOL);
        c.appendRegRef(bLiP[i]);
    }


    // Set up storage for instructions
    // a real compiler would probably cons up instructions and insert them
    // directly into the calculator. keep an array of the instructions at
    // this level to allow printing of the program after execution, and other
    // debugging
    Instruction **instP;
    instP = new (Instruction *)[200];
    int pc=0, outC= 0;

    // not
    instP[pc++] = new BoolNot(bOutP[outC++], bLiP[0]);
    instP[pc++] = new BoolNot(bOutP[outC++], bLiP[1]);
    instP[pc++] = new BoolNot(bOutP[outC++], bLiP[nullidx]);

    // and
    instP[pc++] = new BoolAnd(bOutP[outC++], bLiP[0], bLiP[0]);
    instP[pc++] = new BoolAnd(bOutP[outC++], bLiP[1], bLiP[1]);
    instP[pc++] = new BoolAnd(bOutP[outC++], bLiP[0], bLiP[1]);
    instP[pc++] = new BoolAnd(bOutP[outC++], bLiP[1], bLiP[0]);
    instP[pc++] = new BoolAnd(bOutP[outC++], bLiP[nullidx], bLiP[0]);
    instP[pc++] = new BoolAnd(bOutP[outC++], bLiP[nullidx], bLiP[1]);
    instP[pc++] = new BoolAnd(bOutP[outC++], bLiP[0], bLiP[nullidx]);
    instP[pc++] = new BoolAnd(bOutP[outC++], bLiP[1], bLiP[nullidx]);
    instP[pc++] = new BoolAnd(bOutP[outC++], bLiP[nullidx], bLiP[nullidx]);

    // or
    instP[pc++] = new BoolOr(bOutP[outC++], bLiP[0], bLiP[0]);
    instP[pc++] = new BoolOr(bOutP[outC++], bLiP[1], bLiP[1]);
    instP[pc++] = new BoolOr(bOutP[outC++], bLiP[0], bLiP[1]);
    instP[pc++] = new BoolOr(bOutP[outC++], bLiP[1], bLiP[0]);
    instP[pc++] = new BoolOr(bOutP[outC++], bLiP[nullidx], bLiP[0]);
    instP[pc++] = new BoolOr(bOutP[outC++], bLiP[nullidx], bLiP[1]);
    instP[pc++] = new BoolOr(bOutP[outC++], bLiP[0], bLiP[nullidx]);
    instP[pc++] = new BoolOr(bOutP[outC++], bLiP[1], bLiP[nullidx]);
    instP[pc++] = new BoolOr(bOutP[outC++], bLiP[nullidx], bLiP[nullidx]);

    // move
    instP[pc++] = new BoolMove(bOutP[outC++], bLiP[0]);
    instP[pc++] = new BoolMove(bOutP[outC++], bLiP[1]);
    instP[pc++] = new BoolMove(bOutP[outC++], bLiP[nullidx]);

    // is
    instP[pc++] = new BoolIs(bOutP[outC++], bLiP[0], bLiP[0]);
    instP[pc++] = new BoolIs(bOutP[outC++], bLiP[1], bLiP[1]);
    instP[pc++] = new BoolIs(bOutP[outC++], bLiP[0], bLiP[1]);
    instP[pc++] = new BoolIs(bOutP[outC++], bLiP[1], bLiP[0]);

    instP[pc++] = new BoolIs(bOutP[outC++], bLiP[nullidx], bLiP[0]);
    instP[pc++] = new BoolIs(bOutP[outC++], bLiP[nullidx], bLiP[1]);
    instP[pc++] = new BoolIs(bOutP[outC++], bLiP[0], bLiP[nullidx]);
    instP[pc++] = new BoolIs(bOutP[outC++], bLiP[1], bLiP[nullidx]);
    instP[pc++] = new BoolIs(bOutP[outC++], bLiP[nullidx], bLiP[nullidx]);

    // isnot
    instP[pc++] = new BoolIsNot(bOutP[outC++], bLiP[0], bLiP[0]);
    instP[pc++] = new BoolIsNot(bOutP[outC++], bLiP[1], bLiP[1]);
    instP[pc++] = new BoolIsNot(bOutP[outC++], bLiP[0], bLiP[1]);
    instP[pc++] = new BoolIsNot(bOutP[outC++], bLiP[1], bLiP[0]);

    instP[pc++] = new BoolIsNot(bOutP[outC++], bLiP[nullidx], bLiP[0]);
    instP[pc++] = new BoolIsNot(bOutP[outC++], bLiP[nullidx], bLiP[1]);
    instP[pc++] = new BoolIsNot(bOutP[outC++], bLiP[0], bLiP[nullidx]);
    instP[pc++] = new BoolIsNot(bOutP[outC++], bLiP[1], bLiP[nullidx]);
    instP[pc++] = new BoolIsNot(bOutP[outC++], bLiP[nullidx], bLiP[nullidx]);

    // equal
    instP[pc++] = new BoolEqual(bOutP[outC++], bLiP[0], bLiP[0]);
    instP[pc++] = new BoolEqual(bOutP[outC++], bLiP[1], bLiP[1]);
    instP[pc++] = new BoolEqual(bOutP[outC++], bLiP[0], bLiP[1]);
    instP[pc++] = new BoolEqual(bOutP[outC++], bLiP[1], bLiP[0]);

    instP[pc++] = new BoolEqual(bOutP[outC++], bLiP[nullidx], bLiP[0]);
    instP[pc++] = new BoolEqual(bOutP[outC++], bLiP[nullidx], bLiP[1]);
    instP[pc++] = new BoolEqual(bOutP[outC++], bLiP[0], bLiP[nullidx]);
    instP[pc++] = new BoolEqual(bOutP[outC++], bLiP[1], bLiP[nullidx]);
    instP[pc++] = new BoolEqual(bOutP[outC++], bLiP[nullidx], bLiP[nullidx]);

    // notequal
    instP[pc++] = new BoolNotEqual(bOutP[outC++], bLiP[0], bLiP[0]);
    instP[pc++] = new BoolNotEqual(bOutP[outC++], bLiP[1], bLiP[1]);
    instP[pc++] = new BoolNotEqual(bOutP[outC++], bLiP[0], bLiP[1]);
    instP[pc++] = new BoolNotEqual(bOutP[outC++], bLiP[1], bLiP[0]);

    instP[pc++] = new BoolNotEqual(bOutP[outC++], bLiP[nullidx], bLiP[0]);
    instP[pc++] = new BoolNotEqual(bOutP[outC++], bLiP[nullidx], bLiP[1]);
    instP[pc++] = new BoolNotEqual(bOutP[outC++], bLiP[0], bLiP[nullidx]);
    instP[pc++] = new BoolNotEqual(bOutP[outC++], bLiP[1], bLiP[nullidx]);
    instP[pc++] = new BoolNotEqual(bOutP[outC++], bLiP[nullidx], bLiP[nullidx]);

    // greater
    instP[pc++] = new BoolGreater(bOutP[outC++], bLiP[0], bLiP[0]);
    instP[pc++] = new BoolGreater(bOutP[outC++], bLiP[1], bLiP[1]);
    instP[pc++] = new BoolGreater(bOutP[outC++], bLiP[0], bLiP[1]);
    instP[pc++] = new BoolGreater(bOutP[outC++], bLiP[1], bLiP[0]);

    instP[pc++] = new BoolGreater(bOutP[outC++], bLiP[nullidx], bLiP[0]);
    instP[pc++] = new BoolGreater(bOutP[outC++], bLiP[nullidx], bLiP[1]);
    instP[pc++] = new BoolGreater(bOutP[outC++], bLiP[0], bLiP[nullidx]);
    instP[pc++] = new BoolGreater(bOutP[outC++], bLiP[1], bLiP[nullidx]);
    instP[pc++] = new BoolGreater(bOutP[outC++], bLiP[nullidx], bLiP[nullidx]);

    // greaterequal
    instP[pc++] = new BoolGreaterEqual(bOutP[outC++], bLiP[0], bLiP[0]);
    instP[pc++] = new BoolGreaterEqual(bOutP[outC++], bLiP[1], bLiP[1]);
    instP[pc++] = new BoolGreaterEqual(bOutP[outC++], bLiP[0], bLiP[1]);
    instP[pc++] = new BoolGreaterEqual(bOutP[outC++], bLiP[1], bLiP[0]);

    instP[pc++] = new BoolGreaterEqual(bOutP[outC++], bLiP[nullidx], bLiP[0]);
    instP[pc++] = new BoolGreaterEqual(bOutP[outC++], bLiP[nullidx], bLiP[1]);
    instP[pc++] = new BoolGreaterEqual(bOutP[outC++], bLiP[0], bLiP[nullidx]);
    instP[pc++] = new BoolGreaterEqual(bOutP[outC++], bLiP[1], bLiP[nullidx]);
    instP[pc++] = new BoolGreaterEqual(bOutP[outC++], bLiP[nullidx], bLiP[nullidx]);

    // less
    instP[pc++] = new BoolLess(bOutP[outC++], bLiP[0], bLiP[0]);
    instP[pc++] = new BoolLess(bOutP[outC++], bLiP[1], bLiP[1]);
    instP[pc++] = new BoolLess(bOutP[outC++], bLiP[0], bLiP[1]);
    instP[pc++] = new BoolLess(bOutP[outC++], bLiP[1], bLiP[0]);

    instP[pc++] = new BoolLess(bOutP[outC++], bLiP[nullidx], bLiP[0]);
    instP[pc++] = new BoolLess(bOutP[outC++], bLiP[nullidx], bLiP[1]);
    instP[pc++] = new BoolLess(bOutP[outC++], bLiP[0], bLiP[nullidx]);
    instP[pc++] = new BoolLess(bOutP[outC++], bLiP[1], bLiP[nullidx]);
    instP[pc++] = new BoolLess(bOutP[outC++], bLiP[nullidx], bLiP[nullidx]);

    // lessequal
    instP[pc++] = new BoolLessEqual(bOutP[outC++], bLiP[0], bLiP[0]);
    instP[pc++] = new BoolLessEqual(bOutP[outC++], bLiP[1], bLiP[1]);
    instP[pc++] = new BoolLessEqual(bOutP[outC++], bLiP[0], bLiP[1]);
    instP[pc++] = new BoolLessEqual(bOutP[outC++], bLiP[1], bLiP[0]);

    instP[pc++] = new BoolLessEqual(bOutP[outC++], bLiP[nullidx], bLiP[0]);
    instP[pc++] = new BoolLessEqual(bOutP[outC++], bLiP[nullidx], bLiP[1]);
    instP[pc++] = new BoolLessEqual(bOutP[outC++], bLiP[0], bLiP[nullidx]);
    instP[pc++] = new BoolLessEqual(bOutP[outC++], bLiP[1], bLiP[nullidx]);
    instP[pc++] = new BoolLessEqual(bOutP[outC++], bLiP[nullidx], bLiP[nullidx]);

    // isnull
    instP[pc++] = new BoolIsNull(bOutP[outC++], bLiP[1]);
    instP[pc++] = new BoolIsNull(bOutP[outC++], bLiP[nullidx]);

    // isnotnull
    instP[pc++] = new BoolIsNotNull(bOutP[outC++], bLiP[1]);
    instP[pc++] = new BoolIsNotNull(bOutP[outC++], bLiP[nullidx]);

    // tonull
    instP[pc++] = new BoolToNull(bOutP[outC++]);
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
  
    string out;
    for (i = 0; i < pc; i++) {
        instP[i]->describe(out, true);
        printf("[%2d] %s\n", i, out.c_str());
    }
    if (!c.mWarnings.empty()) fail("boolwarnings", __LINE__);

    // Print out the output tuple
    tuplePrinter.print(cout, tupleDesc, output);
    cout << endl;

    outC = 0;
    // not
    if (*(output[outC++].pData) != true) fail("boolnot1", __LINE__);
    if (*(output[outC++].pData) != false) fail("boolnot2", __LINE__);
    if (output[outC++].pData != NULL) fail("boolnot3", __LINE__);

    // and
    if (*(output[outC++].pData) != false) fail("booland1", __LINE__);
    if (*(output[outC++].pData) != true) fail("booland2", __LINE__);
    if (*(output[outC++].pData) != false) fail("booland3", __LINE__);
    if (*(output[outC++].pData) != false) fail("booland4", __LINE__);

    if (*(output[outC++].pData) != false) fail("booland5", __LINE__);
    if (output[outC++].pData != NULL) fail("booland6", __LINE__);
    if (*(output[outC++].pData) != false) fail("booland7", __LINE__);
    if (output[outC++].pData != NULL) fail("booland8", __LINE__);
    if (output[outC++].pData != NULL) fail("booland9", __LINE__);

    // or
    if (*(output[outC++].pData) != false) fail("boolor1", __LINE__);
    if (*(output[outC++].pData) != true) fail("boolor2", __LINE__);
    if (*(output[outC++].pData) != true) fail("boolor3", __LINE__);
    if (*(output[outC++].pData) != true) fail("boolor4", __LINE__);

    if (output[outC++].pData != NULL) fail("boolor5", __LINE__);
    if (*(output[outC++].pData) != true) fail("boolor6", __LINE__);
    if (output[outC++].pData != NULL) fail("boolor7", __LINE__);
    if (*(output[outC++].pData) != true) fail("boolor8", __LINE__);
    if (output[outC++].pData != NULL) fail("boolor9", __LINE__);

    // move
    if (*(output[outC++].pData) != false) fail("boolmove1", __LINE__);
    if (*(output[outC++].pData) != true) fail("boolmove2", __LINE__);
    if (output[outC++].pData != NULL) fail("boolmove3", __LINE__);

    // is
    if (*(output[outC++].pData) != true) fail("boolis1", __LINE__);
    if (*(output[outC++].pData) != true) fail("boolis2", __LINE__);
    if (*(output[outC++].pData) != false) fail("boolis3", __LINE__);
    if (*(output[outC++].pData) != false) fail("boolis4", __LINE__);

    if (*(output[outC++].pData) != false) fail("boolis5", __LINE__);
    if (*(output[outC++].pData) != false) fail("boolis6", __LINE__);
    if (*(output[outC++].pData) != false) fail("boolis7", __LINE__);
    if (*(output[outC++].pData) != false) fail("boolis8", __LINE__);
    if (*(output[outC++].pData) != true) fail("boolis9", __LINE__);

    // isnot
    if (*(output[outC++].pData) != false) fail("boolisnot1", __LINE__);
    if (*(output[outC++].pData) != false) fail("boolisnot2", __LINE__);
    if (*(output[outC++].pData) != true) fail("boolisnot3", __LINE__);
    if (*(output[outC++].pData) != true) fail("boolisnot4", __LINE__);

    if (*(output[outC++].pData) != true) fail("boolisnot5", __LINE__);
    if (*(output[outC++].pData) != true) fail("boolisnot6", __LINE__);
    if (*(output[outC++].pData) != true) fail("boolisnot7", __LINE__);
    if (*(output[outC++].pData) != true) fail("boolisnot8", __LINE__);
    if (*(output[outC++].pData) != false) fail("boolisnot9", __LINE__);

    // equal
    if (*(output[outC++].pData) != true) fail("boolequal1", __LINE__);
    if (*(output[outC++].pData) != true) fail("boolequal2", __LINE__);
    if (*(output[outC++].pData) != false) fail("boolequal3", __LINE__);
    if (*(output[outC++].pData) != false) fail("boolequal4", __LINE__);

    if (output[outC++].pData != NULL) fail("boolequal5", __LINE__);
    if (output[outC++].pData != NULL) fail("boolequal6", __LINE__);
    if (output[outC++].pData != NULL) fail("boolequal7", __LINE__);
    if (output[outC++].pData != NULL) fail("boolequal8", __LINE__);
    if (output[outC++].pData != NULL) fail("boolequal9", __LINE__);

    // notequal
    if (*(output[outC++].pData) != false) fail("boolnotequal1", __LINE__);
    if (*(output[outC++].pData) != false) fail("boolnotequal2", __LINE__);
    if (*(output[outC++].pData) != true) fail("boolnotequal3", __LINE__);
    if (*(output[outC++].pData) != true) fail("boolnotequal4", __LINE__);

    if (output[outC++].pData != NULL) fail("boolnotequal5", __LINE__);
    if (output[outC++].pData != NULL) fail("boolnotequal6", __LINE__);
    if (output[outC++].pData != NULL) fail("boolnotequal7", __LINE__);
    if (output[outC++].pData != NULL) fail("boolnotequal8", __LINE__);
    if (output[outC++].pData != NULL) fail("boolnotequal9", __LINE__);

    // greater
    if (*(output[outC++].pData) != false) fail("boolgreater1", __LINE__);
    if (*(output[outC++].pData) != false) fail("boolgreater2", __LINE__);
    if (*(output[outC++].pData) != false) fail("boolgreater3", __LINE__);
    if (*(output[outC++].pData) != true) fail("boolgreater4", __LINE__);

    if (output[outC++].pData != NULL) fail("boolgreater5", __LINE__);
    if (output[outC++].pData != NULL) fail("boolgreater6", __LINE__);
    if (output[outC++].pData != NULL) fail("boolgreater7", __LINE__);
    if (output[outC++].pData != NULL) fail("boolgreater8", __LINE__);
    if (output[outC++].pData != NULL) fail("boolgreater9", __LINE__);

    // greaterequal
    if (*(output[outC++].pData) != true) fail("boolgreaterequal1", __LINE__);
    if (*(output[outC++].pData) != true) fail("boolgreaterequal2", __LINE__);
    if (*(output[outC++].pData) != false) fail("boolgreaterequal3", __LINE__);
    if (*(output[outC++].pData) != true) fail("boolgreaterequal4", __LINE__);

    if (output[outC++].pData != NULL) fail("boolgreaterequal5", __LINE__);
    if (output[outC++].pData != NULL) fail("boolgreaterequal6", __LINE__);
    if (output[outC++].pData != NULL) fail("boolgreaterequal7", __LINE__);
    if (output[outC++].pData != NULL) fail("boolgreaterequal8", __LINE__);
    if (output[outC++].pData != NULL) fail("boolgreaterequal9", __LINE__);

    // less
    if (*(output[outC++].pData) != false) fail("boolless1", __LINE__);
    if (*(output[outC++].pData) != false) fail("boolless2", __LINE__);
    if (*(output[outC++].pData) != true) fail("boolless3", __LINE__);
    if (*(output[outC++].pData) != false) fail("boolless4", __LINE__);

    if (output[outC++].pData != NULL) fail("boolless5", __LINE__);
    if (output[outC++].pData != NULL) fail("boolless6", __LINE__);
    if (output[outC++].pData != NULL) fail("boolless7", __LINE__);
    if (output[outC++].pData != NULL) fail("boolless8", __LINE__);
    if (output[outC++].pData != NULL) fail("boolless9", __LINE__);

    // lessequal
    if (*(output[outC++].pData) != true) fail("boollessequal1", __LINE__);
    if (*(output[outC++].pData) != true) fail("boollessequal2", __LINE__);
    if (*(output[outC++].pData) != true) fail("boollessequal3", __LINE__);
    if (*(output[outC++].pData) != false) fail("boollessequal4", __LINE__);

    if (output[outC++].pData != NULL) fail("boollessequal5", __LINE__);
    if (output[outC++].pData != NULL) fail("boollessequal6", __LINE__);
    if (output[outC++].pData != NULL) fail("boollessequal7", __LINE__);
    if (output[outC++].pData != NULL) fail("boollessequal8", __LINE__);
    if (output[outC++].pData != NULL) fail("boollessequal9", __LINE__);

    // isnull
    if (*(output[outC++].pData) != false) fail("boolisnull1", __LINE__);
    if (*(output[outC++].pData) != true) fail("boolisnull1", __LINE__);

    // isnotnull
    if (*(output[outC++].pData) != true) fail("boolisnotnull1", __LINE__);
    if (*(output[outC++].pData) != false) fail("boolisnotnull1", __LINE__);

    // tonull
    if (output[outC++].pData != NULL) fail("booltonull1", __LINE__);

    cout << "Calculator Warnings: " << c.warnings() << endl;

    delete [] bInP;
    delete [] bOutP;
    delete [] bLoP;
    delete [] bLiP;
    for (i = 0; i < lastPC; i++) {
        delete instP[i];
    }
    delete [] instP;
}

void
unitTestLong()
{
    printf("=========================================================\n");
    printf("=========================================================\n");
    printf("=====\n");
    printf("=====     unitTestLong()\n");
    printf("=====\n");
    printf("=========================================================\n");
    printf("=========================================================\n");
    bool isNullable = true;    // Can tuple contain nulls?
    int i, registersize = 200;

    TupleDescriptor tupleDesc;
    tupleDesc.clear();

    // Build up a description of what we'd like the tuple to look like
    StandardTypeDescriptorFactory typeFactory;
    for (i=0;i < registersize; i++) {
        // longs in first "half"
        StoredTypeDescriptor const &typeDesc = typeFactory.newDataType(STANDARD_TYPE_INT_32);
        tupleDesc.push_back(TupleAttributeDescriptor(typeDesc, isNullable));
    }
    for (i=0;i < registersize; i++) {
        // booleans in second "half"
        StoredTypeDescriptor const &typeDesc = typeFactory.newDataType(STANDARD_TYPE_UINT_8);
        tupleDesc.push_back(TupleAttributeDescriptor(typeDesc, isNullable));
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
    tupleAccessorFixedLiteral.compute(tupleDesc, TUPLE_FORMAT_ALL_NOT_NULL_AND_FIXED);
    tupleAccessorFixedInput.compute(tupleDesc, TUPLE_FORMAT_ALL_NOT_NULL_AND_FIXED);
    tupleAccessorFixedOutput.compute(tupleDesc, TUPLE_FORMAT_ALL_NOT_NULL_AND_FIXED);
    tupleAccessorFixedLocal.compute(tupleDesc, TUPLE_FORMAT_ALL_NOT_NULL_AND_FIXED);
    tupleAccessorFixedStatus.compute(tupleDesc, TUPLE_FORMAT_ALL_NOT_NULL_AND_FIXED);

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
    tupleAccessorFixedLiteral.setCurrentTupleBuf(pTupleBufFixedLiteral.get());
    tupleAccessorFixedInput.setCurrentTupleBuf(pTupleBufFixedInput.get());
    tupleAccessorFixedOutput.setCurrentTupleBuf(pTupleBufFixedOutput.get());
    tupleAccessorFixedLocal.setCurrentTupleBuf(pTupleBufFixedLocal.get());
    tupleAccessorFixedStatus.setCurrentTupleBuf(pTupleBufFixedStatus.get());

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

    TupleData::iterator itr = literal.begin();
    for(i=0; i < registersize; i++, itr++) {
        // set up some nice literals for tests
        if (i % 2) {
            *(reinterpret_cast<int32_t *>(const_cast<PBuffer>(itr->pData))) = i * -1;
        } else {
            *(reinterpret_cast<int32_t *>(const_cast<PBuffer>(itr->pData))) = i;
        }
    }
    itr = input.begin();
    for(i=0; i < registersize; i++, itr++) {
        *(reinterpret_cast<int32_t *>(const_cast<PBuffer>(itr->pData))) = -1;
    }
    itr = output.begin();
    for(i=0; i < registersize; i++, itr++) {
        *(reinterpret_cast<int32_t *>(const_cast<PBuffer>(itr->pData))) = -1;
    }
    itr = local.begin();
    for(i=0; i < registersize; i++, itr++) {
        *(reinterpret_cast<int32_t *>(const_cast<PBuffer>(itr->pData))) = -1;
    }

    // set up boolean literals
    int falseIdx = 0;
    int trueIdx = 1;
    *(reinterpret_cast<bool *>
      (const_cast<PBuffer>
       (literal[trueIdx+registersize].pData))) = true;
    *(reinterpret_cast<bool *>
      (const_cast<PBuffer>
       (literal[falseIdx+registersize].pData))) = false;

  
    // null out last element of each type
    int nullidx = registersize - 1;
    literal[nullidx].pData = NULL;
    input[nullidx].pData = NULL;
    output[nullidx].pData = NULL;
    local[nullidx].pData = NULL;

    // also make a null in the boolean part of the literal set
    int boolnullidx = (2 * registersize) - 1;
    literal[boolnullidx].pData = NULL;

    // Print out the nullable tuple
    TuplePrinter tuplePrinter;
    printf("Literals\n");
    tuplePrinter.print(cout, tupleDesc, literal);
    printf("\nInput\n");
    tuplePrinter.print(cout, tupleDesc, input);
    cout << endl;
    printf("\nOutput\n");
    tuplePrinter.print(cout, tupleDesc, output);
    cout << endl;
    printf("\nLocal\n");
    tuplePrinter.print(cout, tupleDesc, local);
    cout << endl;


    // predefine register references. a real compiler wouldn't do
    // something so regular and pre-determined. a compiler would
    // probably build these on the fly as it built each instruction.
    // predefine register references. a real compiler wouldn't do
    // something so regular and pre-determined
    RegisterRef<int32_t> **bInP, **bOutP, **bLoP, **bLiP;
    RegisterRef<bool> **bOutBoolP, **bLiteralBoolP;
    bInP = new RegisterRef<int32_t>*[registersize];
    bOutP = new RegisterRef<int32_t>*[registersize];
    bLoP = new RegisterRef<int32_t>*[registersize];
    bLiP = new RegisterRef<int32_t>*[registersize];
    bOutBoolP = new RegisterRef<bool>*[registersize];
    bLiteralBoolP = new RegisterRef<bool>*[registersize];

    // Set up the Calculator
    Calculator c(0,0,0,0,0,0);
    c.outputRegisterByReference(false);

    // set up register references to symbolically point to 
    // their corresponding storage locations -- makes for easy test case
    // generation. again, a compiler wouldn't do things in quite
    // this way 
    for (i=0; i < registersize; i++) {
        bInP[i] = new RegisterRef<int32_t>(RegisterReference::EInput,
                                           i,
                                           STANDARD_TYPE_INT_32);
        c.appendRegRef(bInP[i]);
        bOutP[i] = new RegisterRef<int32_t>(RegisterReference::EOutput,
                                            i,
                                            STANDARD_TYPE_INT_32);
        c.appendRegRef(bOutP[i]);
        bLoP[i] = new RegisterRef<int32_t>(RegisterReference::ELocal,
                                           i,
                                           STANDARD_TYPE_INT_32);
        c.appendRegRef(bLoP[i]);
        bLiP[i] = new RegisterRef<int32_t>(RegisterReference::ELiteral,
                                           i,
                                           STANDARD_TYPE_INT_32);
        c.appendRegRef(bLiP[i]);
        bOutBoolP[i] = new RegisterRef<bool>(RegisterReference::EOutput,
                                             i+registersize,
                                             STANDARD_TYPE_BOOL);
        c.appendRegRef(bOutBoolP[i]);
        bLiteralBoolP[i] = new RegisterRef<bool>(RegisterReference::ELiteral,
                                                 i+registersize,
                                                 STANDARD_TYPE_BOOL);
        
        c.appendRegRef(bLiteralBoolP[i]);
    }

    // Set up storage for instructions
    // a real compiler would probably cons up instructions and insert them
    // directly into the calculator. keep an array of the instructions at
    // this level to allow printing of the program after execution, and other
    // debugging
    Instruction **instP;
    instP = new (Instruction *)[200];
    int pc=0, outC= 0, outBoolC = 0;

    StandardTypeDescriptorOrdinal isLong = STANDARD_TYPE_INT_32;

    // add
    instP[pc++] = new NativeAdd<int32_t>(bOutP[outC++], bLiP[10], bLiP[10], isLong);
    instP[pc++] = new NativeAdd<int32_t>(bOutP[outC++], bLiP[10], bLiP[9], isLong);
    instP[pc++] = new NativeAdd<int32_t>(bOutP[outC++], bLiP[nullidx], bLiP[9], isLong);
    instP[pc++] = new NativeAdd<int32_t>(bOutP[outC++], bLiP[10], bLiP[nullidx], isLong);
    instP[pc++] = new NativeAdd<int32_t>(bOutP[outC++], bLiP[nullidx], bLiP[nullidx], isLong);

    // sub
    instP[pc++] = new NativeSub<int32_t>(bOutP[outC++], bLiP[10], bLiP[9], isLong);
    instP[pc++] = new NativeSub<int32_t>(bOutP[outC++], bLiP[10], bLiP[10], isLong);
    instP[pc++] = new NativeSub<int32_t>(bOutP[outC++], bLiP[nullidx], bLiP[10], isLong);
    instP[pc++] = new NativeSub<int32_t>(bOutP[outC++], bLiP[10], bLiP[nullidx], isLong);
    instP[pc++] = new NativeSub<int32_t>(bOutP[outC++], bLiP[nullidx], bLiP[nullidx], isLong);

    // mul
    instP[pc++] = new NativeMul<int32_t>(bOutP[outC++], bLiP[4], bLiP[6], isLong);
    instP[pc++] = new NativeMul<int32_t>(bOutP[outC++], bLiP[4], bLiP[5], isLong);

    instP[pc++] = new NativeMul<int32_t>(bOutP[outC++], bLiP[nullidx], bLiP[5], isLong);
    instP[pc++] = new NativeMul<int32_t>(bOutP[outC++], bLiP[4], bLiP[nullidx], isLong);
    instP[pc++] = new NativeMul<int32_t>(bOutP[outC++], bLiP[nullidx], bLiP[nullidx], isLong);

    // div
    instP[pc++] = new NativeDiv<int32_t>(bOutP[outC++], bLiP[12], bLiP[4], isLong);
    instP[pc++] = new NativeDiv<int32_t>(bOutP[outC++], bLiP[12], bLiP[3], isLong);
    instP[pc++] = new NativeDiv<int32_t>(bOutP[outC++], bLiP[12], bLiP[nullidx], isLong);
    instP[pc++] = new NativeDiv<int32_t>(bOutP[outC++], bLiP[nullidx], bLiP[3], isLong);
    instP[pc++] = new NativeDiv<int32_t>(bOutP[outC++], bLiP[nullidx], bLiP[nullidx], isLong);
    // div by zero
    int divbyzero = pc;
    instP[pc++] = new NativeDiv<int32_t>(bOutP[outC++], bLiP[4], bLiP[0], isLong);

    // neg
    instP[pc++] = new NativeNeg<int32_t>(bOutP[outC++], bLiP[3], isLong);
    instP[pc++] = new NativeNeg<int32_t>(bOutP[outC++], bLiP[6], isLong);
    instP[pc++] = new NativeNeg<int32_t>(bOutP[outC++], bLiP[nullidx], isLong);

    // move
    instP[pc++] = new NativeMove<int32_t>(bOutP[outC++], bLiP[3], isLong);
    instP[pc++] = new NativeMove<int32_t>(bOutP[outC++], bLiP[6], isLong);
    instP[pc++] = new NativeMove<int32_t>(bOutP[outC++], bLiP[nullidx], isLong);

    // mod
    instP[pc++] = new IntegralNativeMod<int32_t>(bOutP[outC++], bLiP[20], bLiP[4], isLong);
    instP[pc++] = new IntegralNativeMod<int32_t>(bOutP[outC++], bLiP[20], bLiP[6], isLong);
    instP[pc++] = new IntegralNativeMod<int32_t>(bOutP[outC++], bLiP[20], bLiP[5], isLong);
    instP[pc++] = new IntegralNativeMod<int32_t>(bOutP[outC++], bLiP[20], bLiP[7], isLong);
    instP[pc++] = new IntegralNativeMod<int32_t>(bOutP[outC++], bLiP[19], bLiP[7], isLong);
    instP[pc++] = new IntegralNativeMod<int32_t>(bOutP[outC++], bLiP[19], bLiP[4], isLong);

    instP[pc++] = new IntegralNativeMod<int32_t>(bOutP[outC++], bLiP[12], bLiP[nullidx], isLong);
    instP[pc++] = new IntegralNativeMod<int32_t>(bOutP[outC++], bLiP[nullidx], bLiP[3], isLong);
    instP[pc++] = new IntegralNativeMod<int32_t>(bOutP[outC++], bLiP[nullidx], bLiP[nullidx], isLong);

    // mod by zero
    int modbyzero = pc;
    instP[pc++] = new IntegralNativeMod<int32_t>(bOutP[outC++], bLiP[3], bLiP[0], isLong);

    // bitwise and
    instP[pc++] = new IntegralNativeAnd<int32_t>(bOutP[outC++], bLiP[4], bLiP[4], isLong);
    instP[pc++] = new IntegralNativeAnd<int32_t>(bOutP[outC++], bLiP[30], bLiP[4], isLong);
    instP[pc++] = new IntegralNativeAnd<int32_t>(bOutP[outC++], bLiP[30], bLiP[6], isLong);
    instP[pc++] = new IntegralNativeAnd<int32_t>(bOutP[outC++], bLiP[30], bLiP[32], isLong);

    instP[pc++] = new IntegralNativeAnd<int32_t>(bOutP[outC++], bLiP[12], bLiP[nullidx], isLong);
    instP[pc++] = new IntegralNativeAnd<int32_t>(bOutP[outC++], bLiP[nullidx], bLiP[3], isLong);
    instP[pc++] = new IntegralNativeAnd<int32_t>(bOutP[outC++], bLiP[nullidx], bLiP[nullidx], isLong);

    // bitwise or
    instP[pc++] = new IntegralNativeOr<int32_t>(bOutP[outC++], bLiP[4], bLiP[4], isLong);
    instP[pc++] = new IntegralNativeOr<int32_t>(bOutP[outC++], bLiP[30], bLiP[64], isLong);
    instP[pc++] = new IntegralNativeOr<int32_t>(bOutP[outC++], bLiP[30], bLiP[0], isLong);
    instP[pc++] = new IntegralNativeOr<int32_t>(bOutP[outC++], bLiP[0], bLiP[0], isLong);

    instP[pc++] = new IntegralNativeOr<int32_t>(bOutP[outC++], bLiP[12], bLiP[nullidx], isLong);
    instP[pc++] = new IntegralNativeOr<int32_t>(bOutP[outC++], bLiP[nullidx], bLiP[3], isLong);
    instP[pc++] = new IntegralNativeOr<int32_t>(bOutP[outC++], bLiP[nullidx], bLiP[nullidx], isLong);

    // bitwise shift left
    instP[pc++] = new IntegralNativeShiftLeft<int32_t>(bOutP[outC++], bLiP[4], bLiP[2], isLong);
    instP[pc++] = new IntegralNativeShiftLeft<int32_t>(bOutP[outC++], bLiP[4], bLiP[0], isLong);

    instP[pc++] = new IntegralNativeShiftLeft<int32_t>(bOutP[outC++], bLiP[12], bLiP[nullidx], isLong);
    instP[pc++] = new IntegralNativeShiftLeft<int32_t>(bOutP[outC++], bLiP[nullidx], bLiP[3], isLong);
    instP[pc++] = new IntegralNativeShiftLeft<int32_t>(bOutP[outC++], bLiP[nullidx], bLiP[nullidx], isLong);

    // bitwise shift right
    instP[pc++] = new IntegralNativeShiftRight<int32_t>(bOutP[outC++], bLiP[4], bLiP[2], isLong);
    instP[pc++] = new IntegralNativeShiftRight<int32_t>(bOutP[outC++], bLiP[4], bLiP[0], isLong);

    instP[pc++] = new IntegralNativeShiftRight<int32_t>(bOutP[outC++], bLiP[12], bLiP[nullidx], isLong);
    instP[pc++] = new IntegralNativeShiftRight<int32_t>(bOutP[outC++], bLiP[nullidx], bLiP[3], isLong);
    instP[pc++] = new IntegralNativeShiftRight<int32_t>(bOutP[outC++], bLiP[nullidx], bLiP[nullidx], isLong);

    // equal
    instP[pc++] = new BoolNativeEqual<int32_t>(bOutBoolP[outBoolC++], bLiP[0], bLiP[0], isLong);
    instP[pc++] = new BoolNativeEqual<int32_t>(bOutBoolP[outBoolC++], bLiP[4], bLiP[4], isLong);
    instP[pc++] = new BoolNativeEqual<int32_t>(bOutBoolP[outBoolC++], bLiP[9], bLiP[9], isLong);
    instP[pc++] = new BoolNativeEqual<int32_t>(bOutBoolP[outBoolC++], bLiP[3], bLiP[5], isLong);
    instP[pc++] = new BoolNativeEqual<int32_t>(bOutBoolP[outBoolC++], bLiP[5], bLiP[3], isLong);
    instP[pc++] = new BoolNativeEqual<int32_t>(bOutBoolP[outBoolC++], bLiP[6], bLiP[2], isLong);
    instP[pc++] = new BoolNativeEqual<int32_t>(bOutBoolP[outBoolC++], bLiP[2], bLiP[6], isLong);

    instP[pc++] = new BoolNativeEqual<int32_t>(bOutBoolP[outBoolC++], bLiP[12], bLiP[nullidx], isLong);
    instP[pc++] = new BoolNativeEqual<int32_t>(bOutBoolP[outBoolC++], bLiP[nullidx], bLiP[3], isLong);
    instP[pc++] = new BoolNativeEqual<int32_t>(bOutBoolP[outBoolC++], bLiP[nullidx], bLiP[nullidx], isLong);

    // notequal
    instP[pc++] = new BoolNativeNotEqual<int32_t>(bOutBoolP[outBoolC++], bLiP[0], bLiP[0], isLong);
    instP[pc++] = new BoolNativeNotEqual<int32_t>(bOutBoolP[outBoolC++], bLiP[4], bLiP[4], isLong);
    instP[pc++] = new BoolNativeNotEqual<int32_t>(bOutBoolP[outBoolC++], bLiP[9], bLiP[9], isLong);
    instP[pc++] = new BoolNativeNotEqual<int32_t>(bOutBoolP[outBoolC++], bLiP[3], bLiP[5], isLong);
    instP[pc++] = new BoolNativeNotEqual<int32_t>(bOutBoolP[outBoolC++], bLiP[5], bLiP[3], isLong);
    instP[pc++] = new BoolNativeNotEqual<int32_t>(bOutBoolP[outBoolC++], bLiP[6], bLiP[2], isLong);
    instP[pc++] = new BoolNativeNotEqual<int32_t>(bOutBoolP[outBoolC++], bLiP[2], bLiP[6], isLong);

    instP[pc++] = new BoolNativeNotEqual<int32_t>(bOutBoolP[outBoolC++], bLiP[12], bLiP[nullidx], isLong);
    instP[pc++] = new BoolNativeNotEqual<int32_t>(bOutBoolP[outBoolC++], bLiP[nullidx], bLiP[3], isLong);
    instP[pc++] = new BoolNativeNotEqual<int32_t>(bOutBoolP[outBoolC++], bLiP[nullidx], bLiP[nullidx], isLong);

    // greater
    instP[pc++] = new BoolNativeGreater<int32_t>(bOutBoolP[outBoolC++], bLiP[0], bLiP[0], isLong);
    instP[pc++] = new BoolNativeGreater<int32_t>(bOutBoolP[outBoolC++], bLiP[4], bLiP[4], isLong);
    instP[pc++] = new BoolNativeGreater<int32_t>(bOutBoolP[outBoolC++], bLiP[9], bLiP[9], isLong);
    instP[pc++] = new BoolNativeGreater<int32_t>(bOutBoolP[outBoolC++], bLiP[3], bLiP[5], isLong);
    instP[pc++] = new BoolNativeGreater<int32_t>(bOutBoolP[outBoolC++], bLiP[5], bLiP[3], isLong);
    instP[pc++] = new BoolNativeGreater<int32_t>(bOutBoolP[outBoolC++], bLiP[6], bLiP[2], isLong);
    instP[pc++] = new BoolNativeGreater<int32_t>(bOutBoolP[outBoolC++], bLiP[2], bLiP[6], isLong);

    instP[pc++] = new BoolNativeGreater<int32_t>(bOutBoolP[outBoolC++], bLiP[12], bLiP[nullidx], isLong);
    instP[pc++] = new BoolNativeGreater<int32_t>(bOutBoolP[outBoolC++], bLiP[nullidx], bLiP[3], isLong);
    instP[pc++] = new BoolNativeGreater<int32_t>(bOutBoolP[outBoolC++], bLiP[nullidx], bLiP[nullidx], isLong);

    // greaterequal
    instP[pc++] = new BoolNativeGreaterEqual<int32_t>(bOutBoolP[outBoolC++], bLiP[0], bLiP[0], isLong);
    instP[pc++] = new BoolNativeGreaterEqual<int32_t>(bOutBoolP[outBoolC++], bLiP[4], bLiP[4], isLong);
    instP[pc++] = new BoolNativeGreaterEqual<int32_t>(bOutBoolP[outBoolC++], bLiP[9], bLiP[9], isLong);
    instP[pc++] = new BoolNativeGreaterEqual<int32_t>(bOutBoolP[outBoolC++], bLiP[3], bLiP[5], isLong);
    instP[pc++] = new BoolNativeGreaterEqual<int32_t>(bOutBoolP[outBoolC++], bLiP[5], bLiP[3], isLong);
    instP[pc++] = new BoolNativeGreaterEqual<int32_t>(bOutBoolP[outBoolC++], bLiP[6], bLiP[2], isLong);
    instP[pc++] = new BoolNativeGreaterEqual<int32_t>(bOutBoolP[outBoolC++], bLiP[2], bLiP[6], isLong);

    instP[pc++] = new BoolNativeGreaterEqual<int32_t>(bOutBoolP[outBoolC++], bLiP[12], bLiP[nullidx], isLong);
    instP[pc++] = new BoolNativeGreaterEqual<int32_t>(bOutBoolP[outBoolC++], bLiP[nullidx], bLiP[3], isLong);
    instP[pc++] = new BoolNativeGreaterEqual<int32_t>(bOutBoolP[outBoolC++], bLiP[nullidx], bLiP[nullidx], isLong);

    // less
    instP[pc++] = new BoolNativeLess<int32_t>(bOutBoolP[outBoolC++], bLiP[0], bLiP[0], isLong);
    instP[pc++] = new BoolNativeLess<int32_t>(bOutBoolP[outBoolC++], bLiP[4], bLiP[4], isLong);
    instP[pc++] = new BoolNativeLess<int32_t>(bOutBoolP[outBoolC++], bLiP[9], bLiP[9], isLong);
    instP[pc++] = new BoolNativeLess<int32_t>(bOutBoolP[outBoolC++], bLiP[3], bLiP[5], isLong);
    instP[pc++] = new BoolNativeLess<int32_t>(bOutBoolP[outBoolC++], bLiP[5], bLiP[3], isLong);
    instP[pc++] = new BoolNativeLess<int32_t>(bOutBoolP[outBoolC++], bLiP[6], bLiP[2], isLong);
    instP[pc++] = new BoolNativeLess<int32_t>(bOutBoolP[outBoolC++], bLiP[2], bLiP[6], isLong);

    instP[pc++] = new BoolNativeLess<int32_t>(bOutBoolP[outBoolC++], bLiP[12], bLiP[nullidx], isLong);
    instP[pc++] = new BoolNativeLess<int32_t>(bOutBoolP[outBoolC++], bLiP[nullidx], bLiP[3], isLong);
    instP[pc++] = new BoolNativeLess<int32_t>(bOutBoolP[outBoolC++], bLiP[nullidx], bLiP[nullidx], isLong);

    // lessequal
    instP[pc++] = new BoolNativeLessEqual<int32_t>(bOutBoolP[outBoolC++], bLiP[0], bLiP[0], isLong);
    instP[pc++] = new BoolNativeLessEqual<int32_t>(bOutBoolP[outBoolC++], bLiP[4], bLiP[4], isLong);
    instP[pc++] = new BoolNativeLessEqual<int32_t>(bOutBoolP[outBoolC++], bLiP[9], bLiP[9], isLong);
    instP[pc++] = new BoolNativeLessEqual<int32_t>(bOutBoolP[outBoolC++], bLiP[3], bLiP[5], isLong);
    instP[pc++] = new BoolNativeLessEqual<int32_t>(bOutBoolP[outBoolC++], bLiP[5], bLiP[3], isLong);
    instP[pc++] = new BoolNativeLessEqual<int32_t>(bOutBoolP[outBoolC++], bLiP[6], bLiP[2], isLong);
    instP[pc++] = new BoolNativeLessEqual<int32_t>(bOutBoolP[outBoolC++], bLiP[2], bLiP[6], isLong);

    instP[pc++] = new BoolNativeLessEqual<int32_t>(bOutBoolP[outBoolC++], bLiP[12], bLiP[nullidx], isLong);
    instP[pc++] = new BoolNativeLessEqual<int32_t>(bOutBoolP[outBoolC++], bLiP[nullidx], bLiP[3], isLong);
    instP[pc++] = new BoolNativeLessEqual<int32_t>(bOutBoolP[outBoolC++], bLiP[nullidx], bLiP[nullidx], isLong);
  
    // isnull
    instP[pc++] = new BoolNativeIsNull<int32_t>(bOutBoolP[outBoolC++], bLiP[12], isLong);
    instP[pc++] = new BoolNativeIsNull<int32_t>(bOutBoolP[outBoolC++], bLiP[nullidx], isLong); 
    // isnotnull
    instP[pc++] = new BoolNativeIsNotNull<int32_t>(bOutBoolP[outBoolC++], bLiP[12], isLong);
    instP[pc++] = new BoolNativeIsNotNull<int32_t>(bOutBoolP[outBoolC++], bLiP[nullidx], isLong);
    // tonull
    instP[pc++] = new NativeToNull<int32_t>(bOutP[outC++], isLong);

    // jump
    instP[pc++] = new NativeMove<int32_t>(bOutP[outC], bLiP[22], isLong);
    instP[pc] = new Jump(pc+2); pc++;
    instP[pc++] = new NativeMove<int32_t>(bOutP[outC++], bLiP[12], isLong);  // bad flag

    // jumptrue
    instP[pc++] = new NativeMove<int32_t>(bOutP[outC], bLiP[24], isLong);  // jump here good flag
    instP[pc] = new JumpTrue(pc+2, bLiteralBoolP[trueIdx]); pc++;          // jump over bad flag
    instP[pc++] = new NativeMove<int32_t>(bOutP[outC++], bLiP[14], isLong);// bad flag
    instP[pc] = new JumpTrue(pc+3, bLiteralBoolP[falseIdx]); pc++;         // won't jump to bad flag
    instP[pc++] = new NativeMove<int32_t>(bOutP[outC], bLiP[26], isLong);  // good flag
    instP[pc] = new Jump(pc+2); pc++;                                      // jump over bad flag
    instP[pc++] = new NativeMove<int32_t>(bOutP[outC++], bLiP[18], isLong);// bad flag
    instP[pc++] = new NativeMove<int32_t>(bOutP[outC++], bLiP[28], isLong);// good flag

    // jumpfalse
    instP[pc++] = new NativeMove<int32_t>(bOutP[outC], bLiP[34], isLong);  // good flag
    instP[pc] = new JumpFalse(pc+2, bLiteralBoolP[falseIdx]); pc++;        // jump over bad flag
    instP[pc++] = new NativeMove<int32_t>(bOutP[outC++], bLiP[14], isLong);// bad flag
    instP[pc] = new JumpFalse(pc+3, bLiteralBoolP[trueIdx]); pc++;         // won't jump to bad flag
    instP[pc++] = new NativeMove<int32_t>(bOutP[outC], bLiP[36], isLong);  // good flag
    instP[pc] = new Jump(pc+2); pc++;                                      // jump over bad flag
    instP[pc++] = new NativeMove<int32_t>(bOutP[outC++], bLiP[18], isLong);// bad flag
    instP[pc++] = new NativeMove<int32_t>(bOutP[outC++], bLiP[38], isLong);// good flag

    // jumpnull
    instP[pc++] = new NativeMove<int32_t>(bOutP[outC], bLiP[44], isLong);  // good flag
    instP[pc] = new JumpNull(pc+2, bLiteralBoolP[nullidx]); pc++;          // jump over bad flag
    instP[pc++] = new NativeMove<int32_t>(bOutP[outC++], bLiP[14], isLong);// bad flag
    instP[pc] = new JumpNull(pc+3, bLiteralBoolP[trueIdx]); pc++;          // won't jump to bad flag
    instP[pc++] = new NativeMove<int32_t>(bOutP[outC], bLiP[46], isLong);  // good flag
    instP[pc] = new Jump(pc+2); pc++;                                      // jump over bad flag
    instP[pc++] = new NativeMove<int32_t>(bOutP[outC++], bLiP[18], isLong);// bad flag
    instP[pc++] = new NativeMove<int32_t>(bOutP[outC++], bLiP[48], isLong);// good flag

    // jumpnotnull
    instP[pc++] = new NativeMove<int32_t>(bOutP[outC], bLiP[64], isLong);  // good flag
    instP[pc] = new JumpNotNull(pc+2, bLiteralBoolP[trueIdx]); pc++;       // jump over bad flag
    instP[pc++] = new NativeMove<int32_t>(bOutP[outC++], bLiP[14], isLong);// bad flag
    instP[pc] = new JumpNotNull(pc+3, bLiteralBoolP[nullidx]); pc++;       // won't jump to bad flag
    instP[pc++] = new NativeMove<int32_t>(bOutP[outC], bLiP[66], isLong);  // good flag
    instP[pc] = new Jump(pc+2); pc++;                                      // jump over bad flag
    instP[pc++] = new NativeMove<int32_t>(bOutP[outC++], bLiP[18], isLong);// bad flag
    instP[pc++] = new NativeMove<int32_t>(bOutP[outC++], bLiP[68], isLong);// good flag

    // return
    instP[pc++] = new NativeMove<int32_t>(bOutP[outC], bLiP[70], isLong);  // good flag
    instP[pc++] = new ReturnInstruction();
    instP[pc++] = new NativeMove<int32_t>(bOutP[outC++], bLiP[15], isLong);// bad flag
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

    string out;
    for (i = 0; i < pc; i++) {
        instP[i]->describe(out, true);
        printf("[%2d] %s\n", i, out.c_str());
    }

    // Print out the output tuple
    printf("Output Tuple\n");
    tuplePrinter.print(cout, tupleDesc, output);
    cout << endl;

    outC = 0;
    outBoolC = registersize;
    // TODO tests to add: Maxint, minint, zeros, negatives, overflow, underflow, etc
    // add
    if (*(reinterpret_cast<const int32_t *>(output[outC++].pData)) != 20) fail("longadd1", __LINE__);
    if (*(reinterpret_cast<const int32_t *>(output[outC++].pData)) != 1) fail("longadd2", __LINE__);
    if (output[outC++].pData != NULL) fail("longadd3", __LINE__);
    if (output[outC++].pData != NULL) fail("longadd4", __LINE__);
    if (output[outC++].pData != NULL) fail("longadd5", __LINE__);

    // sub
    if (*(reinterpret_cast<const int32_t *>(output[outC++].pData)) != 19) fail("longsub1", __LINE__);
    if (*(reinterpret_cast<const int32_t *>(output[outC++].pData)) != 0) fail("longsub2", __LINE__);
    if (output[outC++].pData != NULL) fail("longsub3", __LINE__);
    if (output[outC++].pData != NULL) fail("longsub4", __LINE__);
    if (output[outC++].pData != NULL) fail("longsub5", __LINE__);

    // mul
    if (*(reinterpret_cast<const int32_t *>(output[outC++].pData)) != 24) fail("longmul1", __LINE__);
    if (*(reinterpret_cast<const int32_t *>(output[outC++].pData)) != -20) fail("longmul2", __LINE__);
    if (output[outC++].pData != NULL) fail("longmul3", __LINE__);
    if (output[outC++].pData != NULL) fail("longmul4", __LINE__);
    if (output[outC++].pData != NULL) fail("longmul5", __LINE__);

    // div
    if (*(reinterpret_cast<const int32_t *>(output[outC++].pData)) != 3) fail("longdiv1", __LINE__);
    if (*(reinterpret_cast<const int32_t *>(output[outC++].pData)) != -4) fail("longdiv2", __LINE__);
    if (output[outC++].pData != NULL) fail("longdiv3", __LINE__);
    if (output[outC++].pData != NULL) fail("longdiv4", __LINE__);
    if (output[outC++].pData != NULL) fail("longdiv5", __LINE__);
    // div by zero 
    assert(outC == divbyzero);
    if (output[outC++].pData != NULL) fail("longdiv6", __LINE__);
    deque<CalcMessage>::iterator iter = c.mWarnings.begin();
    if (iter->pc != divbyzero)
        fail("longdiv by zero failed, pc wrong\n", __LINE__);
    string expectederror("22012");
    if (expectederror.compare(iter->str)) 
        fail("longdiv by zero failed string was wrong", __LINE__);

    // neg
    if (*(reinterpret_cast<const int32_t *>(output[outC++].pData)) != 3) fail("longneg1", __LINE__);
    if (*(reinterpret_cast<const int32_t *>(output[outC++].pData)) != -6) fail("longneg2", __LINE__);
    if (output[outC++].pData != NULL) fail("longneg3", __LINE__);

    // move
    if (*(reinterpret_cast<const int32_t *>(output[outC++].pData)) != -3) fail("longmove1", __LINE__);
    if (*(reinterpret_cast<const int32_t *>(output[outC++].pData)) != 6) fail("longmove2", __LINE__);
    if (output[outC++].pData != NULL) fail("longmove3", __LINE__);


    // mod
    if (*(reinterpret_cast<const int32_t *>(output[outC++].pData)) != 0) fail("longmod1", __LINE__);
    if (*(reinterpret_cast<const int32_t *>(output[outC++].pData)) != 2) fail("longmod2", __LINE__);
    if (*(reinterpret_cast<const int32_t *>(output[outC++].pData)) != 0) fail("longmod3", __LINE__);
    if (*(reinterpret_cast<const int32_t *>(output[outC++].pData)) != 6) fail("longmod4", __LINE__);
    if (*(reinterpret_cast<const int32_t *>(output[outC++].pData)) != -5) fail("longmod5", __LINE__);
    if (*(reinterpret_cast<const int32_t *>(output[outC++].pData)) != -3) fail("longmod6", __LINE__);

    if (output[outC++].pData != NULL) fail("longmod7", __LINE__);
    if (output[outC++].pData != NULL) fail("longmod8", __LINE__);
    if (output[outC++].pData != NULL) fail("longmod9", __LINE__);
  
    // mod by zero
    assert(outC == modbyzero);
    if (output[outC++].pData != NULL) fail("longmod10", __LINE__);
    iter++;
    if (iter->pc != modbyzero)
        fail("longmod by zero failed, pc wrong\n", __LINE__);
    expectederror = "22012";
    if (expectederror.compare(iter->str)) 
        fail("longmod by zero failed string was wrong", __LINE__);

    // bitwise and
    if (*(reinterpret_cast<const int32_t *>(output[outC++].pData)) != 4) fail("longbitand1", __LINE__);
    if (*(reinterpret_cast<const int32_t *>(output[outC++].pData)) != 4) fail("longbitand2", __LINE__);
    if (*(reinterpret_cast<const int32_t *>(output[outC++].pData)) != 6) fail("longbitand3", __LINE__);
    if (*(reinterpret_cast<const int32_t *>(output[outC++].pData)) != 0) fail("longbitand4", __LINE__);

    if (output[outC++].pData != NULL) fail("longbitand5", __LINE__);
    if (output[outC++].pData != NULL) fail("longbitand6", __LINE__);
    if (output[outC++].pData != NULL) fail("longbitand7", __LINE__);

    // bitwise or
    if (*(reinterpret_cast<const int32_t *>(output[outC++].pData)) != 4) fail("longbitor1", __LINE__);
    if (*(reinterpret_cast<const int32_t *>(output[outC++].pData)) != 94) fail("longbitor2", __LINE__);
    if (*(reinterpret_cast<const int32_t *>(output[outC++].pData)) != 30) fail("longbitor3", __LINE__);
    if (*(reinterpret_cast<const int32_t *>(output[outC++].pData)) != 0) fail("longbitor4", __LINE__);

    if (output[outC++].pData != NULL) fail("longbitor5", __LINE__);
    if (output[outC++].pData != NULL) fail("longbitor6", __LINE__);
    if (output[outC++].pData != NULL) fail("longbitor7", __LINE__);

    // bitwise shift left
    if (*(reinterpret_cast<const int32_t *>(output[outC++].pData)) != 16) fail("longbitshiftleft1", __LINE__);
    if (*(reinterpret_cast<const int32_t *>(output[outC++].pData)) != 4) fail("longbitshiftleft2", __LINE__);

    if (output[outC++].pData != NULL) fail("longbitshiftleft5", __LINE__);
    if (output[outC++].pData != NULL) fail("longbitshiftleft6", __LINE__);
    if (output[outC++].pData != NULL) fail("longbitshiftleft7", __LINE__);

    // bitwise shift right
    if (*(reinterpret_cast<const int32_t *>(output[outC++].pData)) != 1) fail("longbitshiftright1", __LINE__);
    if (*(reinterpret_cast<const int32_t *>(output[outC++].pData)) != 4) fail("longbitshiftright2", __LINE__);

    if (output[outC++].pData != NULL) fail("longbitshiftright5", __LINE__);
    if (output[outC++].pData != NULL) fail("longbitshiftright6", __LINE__);
    if (output[outC++].pData != NULL) fail("longbitshiftright7", __LINE__);

    // equal
    if (*(output[outBoolC++].pData) != true) fail("longequal1", __LINE__);
    if (*(output[outBoolC++].pData) != true) fail("longequal2", __LINE__);
    if (*(output[outBoolC++].pData) != true) fail("longequal3", __LINE__);
    if (*(output[outBoolC++].pData) != false) fail("longequal4", __LINE__);
    if (*(output[outBoolC++].pData) != false) fail("longequal5", __LINE__);
    if (*(output[outBoolC++].pData) != false) fail("longequal6", __LINE__);
    if (*(output[outBoolC++].pData) != false) fail("longequal7", __LINE__);

    if (output[outBoolC++].pData != NULL) fail("longequal8", __LINE__);
    if (output[outBoolC++].pData != NULL) fail("longequal9", __LINE__);
    if (output[outBoolC++].pData != NULL) fail("longequal10", __LINE__);

    // notequal
    if (*(output[outBoolC++].pData) != false) fail("longnotequal1", __LINE__);
    if (*(output[outBoolC++].pData) != false) fail("longnotequal2", __LINE__);
    if (*(output[outBoolC++].pData) != false) fail("longnotequal3", __LINE__);
    if (*(output[outBoolC++].pData) != true) fail("longnotequal4", __LINE__);
    if (*(output[outBoolC++].pData) != true) fail("longnotequal5", __LINE__);
    if (*(output[outBoolC++].pData) != true) fail("longnotequal6", __LINE__);
    if (*(output[outBoolC++].pData) != true) fail("longnotequal7", __LINE__);

    if (output[outBoolC++].pData != NULL) fail("longnotequal8", __LINE__);
    if (output[outBoolC++].pData != NULL) fail("longnotequal9", __LINE__);
    if (output[outBoolC++].pData != NULL) fail("longnotequal10", __LINE__);

    // greater
    if (*(output[outBoolC++].pData) != false) fail("longgreater1", __LINE__);
    if (*(output[outBoolC++].pData) != false) fail("longgreater2", __LINE__);
    if (*(output[outBoolC++].pData) != false) fail("longgreater3", __LINE__);
    if (*(output[outBoolC++].pData) != true) fail("longgreater4", __LINE__);
    if (*(output[outBoolC++].pData) != false) fail("longgreater5", __LINE__);
    if (*(output[outBoolC++].pData) != true) fail("longgreater6", __LINE__);
    if (*(output[outBoolC++].pData) != false) fail("longgreater7", __LINE__);

    if (output[outBoolC++].pData != NULL) fail("longgreater8", __LINE__);
    if (output[outBoolC++].pData != NULL) fail("longgreater9", __LINE__);
    if (output[outBoolC++].pData != NULL) fail("longgreater10", __LINE__);

    // greaterequal
    if (*(output[outBoolC++].pData) != true) fail("longgreaterequal1", __LINE__);
    if (*(output[outBoolC++].pData) != true) fail("longgreaterequal2", __LINE__);
    if (*(output[outBoolC++].pData) != true) fail("longgreaterequal3", __LINE__);
    if (*(output[outBoolC++].pData) != true) fail("longgreaterequal4", __LINE__);
    if (*(output[outBoolC++].pData) != false) fail("longgreaterequal5", __LINE__);
    if (*(output[outBoolC++].pData) != true) fail("longgreaterequal6", __LINE__);
    if (*(output[outBoolC++].pData) != false) fail("longgreaterequal7", __LINE__);

    if (output[outBoolC++].pData != NULL) fail("longgreaterequal8", __LINE__);
    if (output[outBoolC++].pData != NULL) fail("longgreaterequal9", __LINE__);
    if (output[outBoolC++].pData != NULL) fail("longgreaterequal10", __LINE__);

    // less
    if (*(output[outBoolC++].pData) != false) fail("longless1", __LINE__);
    if (*(output[outBoolC++].pData) != false) fail("longless2", __LINE__);
    if (*(output[outBoolC++].pData) != false) fail("longless3", __LINE__);
    if (*(output[outBoolC++].pData) != false) fail("longless4", __LINE__);
    if (*(output[outBoolC++].pData) != true) fail("longless5", __LINE__);
    if (*(output[outBoolC++].pData) != false) fail("longless6", __LINE__);
    if (*(output[outBoolC++].pData) != true) fail("longless7", __LINE__);

    if (output[outBoolC++].pData != NULL) fail("longless8", __LINE__);
    if (output[outBoolC++].pData != NULL) fail("longless9", __LINE__);
    if (output[outBoolC++].pData != NULL) fail("longless10", __LINE__);

    // lessequal
    if (*(output[outBoolC++].pData) != true) fail("longlessequal1", __LINE__);
    if (*(output[outBoolC++].pData) != true) fail("longlessequal2", __LINE__);
    if (*(output[outBoolC++].pData) != true) fail("longlessequal3", __LINE__);
    if (*(output[outBoolC++].pData) != false) fail("longlessequal4", __LINE__);
    if (*(output[outBoolC++].pData) != true) fail("longlessequal5", __LINE__);
    if (*(output[outBoolC++].pData) != false) fail("longlessequal6", __LINE__);
    if (*(output[outBoolC++].pData) != true) fail("longlessequal7", __LINE__);

    if (output[outBoolC++].pData != NULL) fail("longlessequal8", __LINE__);
    if (output[outBoolC++].pData != NULL) fail("longlessequal9", __LINE__);
    if (output[outBoolC++].pData != NULL) fail("longlessequal10", __LINE__);

    // isnull
    if (*(output[outBoolC++].pData) != false) fail("longisnull1", __LINE__);
    if (*(output[outBoolC++].pData) != true) fail("longisnull2", __LINE__);

    // isnotnull
    if (*(output[outBoolC++].pData) != true) fail("longisnotnull1", __LINE__);
    if (*(output[outBoolC++].pData) != false) fail("longisnotnull2", __LINE__);

    // tonull
    if (output[outC++].pData != NULL) fail("longtonull1", __LINE__);

    // jump
    if (*(reinterpret_cast<const int32_t *>(output[outC++].pData)) != 22) fail("longjump1", __LINE__);

    // jumptrue
    if (*(reinterpret_cast<const int32_t *>(output[outC++].pData)) != 24) fail("longjumptrue1", __LINE__);
    if (*(reinterpret_cast<const int32_t *>(output[outC++].pData)) != 26) fail("longjumptrue2", __LINE__);
    if (*(reinterpret_cast<const int32_t *>(output[outC++].pData)) != 28) fail("longjumptrue3", __LINE__);

    // jumpfalse
    if (*(reinterpret_cast<const int32_t *>(output[outC++].pData)) != 34) fail("longjumpfalse1", __LINE__);
    if (*(reinterpret_cast<const int32_t *>(output[outC++].pData)) != 36) fail("longjumpfalse2", __LINE__);
    if (*(reinterpret_cast<const int32_t *>(output[outC++].pData)) != 38) fail("longjumpfalse3", __LINE__);

    // jumpnull
    if (*(reinterpret_cast<const int32_t *>(output[outC++].pData)) != 44) fail("longjumpnull1", __LINE__);
    if (*(reinterpret_cast<const int32_t *>(output[outC++].pData)) != 46) fail("longjumpnull2", __LINE__);
    if (*(reinterpret_cast<const int32_t *>(output[outC++].pData)) != 48) fail("longjumpnull3", __LINE__);

    // jumpnotnull
    if (*(reinterpret_cast<const int32_t *>(output[outC++].pData)) != 64) fail("longjumpnotnull1", __LINE__);
    if (*(reinterpret_cast<const int32_t *>(output[outC++].pData)) != 66) fail("longjumpnotnull2", __LINE__);
    if (*(reinterpret_cast<const int32_t *>(output[outC++].pData)) != 68) fail("longjumpnotnull3", __LINE__);

    // return
    if (*(reinterpret_cast<const int32_t *>(output[outC++].pData)) != 70) fail("longreturn", __LINE__);

    cout << "Calculator Warnings: " << c.warnings() << endl;

    delete [] bInP;
    delete [] bOutP;
    delete [] bLoP;
    delete [] bLiP;
    delete [] bOutBoolP;
    delete [] bLiteralBoolP;
    for (i = 0; i < lastPC; i++) {
        delete instP[i];
    }
    delete [] instP;
}


void
unitTestFloat()
{
    printf("=========================================================\n");
    printf("=========================================================\n");
    printf("=====\n");
    printf("=====     unitTestFloat()\n");
    printf("=====\n");
    printf("=========================================================\n");
    printf("=========================================================\n");

    bool isNullable = true;    // Can tuple contain nulls?
    int i, registersize = 200;

    TupleDescriptor tupleDesc;
    tupleDesc.clear();

    // Build up a description of what we'd like the tuple to look like
    StandardTypeDescriptorFactory typeFactory;
    for (i=0;i < registersize; i++) {
        // float in first "half"
        StoredTypeDescriptor const &typeDesc = typeFactory.newDataType(STANDARD_TYPE_REAL);
        tupleDesc.push_back(TupleAttributeDescriptor(typeDesc, isNullable));
    }
    for (i=0;i < registersize; i++) {
        // booleans in second "half"
        StoredTypeDescriptor const &typeDesc = typeFactory.newDataType(STANDARD_TYPE_UINT_8);
        tupleDesc.push_back(TupleAttributeDescriptor(typeDesc, isNullable));
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
    tupleAccessorFixedLiteral.compute(tupleDesc, TUPLE_FORMAT_ALL_NOT_NULL_AND_FIXED);
    tupleAccessorFixedInput.compute(tupleDesc, TUPLE_FORMAT_ALL_NOT_NULL_AND_FIXED);
    tupleAccessorFixedOutput.compute(tupleDesc, TUPLE_FORMAT_ALL_NOT_NULL_AND_FIXED);
    tupleAccessorFixedLocal.compute(tupleDesc, TUPLE_FORMAT_ALL_NOT_NULL_AND_FIXED);
    tupleAccessorFixedStatus.compute(tupleDesc, TUPLE_FORMAT_ALL_NOT_NULL_AND_FIXED);

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
    tupleAccessorFixedLiteral.setCurrentTupleBuf(pTupleBufFixedLiteral.get());
    tupleAccessorFixedInput.setCurrentTupleBuf(pTupleBufFixedInput.get());
    tupleAccessorFixedOutput.setCurrentTupleBuf(pTupleBufFixedOutput.get());
    tupleAccessorFixedLocal.setCurrentTupleBuf(pTupleBufFixedLocal.get());
    tupleAccessorFixedStatus.setCurrentTupleBuf(pTupleBufFixedStatus.get());

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

    TupleData::iterator itr = tupleDataFixedLiteral.begin();
    int neg = registersize / 2;
    for(i=0; i < registersize; i++, itr++) {
        // set up some nice literals for tests
        if (i < neg) {
            *(reinterpret_cast<float *>(const_cast<PBuffer>(itr->pData))) = (float) i / 2;
        } else {
            *(reinterpret_cast<float *>(const_cast<PBuffer>(itr->pData))) = (float) (i - neg) / -2;
        }
    }
    itr = tupleDataFixedInput.begin();
    for(i=0; i < registersize; i++, itr++) {
        *(reinterpret_cast<float *>(const_cast<PBuffer>(itr->pData))) = -1;
    }
    itr = tupleDataFixedOutput.begin();
    for(i=0; i < registersize; i++, itr++) {
        *(reinterpret_cast<float *>(const_cast<PBuffer>(itr->pData))) = -1;
    }
    itr = tupleDataFixedLocal.begin();
    for(i=0; i < registersize; i++, itr++) {
        *(reinterpret_cast<float *>(const_cast<PBuffer>(itr->pData))) = -1;
    }

    // set up boolean literals
    int falseIdx = 0;
    int trueIdx = 1;
    *(reinterpret_cast<bool *>
      (const_cast<PBuffer>
       (tupleDataFixedLiteral[trueIdx+registersize].pData))) = true;
    *(reinterpret_cast<bool *>
      (const_cast<PBuffer>
       (tupleDataFixedLiteral[falseIdx+registersize].pData))) = false;

    // Create another TupleData object that will be nullable
    TupleData literal = tupleDataFixedLiteral;
    TupleData input = tupleDataFixedInput;
    TupleData output = tupleDataFixedOutput;
    TupleData local = tupleDataFixedLocal;
    TupleData status = tupleDataFixedStatus;
  
    // null out last element of each type
    int nullidx = registersize - 1;
    literal[nullidx].pData = NULL;
    input[nullidx].pData = NULL;
    output[nullidx].pData = NULL;
    local[nullidx].pData = NULL;

    // also make a null in the boolean part of the literal set
    int boolnullidx = (2 * registersize) - 1;
    literal[boolnullidx].pData = NULL;

    // Print out the nullable tuple
    TuplePrinter tuplePrinter;
    printf("Literals\n");
    tuplePrinter.print(cout, tupleDesc, literal);
    printf("\nInput\n");
    tuplePrinter.print(cout, tupleDesc, input);
    cout << endl;
    printf("\nOutput\n");
    tuplePrinter.print(cout, tupleDesc, output);
    cout << endl;
    printf("\nLocal\n");
    tuplePrinter.print(cout, tupleDesc, local);
    cout << endl;

    // predefine register references. a real compiler wouldn't do
    // something so regular and pre-determined. a compiler would
    // probably build these on the fly as it built each instruction.
    // predefine register references. a real compiler wouldn't do
    // something so regular and pre-determined
    RegisterRef<float> **fInP, **fOutP, **fLoP, **fLiP;
    RegisterRef<bool> **bOutP;
    
    fInP = new RegisterRef<float>*[registersize];
    fOutP = new RegisterRef<float>*[registersize];
    fLoP = new RegisterRef<float>*[registersize];
    fLiP = new RegisterRef<float>*[registersize];
    bOutP = new RegisterRef<bool>*[registersize];

    // Set up the Calculator
    Calculator c(0,0,0,0,0,0);
    c.outputRegisterByReference(false);

    // set up register references to symbolically point to 
    // their corresponding storage locations -- makes for easy test case
    // generation. again, a compiler wouldn't do things in quite
    // this way 
    for (i=0; i < registersize; i++) {
        fInP[i] = new RegisterRef<float>(RegisterReference::EInput,
                                         i, 
                                         STANDARD_TYPE_REAL);
        c.appendRegRef(fInP[i]);
        fOutP[i] = new RegisterRef<float>(RegisterReference::EOutput,
                                          i,
                                          STANDARD_TYPE_REAL);
        c.appendRegRef(fOutP[i]);
        fLoP[i] = new RegisterRef<float>(RegisterReference::ELocal, 
                                         i,
                                         STANDARD_TYPE_REAL);
        c.appendRegRef(fLoP[i]);
        fLiP[i] = new RegisterRef<float>(RegisterReference::ELiteral,
                                         i,
                                         STANDARD_TYPE_REAL);
        c.appendRegRef(fLiP[i]);

        bOutP[i] = new RegisterRef<bool>(RegisterReference::EOutput,
                                         i+registersize,
                                         STANDARD_TYPE_BOOL);
        c.appendRegRef(bOutP[i]);
    }


    // Set up storage for instructions
    // a real compiler would probably cons up instructions and insert them
    // directly into the calculator. keep an array of the instructions at
    // this level to allow printing of the program after execution, and other
    // debugging
    Instruction **instP;
    instP = new (Instruction *)[200];
    int pc=0, outC= 0, outBoolC = 0;

    StandardTypeDescriptorOrdinal isFloat = STANDARD_TYPE_REAL;

    // add
    instP[pc++] = new NativeAdd<float>(fOutP[outC++], fLiP[10], fLiP[10], isFloat);
    instP[pc++] = new NativeAdd<float>(fOutP[outC++], fLiP[10], fLiP[9], isFloat);
    instP[pc++] = new NativeAdd<float>(fOutP[outC++], fLiP[0], fLiP[0], isFloat);
    instP[pc++] = new NativeAdd<float>(fOutP[outC++], fLiP[neg], fLiP[neg], isFloat); // -0 + -0
    instP[pc++] = new NativeAdd<float>(fOutP[outC++], fLiP[neg+1], fLiP[neg+2], isFloat); 

    instP[pc++] = new NativeAdd<float>(fOutP[outC++], fLiP[nullidx], fLiP[9], isFloat);
    instP[pc++] = new NativeAdd<float>(fOutP[outC++], fLiP[10], fLiP[nullidx], isFloat);
    instP[pc++] = new NativeAdd<float>(fOutP[outC++], fLiP[nullidx], fLiP[nullidx], isFloat);

    // sub
    instP[pc++] = new NativeSub<float>(fOutP[outC++], fLiP[10], fLiP[9], isFloat);
    instP[pc++] = new NativeSub<float>(fOutP[outC++], fLiP[10], fLiP[10], isFloat);
    instP[pc++] = new NativeSub<float>(fOutP[outC++], fLiP[9], fLiP[0], isFloat);
    instP[pc++] = new NativeSub<float>(fOutP[outC++], fLiP[0], fLiP[0], isFloat);
    instP[pc++] = new NativeSub<float>(fOutP[outC++], fLiP[neg], fLiP[neg], isFloat);
    instP[pc++] = new NativeSub<float>(fOutP[outC++], fLiP[neg+4], fLiP[neg+1], isFloat);

    instP[pc++] = new NativeSub<float>(fOutP[outC++], fLiP[nullidx], fLiP[10], isFloat);
    instP[pc++] = new NativeSub<float>(fOutP[outC++], fLiP[10], fLiP[nullidx], isFloat);
    instP[pc++] = new NativeSub<float>(fOutP[outC++], fLiP[nullidx], fLiP[nullidx], isFloat);

    // mul
    instP[pc++] = new NativeMul<float>(fOutP[outC++], fLiP[4], fLiP[6], isFloat);
    instP[pc++] = new NativeMul<float>(fOutP[outC++], fLiP[5], fLiP[5], isFloat);
    instP[pc++] = new NativeMul<float>(fOutP[outC++], fLiP[0], fLiP[0], isFloat);
    instP[pc++] = new NativeMul<float>(fOutP[outC++], fLiP[neg], fLiP[neg], isFloat);
    instP[pc++] = new NativeMul<float>(fOutP[outC++], fLiP[6], fLiP[neg], isFloat);
    instP[pc++] = new NativeMul<float>(fOutP[outC++], fLiP[6], fLiP[0], isFloat);
    instP[pc++] = new NativeMul<float>(fOutP[outC++], fLiP[neg+7], fLiP[2], isFloat);

    instP[pc++] = new NativeMul<float>(fOutP[outC++], fLiP[nullidx], fLiP[5], isFloat);
    instP[pc++] = new NativeMul<float>(fOutP[outC++], fLiP[4], fLiP[nullidx], isFloat);
    instP[pc++] = new NativeMul<float>(fOutP[outC++], fLiP[nullidx], fLiP[nullidx], isFloat);

    // div
    instP[pc++] = new NativeDiv<float>(fOutP[outC++], fLiP[12], fLiP[4], isFloat);
    instP[pc++] = new NativeDiv<float>(fOutP[outC++], fLiP[12], fLiP[3], isFloat);
    instP[pc++] = new NativeDiv<float>(fOutP[outC++], fLiP[0], fLiP[3], isFloat);
    instP[pc++] = new NativeDiv<float>(fOutP[outC++], fLiP[neg], fLiP[3], isFloat);
    instP[pc++] = new NativeDiv<float>(fOutP[outC++], fLiP[neg+9], fLiP[neg+2], isFloat);
    instP[pc++] = new NativeDiv<float>(fOutP[outC++], fLiP[neg+9], fLiP[1], isFloat);

    instP[pc++] = new NativeDiv<float>(fOutP[outC++], fLiP[12], fLiP[nullidx], isFloat);
    instP[pc++] = new NativeDiv<float>(fOutP[outC++], fLiP[nullidx], fLiP[3], isFloat);
    instP[pc++] = new NativeDiv<float>(fOutP[outC++], fLiP[nullidx], fLiP[nullidx], isFloat);
    // div by zero
    int divbyzero = pc;
    instP[pc++] = new NativeDiv<float>(fOutP[outC++], fLiP[4], fLiP[0], isFloat);
    instP[pc++] = new NativeDiv<float>(fOutP[outC++], fLiP[4], fLiP[neg], isFloat);

    // neg
    instP[pc++] = new NativeNeg<float>(fOutP[outC++], fLiP[3], isFloat);
    instP[pc++] = new NativeNeg<float>(fOutP[outC++], fLiP[neg+3], isFloat);
    instP[pc++] = new NativeNeg<float>(fOutP[outC++], fLiP[0], isFloat);
    instP[pc++] = new NativeNeg<float>(fOutP[outC++], fLiP[neg], isFloat);
    instP[pc++] = new NativeNeg<float>(fOutP[outC++], fLiP[nullidx], isFloat);

    // move
    instP[pc++] = new NativeMove<float>(fOutP[outC++], fLiP[3], isFloat);
    instP[pc++] = new NativeMove<float>(fOutP[outC++], fLiP[6], isFloat);
    instP[pc++] = new NativeMove<float>(fOutP[outC++], fLiP[0], isFloat);
    instP[pc++] = new NativeMove<float>(fOutP[outC++], fLiP[neg], isFloat);
    instP[pc++] = new NativeMove<float>(fOutP[outC++], fLiP[nullidx], isFloat);

    // equal
    instP[pc++] = new BoolNativeEqual<float>(bOutP[outBoolC++], fLiP[0], fLiP[0], isFloat);
    instP[pc++] = new BoolNativeEqual<float>(bOutP[outBoolC++], fLiP[neg], fLiP[neg], isFloat);
    instP[pc++] = new BoolNativeEqual<float>(bOutP[outBoolC++], fLiP[4], fLiP[4], isFloat);
    instP[pc++] = new BoolNativeEqual<float>(bOutP[outBoolC++], fLiP[9], fLiP[9], isFloat);
    instP[pc++] = new BoolNativeEqual<float>(bOutP[outBoolC++], fLiP[3], fLiP[5], isFloat);
    instP[pc++] = new BoolNativeEqual<float>(bOutP[outBoolC++], fLiP[5], fLiP[3], isFloat);
    instP[pc++] = new BoolNativeEqual<float>(bOutP[outBoolC++], fLiP[6], fLiP[2], isFloat);
    instP[pc++] = new BoolNativeEqual<float>(bOutP[outBoolC++], fLiP[2], fLiP[6], isFloat);
    instP[pc++] = new BoolNativeEqual<float>(bOutP[outBoolC++], fLiP[neg+5], fLiP[neg+5], isFloat);
    instP[pc++] = new BoolNativeEqual<float>(bOutP[outBoolC++], fLiP[neg+5], fLiP[neg+6], isFloat);

    instP[pc++] = new BoolNativeEqual<float>(bOutP[outBoolC++], fLiP[12], fLiP[nullidx], isFloat);
    instP[pc++] = new BoolNativeEqual<float>(bOutP[outBoolC++], fLiP[nullidx], fLiP[3], isFloat);
    instP[pc++] = new BoolNativeEqual<float>(bOutP[outBoolC++], fLiP[nullidx], fLiP[nullidx], isFloat);

    // notequal
    instP[pc++] = new BoolNativeNotEqual<float>(bOutP[outBoolC++], fLiP[0], fLiP[0], isFloat);
    instP[pc++] = new BoolNativeNotEqual<float>(bOutP[outBoolC++], fLiP[4], fLiP[4], isFloat);
    instP[pc++] = new BoolNativeNotEqual<float>(bOutP[outBoolC++], fLiP[9], fLiP[9], isFloat);
    instP[pc++] = new BoolNativeNotEqual<float>(bOutP[outBoolC++], fLiP[3], fLiP[5], isFloat);
    instP[pc++] = new BoolNativeNotEqual<float>(bOutP[outBoolC++], fLiP[5], fLiP[3], isFloat);
    instP[pc++] = new BoolNativeNotEqual<float>(bOutP[outBoolC++], fLiP[6], fLiP[2], isFloat);
    instP[pc++] = new BoolNativeNotEqual<float>(bOutP[outBoolC++], fLiP[2], fLiP[6], isFloat);

    instP[pc++] = new BoolNativeNotEqual<float>(bOutP[outBoolC++], fLiP[12], fLiP[nullidx], isFloat);
    instP[pc++] = new BoolNativeNotEqual<float>(bOutP[outBoolC++], fLiP[nullidx], fLiP[3], isFloat);
    instP[pc++] = new BoolNativeNotEqual<float>(bOutP[outBoolC++], fLiP[nullidx], fLiP[nullidx], isFloat);

    // greater
    instP[pc++] = new BoolNativeGreater<float>(bOutP[outBoolC++], fLiP[0], fLiP[0], isFloat);
    instP[pc++] = new BoolNativeGreater<float>(bOutP[outBoolC++], fLiP[4], fLiP[4], isFloat);
    instP[pc++] = new BoolNativeGreater<float>(bOutP[outBoolC++], fLiP[9], fLiP[9], isFloat);
    instP[pc++] = new BoolNativeGreater<float>(bOutP[outBoolC++], fLiP[3], fLiP[5], isFloat);
    instP[pc++] = new BoolNativeGreater<float>(bOutP[outBoolC++], fLiP[5], fLiP[3], isFloat);
    instP[pc++] = new BoolNativeGreater<float>(bOutP[outBoolC++], fLiP[neg+3], fLiP[neg+5], isFloat);
    instP[pc++] = new BoolNativeGreater<float>(bOutP[outBoolC++], fLiP[neg+5], fLiP[neg+3], isFloat);
    instP[pc++] = new BoolNativeGreater<float>(bOutP[outBoolC++], fLiP[neg], fLiP[neg], isFloat);
    instP[pc++] = new BoolNativeGreater<float>(bOutP[outBoolC++], fLiP[7], fLiP[neg+7], isFloat);
    instP[pc++] = new BoolNativeGreater<float>(bOutP[outBoolC++], fLiP[neg+7], fLiP[7], isFloat);

    instP[pc++] = new BoolNativeGreater<float>(bOutP[outBoolC++], fLiP[12], fLiP[nullidx], isFloat);
    instP[pc++] = new BoolNativeGreater<float>(bOutP[outBoolC++], fLiP[nullidx], fLiP[3], isFloat);
    instP[pc++] = new BoolNativeGreater<float>(bOutP[outBoolC++], fLiP[nullidx], fLiP[nullidx], isFloat);

    // greaterequal
    instP[pc++] = new BoolNativeGreaterEqual<float>(bOutP[outBoolC++], fLiP[0], fLiP[0], isFloat);
    instP[pc++] = new BoolNativeGreaterEqual<float>(bOutP[outBoolC++], fLiP[4], fLiP[4], isFloat);
    instP[pc++] = new BoolNativeGreaterEqual<float>(bOutP[outBoolC++], fLiP[9], fLiP[9], isFloat);
    instP[pc++] = new BoolNativeGreaterEqual<float>(bOutP[outBoolC++], fLiP[3], fLiP[5], isFloat);
    instP[pc++] = new BoolNativeGreaterEqual<float>(bOutP[outBoolC++], fLiP[5], fLiP[3], isFloat);
    instP[pc++] = new BoolNativeGreaterEqual<float>(bOutP[outBoolC++], fLiP[neg+3], fLiP[neg+5], isFloat);
    instP[pc++] = new BoolNativeGreaterEqual<float>(bOutP[outBoolC++], fLiP[neg+5], fLiP[neg+3], isFloat);
    instP[pc++] = new BoolNativeGreaterEqual<float>(bOutP[outBoolC++], fLiP[neg], fLiP[neg], isFloat);
    instP[pc++] = new BoolNativeGreaterEqual<float>(bOutP[outBoolC++], fLiP[7], fLiP[neg+7], isFloat);
    instP[pc++] = new BoolNativeGreaterEqual<float>(bOutP[outBoolC++], fLiP[neg+7], fLiP[7], isFloat);
    instP[pc++] = new BoolNativeGreaterEqual<float>(bOutP[outBoolC++], fLiP[neg+7], fLiP[neg+7], isFloat);

    instP[pc++] = new BoolNativeGreaterEqual<float>(bOutP[outBoolC++], fLiP[12], fLiP[nullidx], isFloat);
    instP[pc++] = new BoolNativeGreaterEqual<float>(bOutP[outBoolC++], fLiP[nullidx], fLiP[3], isFloat);
    instP[pc++] = new BoolNativeGreaterEqual<float>(bOutP[outBoolC++], fLiP[nullidx], fLiP[nullidx], isFloat);

    // less
    instP[pc++] = new BoolNativeLess<float>(bOutP[outBoolC++], fLiP[0], fLiP[0], isFloat);
    instP[pc++] = new BoolNativeLess<float>(bOutP[outBoolC++], fLiP[4], fLiP[4], isFloat);
    instP[pc++] = new BoolNativeLess<float>(bOutP[outBoolC++], fLiP[9], fLiP[9], isFloat);
    instP[pc++] = new BoolNativeLess<float>(bOutP[outBoolC++], fLiP[3], fLiP[5], isFloat);
    instP[pc++] = new BoolNativeLess<float>(bOutP[outBoolC++], fLiP[5], fLiP[3], isFloat);
    instP[pc++] = new BoolNativeLess<float>(bOutP[outBoolC++], fLiP[neg+3], fLiP[neg+5], isFloat);
    instP[pc++] = new BoolNativeLess<float>(bOutP[outBoolC++], fLiP[neg+5], fLiP[neg+3], isFloat);

    instP[pc++] = new BoolNativeLess<float>(bOutP[outBoolC++], fLiP[12], fLiP[nullidx], isFloat);
    instP[pc++] = new BoolNativeLess<float>(bOutP[outBoolC++], fLiP[nullidx], fLiP[3], isFloat);
    instP[pc++] = new BoolNativeLess<float>(bOutP[outBoolC++], fLiP[nullidx], fLiP[nullidx], isFloat);

    // lessequal
    instP[pc++] = new BoolNativeLessEqual<float>(bOutP[outBoolC++], fLiP[0], fLiP[0], isFloat);
    instP[pc++] = new BoolNativeLessEqual<float>(bOutP[outBoolC++], fLiP[4], fLiP[4], isFloat);
    instP[pc++] = new BoolNativeLessEqual<float>(bOutP[outBoolC++], fLiP[9], fLiP[9], isFloat);
    instP[pc++] = new BoolNativeLessEqual<float>(bOutP[outBoolC++], fLiP[3], fLiP[5], isFloat);
    instP[pc++] = new BoolNativeLessEqual<float>(bOutP[outBoolC++], fLiP[5], fLiP[3], isFloat);
    instP[pc++] = new BoolNativeLessEqual<float>(bOutP[outBoolC++], fLiP[neg+3], fLiP[neg+5], isFloat);
    instP[pc++] = new BoolNativeLessEqual<float>(bOutP[outBoolC++], fLiP[neg+5], fLiP[neg+3], isFloat);

    instP[pc++] = new BoolNativeLessEqual<float>(bOutP[outBoolC++], fLiP[12], fLiP[nullidx], isFloat);
    instP[pc++] = new BoolNativeLessEqual<float>(bOutP[outBoolC++], fLiP[nullidx], fLiP[3], isFloat);
    instP[pc++] = new BoolNativeLessEqual<float>(bOutP[outBoolC++], fLiP[nullidx], fLiP[nullidx], isFloat);
  
    // isnull
    instP[pc++] = new BoolNativeIsNull<float>(bOutP[outBoolC++], fLiP[12], isFloat);
    instP[pc++] = new BoolNativeIsNull<float>(bOutP[outBoolC++], fLiP[nullidx], isFloat); 
    // isnotnull
    instP[pc++] = new BoolNativeIsNotNull<float>(bOutP[outBoolC++], fLiP[12], isFloat);
    instP[pc++] = new BoolNativeIsNotNull<float>(bOutP[outBoolC++], fLiP[nullidx], isFloat);
    // tonull
    instP[pc++] = new NativeToNull<float>(fOutP[outC++], isFloat);

    // return
    instP[pc++] = new NativeMove<float>(fOutP[outC], fLiP[20], isFloat);  // good flag
    instP[pc++] = new ReturnInstruction();
    instP[pc++] = new NativeMove<float>(fOutP[outC++], fLiP[10], isFloat);// bad flag

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

    string out;
    for (i = 0; i < pc; i++) {
        instP[i]->describe(out, true);
        printf("[%2d] %s\n", i, out.c_str());
    }

    // Print out the output tuple
    printf("Output Tuple\n");
    tuplePrinter.print(cout, tupleDesc, output);
    cout << endl;

    outC = 0;
    outBoolC = registersize;
    // TODO tests to add: Maxint, minint, zeros, negatives, overflow, underflow, etc
    // add
    if (*(reinterpret_cast<const float *>(output[outC++].pData)) != 10) fail("floatadd1", __LINE__);
    if (*(reinterpret_cast<const float *>(output[outC++].pData)) != 9.5) fail("floatadd2", __LINE__);
    if (*(reinterpret_cast<const float *>(output[outC++].pData)) != 0) fail("floatadd3", __LINE__);
    if (*(reinterpret_cast<const float *>(output[outC++].pData)) != 0) fail("floatadd4", __LINE__);
    if (*(reinterpret_cast<const float *>(output[outC++].pData)) != -1.5) fail("floatadd5", __LINE__);
    if (output[outC++].pData != NULL) fail("floatadd6", __LINE__);
    if (output[outC++].pData != NULL) fail("floatadd7", __LINE__);
    if (output[outC++].pData != NULL) fail("floatadd8", __LINE__);

    // sub
    if (*(reinterpret_cast<const float *>(output[outC++].pData)) != 0.5) fail("floatsub1", __LINE__);
    if (*(reinterpret_cast<const float *>(output[outC++].pData)) != 0) fail("floatsub2", __LINE__);
    if (*(reinterpret_cast<const float *>(output[outC++].pData)) != 4.5) fail("floatsub3", __LINE__);
    if (*(reinterpret_cast<const float *>(output[outC++].pData)) != 0) fail("floatsub4", __LINE__);
    if (*(reinterpret_cast<const float *>(output[outC++].pData)) != 0) fail("floatsub5", __LINE__);
    if (*(reinterpret_cast<const float *>(output[outC++].pData)) != -1.5) fail("floatsub6", __LINE__);

    if (output[outC++].pData != NULL) fail("floatsub7", __LINE__);
    if (output[outC++].pData != NULL) fail("floatsub8", __LINE__);
    if (output[outC++].pData != NULL) fail("floatsub9", __LINE__);

    // mul
    if (*(reinterpret_cast<const float *>(output[outC++].pData)) != 6) fail("floatmul1", __LINE__);
    if (*(reinterpret_cast<const float *>(output[outC++].pData)) != 6.25) fail("floatmul2", __LINE__);
    if (*(reinterpret_cast<const float *>(output[outC++].pData)) != 0) fail("floatmul3", __LINE__);
    if (*(reinterpret_cast<const float *>(output[outC++].pData)) != 0) fail("floatmul4", __LINE__);
    if (*(reinterpret_cast<const float *>(output[outC++].pData)) != 0) fail("floatmul5", __LINE__);
    if (*(reinterpret_cast<const float *>(output[outC++].pData)) != 0) fail("floatmul6", __LINE__);
    if (*(reinterpret_cast<const float *>(output[outC++].pData)) != -3.5) fail("floatmul7", __LINE__);

    if (output[outC++].pData != NULL) fail("floatmul8", __LINE__);
    if (output[outC++].pData != NULL) fail("floatmul9", __LINE__);
    if (output[outC++].pData != NULL) fail("floatmul10", __LINE__);

    // div
    if (*(reinterpret_cast<const float *>(output[outC++].pData)) != 3) fail("floatdiv1", __LINE__);
    if (*(reinterpret_cast<const float *>(output[outC++].pData)) != 4) fail("floatdiv2", __LINE__);
    if (*(reinterpret_cast<const float *>(output[outC++].pData)) != 0) fail("floatdiv3", __LINE__);
    if (*(reinterpret_cast<const float *>(output[outC++].pData)) != 0) fail("floatdiv4", __LINE__);
    if (*(reinterpret_cast<const float *>(output[outC++].pData)) != 4.5) fail("floatdiv5", __LINE__);
    if (*(reinterpret_cast<const float *>(output[outC++].pData)) != -9) fail("floatdiv6", __LINE__);

    if (output[outC++].pData != NULL) fail("floatdiv7", __LINE__);
    if (output[outC++].pData != NULL) fail("floatdiv8", __LINE__);
    if (output[outC++].pData != NULL) fail("floatdiv9", __LINE__);
    // div by zero 
    assert(outC == divbyzero);
    if (output[outC++].pData != NULL) fail("floatdiv10", __LINE__);
    deque<CalcMessage>::iterator iter = c.mWarnings.begin();
    if (iter->pc != divbyzero)
        fail("floatdiv by zero failed, pc wrong\n", __LINE__);
    string expectederror("22012");
    if (expectederror.compare(iter->str)) 
        fail("floatdiv by zero failed string was wrong", __LINE__);

    if (output[outC++].pData != NULL) fail("floatdiv11", __LINE__);
    iter++;
    if (iter->pc != divbyzero+1)
        fail("floatdiv by zero failed, pc wrong\n", __LINE__);
    if (expectederror.compare(iter->str)) 
        fail("floatdiv by zero failed string was wrong", __LINE__);


    // neg
    if (*(reinterpret_cast<const float *>(output[outC++].pData)) != -1.5) fail("floatneg1", __LINE__);
    if (*(reinterpret_cast<const float *>(output[outC++].pData)) != 1.5) fail("floatneg2", __LINE__);
    if (*(reinterpret_cast<const float *>(output[outC++].pData)) != 0) fail("floatneg3", __LINE__);
    if (*(reinterpret_cast<const float *>(output[outC++].pData)) != 0) fail("floatneg4", __LINE__);
    if (output[outC++].pData != NULL) fail("floatneg5", __LINE__);

    // move
    if (*(reinterpret_cast<const float *>(output[outC++].pData)) != 1.5) fail("floatmove1", __LINE__);
    if (*(reinterpret_cast<const float *>(output[outC++].pData)) != 3) fail("floatmove2", __LINE__);
    if (*(reinterpret_cast<const float *>(output[outC++].pData)) != 0) fail("floatmove3", __LINE__);
    if (*(reinterpret_cast<const float *>(output[outC++].pData)) != 0) fail("floatmove4", __LINE__);
    if (output[outC++].pData != NULL) fail("floatmove5", __LINE__);

    // equal
    if (*(output[outBoolC++].pData) != true) fail("floatequal1", __LINE__);
    if (*(output[outBoolC++].pData) != true) fail("floatequal2", __LINE__);
    if (*(output[outBoolC++].pData) != true) fail("floatequal3", __LINE__);
    if (*(output[outBoolC++].pData) != true) fail("floatequal4", __LINE__);
    if (*(output[outBoolC++].pData) != false) fail("floatequal5", __LINE__);
    if (*(output[outBoolC++].pData) != false) fail("floatequal6", __LINE__);
    if (*(output[outBoolC++].pData) != false) fail("floatequal7", __LINE__);
    if (*(output[outBoolC++].pData) != false) fail("floatequal8", __LINE__);
    if (*(output[outBoolC++].pData) != true) fail("floatequal9", __LINE__);
    if (*(output[outBoolC++].pData) != false) fail("floatequal10", __LINE__);

    if (output[outBoolC++].pData != NULL) fail("floatequal11", __LINE__);
    if (output[outBoolC++].pData != NULL) fail("floatequal12", __LINE__);
    if (output[outBoolC++].pData != NULL) fail("floatequal13", __LINE__);

    // notequal
    if (*(output[outBoolC++].pData) != false) fail("floatnotequal1", __LINE__);
    if (*(output[outBoolC++].pData) != false) fail("floatnotequal2", __LINE__);
    if (*(output[outBoolC++].pData) != false) fail("floatnotequal3", __LINE__);
    if (*(output[outBoolC++].pData) != true) fail("floatnotequal4", __LINE__);
    if (*(output[outBoolC++].pData) != true) fail("floatnotequal5", __LINE__);
    if (*(output[outBoolC++].pData) != true) fail("floatnotequal6", __LINE__);
    if (*(output[outBoolC++].pData) != true) fail("floatnotequal7", __LINE__);

    if (output[outBoolC++].pData != NULL) fail("floatnotequal8", __LINE__);
    if (output[outBoolC++].pData != NULL) fail("floatnotequal9", __LINE__);
    if (output[outBoolC++].pData != NULL) fail("floatnotequal10", __LINE__);

    // greater
    if (*(output[outBoolC++].pData) != false) fail("floatgreater1", __LINE__);
    if (*(output[outBoolC++].pData) != false) fail("floatgreater2", __LINE__);
    if (*(output[outBoolC++].pData) != false) fail("floatgreater3", __LINE__);
    if (*(output[outBoolC++].pData) != false) fail("floatgreater4", __LINE__);
    if (*(output[outBoolC++].pData) != true) fail("floatgreater5", __LINE__);
    if (*(output[outBoolC++].pData) != true) fail("floatgreater6", __LINE__);
    if (*(output[outBoolC++].pData) != false) fail("floatgreater7", __LINE__);
    if (*(output[outBoolC++].pData) != false) fail("floatgreater8", __LINE__);
    if (*(output[outBoolC++].pData) != true) fail("floatgreater9", __LINE__);
    if (*(output[outBoolC++].pData) != false) fail("floatgreater10", __LINE__);

    if (output[outBoolC++].pData != NULL) fail("floatgreater11", __LINE__);
    if (output[outBoolC++].pData != NULL) fail("floatgreater12", __LINE__);
    if (output[outBoolC++].pData != NULL) fail("floatgreater13", __LINE__);

    // greaterequal
    if (*(output[outBoolC++].pData) != true) fail("floatgreaterequal1", __LINE__);
    if (*(output[outBoolC++].pData) != true) fail("floatgreaterequal2", __LINE__);
    if (*(output[outBoolC++].pData) != true) fail("floatgreaterequal3", __LINE__);
    if (*(output[outBoolC++].pData) != false) fail("floatgreaterequal4", __LINE__);
    if (*(output[outBoolC++].pData) != true) fail("floatgreaterequal5", __LINE__);
    if (*(output[outBoolC++].pData) != true) fail("floatgreaterequal6", __LINE__);
    if (*(output[outBoolC++].pData) != false) fail("floatgreaterequal7", __LINE__);
    if (*(output[outBoolC++].pData) != true) fail("floatgreaterequal8", __LINE__);
    if (*(output[outBoolC++].pData) != true) fail("floatgreaterequal9", __LINE__);
    if (*(output[outBoolC++].pData) != false) fail("floatgreaterequal10", __LINE__);
    if (*(output[outBoolC++].pData) != true) fail("floatgreaterequal11", __LINE__);

    if (output[outBoolC++].pData != NULL) fail("floatgreaterequal12", __LINE__);
    if (output[outBoolC++].pData != NULL) fail("floatgreaterequal13", __LINE__);
    if (output[outBoolC++].pData != NULL) fail("floatgreaterequal14", __LINE__);

    // less
    if (*(output[outBoolC++].pData) != false) fail("floatless1", __LINE__);
    if (*(output[outBoolC++].pData) != false) fail("floatless2", __LINE__);
    if (*(output[outBoolC++].pData) != false) fail("floatless3", __LINE__);
    if (*(output[outBoolC++].pData) != true) fail("floatless4", __LINE__);
    if (*(output[outBoolC++].pData) != false) fail("floatless5", __LINE__);
    if (*(output[outBoolC++].pData) != false) fail("floatless6", __LINE__);
    if (*(output[outBoolC++].pData) != true) fail("floatless7", __LINE__);

    if (output[outBoolC++].pData != NULL) fail("floatless8", __LINE__);
    if (output[outBoolC++].pData != NULL) fail("floatless9", __LINE__);
    if (output[outBoolC++].pData != NULL) fail("floatless10", __LINE__);

    // lessequal
    if (*(output[outBoolC++].pData) != true) fail("floatlessequal1", __LINE__);
    if (*(output[outBoolC++].pData) != true) fail("floatlessequal2", __LINE__);
    if (*(output[outBoolC++].pData) != true) fail("floatlessequal3", __LINE__);
    if (*(output[outBoolC++].pData) != true) fail("floatlessequal4", __LINE__);
    if (*(output[outBoolC++].pData) != false) fail("floatlessequal5", __LINE__);
    if (*(output[outBoolC++].pData) != false) fail("floatlessequal6", __LINE__);
    if (*(output[outBoolC++].pData) != true) fail("floatlessequal7", __LINE__);

    if (output[outBoolC++].pData != NULL) fail("floatlessequal8", __LINE__);
    if (output[outBoolC++].pData != NULL) fail("floatlessequal9", __LINE__);
    if (output[outBoolC++].pData != NULL) fail("floatlessequal10", __LINE__);

    // isnull
    if (*(output[outBoolC++].pData) != false) fail("floatisnull1", __LINE__);
    if (*(output[outBoolC++].pData) != true) fail("floatisnull2", __LINE__);

    // isnotnull
    if (*(output[outBoolC++].pData) != true) fail("floatisnotnull1", __LINE__);
    if (*(output[outBoolC++].pData) != false) fail("floatisnotnull2", __LINE__);

    // tonull
    if (output[outC++].pData != NULL) fail("floattonull1", __LINE__);


    // return
    if (*(reinterpret_cast<const float *>(output[outC++].pData)) != 10) fail("floatreturn", __LINE__);

    cout << "Calculator Warnings: " << c.warnings() << endl;

    delete [] fInP;
    delete [] fOutP;
    delete [] fLoP;
    delete [] fLiP;
    delete [] bOutP;
     
    for (i = 0; i < lastPC; i++) {
        delete instP[i];
    }
    delete [] instP;
 
}

void
unitTestPointer()
{
    printf("=========================================================\n");
    printf("=========================================================\n");
    printf("=====\n");
    printf("=====     unitTestPointer()\n");
    printf("=====\n");
    printf("=========================================================\n");
    printf("=========================================================\n");

    bool isNullable = true;    // Can tuple contain nulls?
    int i, registersize = 100;
    static uint bufferlen = 8;

    TupleDescriptor tupleDesc;
    tupleDesc.clear();

    // Build up a description of what we'd like the tuple to look like
    StandardTypeDescriptorFactory typeFactory;
    int idx = 0;

    const int pointerIdx = idx;
    for (i=0;i < registersize; i++) {
        // pointers in first "half"
        StoredTypeDescriptor const &typeDesc = typeFactory.newDataType(STANDARD_TYPE_VARCHAR);
        // tell descriptor the size 
        tupleDesc.push_back(TupleAttributeDescriptor(typeDesc,
                                                     isNullable,
                                                     bufferlen));
        idx++;
    
    }
    const int ulongIdx = idx;
    for (i=0;i < registersize; i++) {
        // unsigned longs in third "half"
        // will serve as PointerSizeT
        StoredTypeDescriptor const &typeDesc = typeFactory.newDataType(STANDARD_TYPE_UINT_32);
        tupleDesc.push_back(TupleAttributeDescriptor(typeDesc, isNullable));
        idx++;
    }
    const int boolIdx = idx;
    for (i=0;i < registersize; i++) {
        // booleans in fourth "half"
        StoredTypeDescriptor const &typeDesc = typeFactory.newDataType(STANDARD_TYPE_UINT_8);
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
    tupleAccessorFixedLiteral.compute(tupleDesc, TUPLE_FORMAT_ALL_NOT_NULL_AND_FIXED);
    tupleAccessorFixedInput.compute(tupleDesc, TUPLE_FORMAT_ALL_NOT_NULL_AND_FIXED);
    tupleAccessorFixedOutput.compute(tupleDesc, TUPLE_FORMAT_ALL_NOT_NULL_AND_FIXED);
    tupleAccessorFixedLocal.compute(tupleDesc, TUPLE_FORMAT_ALL_NOT_NULL_AND_FIXED);
    tupleAccessorFixedStatus.compute(tupleDesc, TUPLE_FORMAT_ALL_NOT_NULL_AND_FIXED);

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
    tupleAccessorFixedLiteral.setCurrentTupleBuf(pTupleBufFixedLiteral.get());
    tupleAccessorFixedInput.setCurrentTupleBuf(pTupleBufFixedInput.get());
    tupleAccessorFixedOutput.setCurrentTupleBuf(pTupleBufFixedOutput.get());
    tupleAccessorFixedLocal.setCurrentTupleBuf(pTupleBufFixedLocal.get());
    tupleAccessorFixedStatus.setCurrentTupleBuf(pTupleBufFixedStatus.get());

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
    
    // Set up some useful literals
    itr = literal.begin();
    for(i=0; i < registersize; i++, itr++) {
        char num[16];
        sprintf(num, "%04d", i);
        char* ptr = reinterpret_cast<char *>(const_cast<PBuffer>(itr->pData));
        memset(ptr, 'C', bufferlen); // VARCHAR is not null terminated
        memcpy(ptr, num, 4); // copy number, but not null
    }
    for(i=0; i < registersize; i++, itr++) {
        *(reinterpret_cast<uint32_t *>(const_cast<PBuffer>(itr->pData))) = i;
    }
    for(i=0; i < registersize; i++, itr++) {
        *(reinterpret_cast<bool *>(const_cast<PBuffer>(itr->pData))) = false;
    }
  
    // Put some data other tuples as well
    itr = input.begin();
    for(i=0; i < registersize; i++, itr++) {
        char num[16];
        sprintf(num, "%04d", i);
        char *ptr = reinterpret_cast<char *>(const_cast<PBuffer>(itr->pData));
        memset(ptr, 'I', bufferlen); // VARCHAR is not null terminated
        memcpy(ptr, num, 4); // copy number, but not null
    }
    for(i=0; i < registersize; i++, itr++) {
        *(reinterpret_cast<uint32_t *>(const_cast<PBuffer>(itr->pData))) = 0;
    }
    for(i=0; i < registersize; i++, itr++) {
        *(reinterpret_cast<bool *>(const_cast<PBuffer>(itr->pData))) = false;
    }
    itr = output.begin();
    for(i=0; i < registersize; i++, itr++) {
        char* ptr = reinterpret_cast<char *>(const_cast<PBuffer>(itr->pData));
        memset(ptr, 'O', bufferlen); // VARCHAR is not null terminated
    }
    for(i=0; i < registersize; i++, itr++) {
        *(reinterpret_cast<uint32_t *>(const_cast<PBuffer>(itr->pData))) = 0;
    }
    for(i=0; i < registersize; i++, itr++) {
        *(reinterpret_cast<bool *>(const_cast<PBuffer>(itr->pData))) = false;
    }
    itr = local.begin();
    for(i=0; i < registersize; i++, itr++) {
        char* ptr = reinterpret_cast<char *>(const_cast<PBuffer>(itr->pData));
        memset(ptr, 'L', bufferlen); // VARCHAR is not null terminated
    }
    for(i=0; i < registersize; i++, itr++) {
        *(reinterpret_cast<uint32_t *>(const_cast<PBuffer>(itr->pData))) = 0;
    }
    for(i=0; i < registersize; i++, itr++) {
        *(reinterpret_cast<bool *>(const_cast<PBuffer>(itr->pData))) = false;
    }

    // set up boolean literals
    int falseIdx = 0;
    int trueIdx = 1;
    *(reinterpret_cast<bool *>
      (const_cast<PBuffer>
       (literal[trueIdx+boolIdx].pData))) = true;
    *(reinterpret_cast<bool *>
      (const_cast<PBuffer>
       (literal[falseIdx+boolIdx].pData))) = false;


  
    // null out last element of each type
    int pointerNullIdx = pointerIdx + registersize - 1;
    int ulongNullIdx = ulongIdx + registersize - 1;
    int boolNullIdx = boolIdx + registersize - 1;
    literal[pointerNullIdx].pData = NULL;
    literal[ulongNullIdx].pData = NULL;
    literal[boolNullIdx].pData = NULL;
    literal[pointerNullIdx].cbData = 0;
    literal[ulongNullIdx].cbData = 0;
    literal[boolNullIdx].cbData = 0;

    input[pointerNullIdx].pData = NULL;
    input[ulongNullIdx].pData = NULL;
    input[boolNullIdx].pData = NULL;
    input[pointerNullIdx].cbData = 0;
    input[ulongNullIdx].cbData = 0;
    input[boolNullIdx].cbData = 0;

    output[pointerNullIdx].pData = NULL;
    output[ulongNullIdx].pData = NULL;
    output[boolNullIdx].pData = NULL;
    output[pointerNullIdx].cbData = 0;
    output[ulongNullIdx].cbData = 0;
    output[boolNullIdx].cbData = 0;
    
    local[pointerNullIdx].pData = NULL;
    local[ulongNullIdx].pData = NULL;
    local[boolNullIdx].pData = NULL;
    local[pointerNullIdx].cbData = 0;
    local[ulongNullIdx].cbData = 0;
    local[boolNullIdx].cbData = 0;

    // Print out the nullable tuple
    TuplePrinter tuplePrinter;
    printf("Literals\n");
    tuplePrinter.print(cout, tupleDesc, literal);
    cout << endl;
    printf("\nInput\n");
    tuplePrinter.print(cout, tupleDesc, input);
    cout << endl;
    printf("\nOutput\n");
    tuplePrinter.print(cout, tupleDesc, output);
    cout << endl;
    printf("\nLocal\n");
    tuplePrinter.print(cout, tupleDesc, local);
    cout << endl;


    // predefine register references. a real compiler wouldn't do
    // something so regular and pre-determined. a compiler would
    // probably build these on the fly as it built each instruction.
    // predefine register references. a real compiler wouldn't do
    // something so regular and pre-determined
    RegisterRef<char *> **cpInP, **cpOutP, **cpLoP, **cpLiP;
    RegisterRef<PointerOperandT> **lInP, **lOutP, **lLoP, **lLiP;
    RegisterRef<bool> **bInP, **bOutP, **bLoP, **bLiP;

    cpInP = new RegisterRef<char *>*[registersize];
    cpOutP = new RegisterRef<char *>*[registersize];
    cpLoP = new RegisterRef<char *>*[registersize];
    cpLiP = new RegisterRef<char *>*[registersize];

    lInP = new RegisterRef<PointerOperandT>*[registersize];
    lOutP = new RegisterRef<PointerOperandT>*[registersize];
    lLoP = new RegisterRef<PointerOperandT>*[registersize];
    lLiP = new RegisterRef<PointerOperandT>*[registersize];

    bInP = new RegisterRef<bool>*[registersize];
    bOutP = new RegisterRef<bool>*[registersize];
    bLoP = new RegisterRef<bool>*[registersize];
    bLiP = new RegisterRef<bool>*[registersize];

    // Set up the Calculator
    Calculator c(0,0,0,0,0,0);
    c.outputRegisterByReference(false);

    // set up register references to symbolically point to 
    // their corresponding storage locations -- makes for easy test case
    // generation. again, a compiler wouldn't do things in quite
    // this way.
    for (i=0; i < registersize; i++) {
        cpInP[i] = new RegisterRef<char *>(RegisterReference::EInput, 
                                           pointerIdx + i, 
                                           STANDARD_TYPE_VARCHAR);
        c.appendRegRef(cpInP[i]);
        cpOutP[i] = new RegisterRef<char *>(RegisterReference::EOutput,
                                            pointerIdx + i,
                                            STANDARD_TYPE_VARCHAR);
        c.appendRegRef(cpOutP[i]);
        cpLoP[i] = new RegisterRef<char *>(RegisterReference::ELocal,
                                           pointerIdx + i,
                                           STANDARD_TYPE_VARCHAR);
        c.appendRegRef(cpLoP[i]);
        cpLiP[i] = new RegisterRef<char *>(RegisterReference::ELiteral,
                                           pointerIdx + i, 
                                           STANDARD_TYPE_VARCHAR);
        c.appendRegRef(cpLiP[i]);

        lInP[i] = new RegisterRef<PointerOperandT>(RegisterReference::EInput,
                                                   ulongIdx + i,
                                                   STANDARD_TYPE_INT_32);
        c.appendRegRef(lInP[i]);
        lOutP[i] = new RegisterRef<PointerOperandT>(RegisterReference::EOutput,
                                                    ulongIdx + i,
                                                    STANDARD_TYPE_INT_32);
        c.appendRegRef(lOutP[i]);
        lLoP[i] = new RegisterRef<PointerOperandT>(RegisterReference::ELocal,
                                                   ulongIdx + i,
                                                   STANDARD_TYPE_INT_32);
        c.appendRegRef(lLoP[i]);
        lLiP[i] = new RegisterRef<PointerOperandT>(RegisterReference::ELiteral,
                                                   ulongIdx + i, 
                                                   STANDARD_TYPE_INT_32);
        c.appendRegRef(lLiP[i]);

        bInP[i] = new RegisterRef<bool>(RegisterReference::EInput,
                                        boolIdx + i,
                                        STANDARD_TYPE_BOOL);
        c.appendRegRef(bInP[i]);
        bOutP[i] = new RegisterRef<bool>(RegisterReference::EOutput,
                                         boolIdx + i,
                                         STANDARD_TYPE_BOOL);
        c.appendRegRef(bOutP[i]);
        bLoP[i] = new RegisterRef<bool>(RegisterReference::ELocal,
                                        boolIdx + i, 
                                        STANDARD_TYPE_BOOL);
        c.appendRegRef(bLoP[i]);
        bLiP[i] = new RegisterRef<bool>(RegisterReference::ELiteral,
                                        boolIdx + i,
                                        STANDARD_TYPE_BOOL);
        c.appendRegRef(bLiP[i]);
    }

    // Set up storage for instructions
    // a real compiler would probably cons up instructions and insert them
    // directly into the calculator. keep an array of the instructions at
    // this level to allow printing of the program after execution, and other
    // debugging
    Instruction **instP;
    instP = new (Instruction *)[200];
    int pc = 0, outCp = 0, outL=0, outB = 0, localCp = 0;
    int nullRegister = registersize - 1;
    
    StandardTypeDescriptorOrdinal isVC = STANDARD_TYPE_VARCHAR;

    // add
    instP[pc++] = new PointerAdd<char *>(cpOutP[0], cpLiP[0], lLiP[0], isVC); // add 0
    instP[pc++] = new PointerAdd<char *>(cpOutP[1], cpLiP[1], lLiP[1], isVC); // add 1
    instP[pc++] = new PointerAdd<char *>(cpOutP[2], cpLiP[2], lLiP[2], isVC); // add 2

    outCp = 3;
    instP[pc++] = new PointerAdd<char *>(cpOutP[outCp++], cpLiP[nullRegister], lLiP[2], isVC); 
    instP[pc++] = new PointerAdd<char *>(cpOutP[outCp++], cpLiP[0], lLiP[nullRegister], isVC);
    instP[pc++] = new PointerAdd<char *>(cpOutP[outCp++], cpLiP[nullRegister], lLiP[nullRegister], isVC);

    // sub
    // refer to previously added values in output buffer so that results are always
    // valid (as opposed to pointing before the legally allocated strings
    instP[pc++] = new PointerSub<char *>(cpOutP[outCp++], cpLiP[0], lLiP[0], isVC);  // sub 0
    instP[pc++] = new PointerSub<char *>(cpOutP[outCp++], cpOutP[1], lLiP[1], isVC); // sub 1
    instP[pc++] = new PointerSub<char *>(cpOutP[outCp++], cpOutP[2], lLiP[2], isVC); // sub 2

    instP[pc++] = new PointerSub<char *>(cpOutP[outCp++], cpLiP[nullRegister], lLiP[2], isVC); 
    instP[pc++] = new PointerSub<char *>(cpOutP[outCp++], cpLiP[0], lLiP[nullRegister], isVC);
    instP[pc++] = new PointerSub<char *>(cpOutP[outCp++], cpLiP[nullRegister], lLiP[nullRegister], isVC);

    // move
    instP[pc++] = new PointerMove<char *>(cpOutP[outCp++], cpLiP[2], isVC);
    instP[pc++] = new PointerMove<char *>(cpOutP[outCp++], cpLiP[nullRegister], isVC);
    // move a valid pointer over a null pointer
    instP[pc++] = new PointerMove<char *>(cpOutP[outCp], cpLiP[nullRegister], isVC);
    instP[pc++] = new PointerMove<char *>(cpOutP[outCp++], cpLiP[3], isVC);
    // move a null pointer to a null pointer
    instP[pc++] = new PointerMove<char *>(cpOutP[outCp], cpLiP[nullRegister], isVC);
    instP[pc++] = new PointerMove<char *>(cpOutP[outCp++], cpLiP[nullRegister], isVC);

    // equal
    instP[pc++] = new BoolPointerEqual<char *>(bOutP[outB++], cpLiP[0], cpLiP[0], isVC);
    instP[pc++] = new BoolPointerEqual<char *>(bOutP[outB++], cpLiP[0], cpLiP[1], isVC);
    instP[pc++] = new BoolPointerEqual<char *>(bOutP[outB++], cpLiP[1], cpLiP[0], isVC);
    instP[pc++] = new BoolPointerEqual<char *>(bOutP[outB++], cpLiP[nullRegister], cpLiP[1], isVC);
    instP[pc++] = new BoolPointerEqual<char *>(bOutP[outB++], cpLiP[0], cpLiP[nullRegister], isVC);
    instP[pc++] = new BoolPointerEqual<char *>(bOutP[outB++], cpLiP[nullRegister], cpLiP[nullRegister], isVC);

    // notequal
    instP[pc++] = new BoolPointerNotEqual<char *>(bOutP[outB++], cpLiP[0], cpLiP[0], isVC);
    instP[pc++] = new BoolPointerNotEqual<char *>(bOutP[outB++], cpLiP[0], cpLiP[1], isVC);
    instP[pc++] = new BoolPointerNotEqual<char *>(bOutP[outB++], cpLiP[1], cpLiP[0], isVC);
    instP[pc++] = new BoolPointerNotEqual<char *>(bOutP[outB++], cpLiP[nullRegister], cpLiP[1], isVC);
    instP[pc++] = new BoolPointerNotEqual<char *>(bOutP[outB++], cpLiP[0], cpLiP[nullRegister], isVC);
    instP[pc++] = new BoolPointerNotEqual<char *>(bOutP[outB++], cpLiP[nullRegister], cpLiP[nullRegister], isVC);

    // greater
    // assume that values allocated later are larger
    assert(output[pointerIdx].pData < output[pointerIdx+1].pData);
    instP[pc++] = new BoolPointerGreater<char *>(bOutP[outB++], cpLiP[0], cpLiP[0], isVC);
    instP[pc++] = new BoolPointerGreater<char *>(bOutP[outB++], cpLiP[0], cpLiP[1], isVC);
    instP[pc++] = new BoolPointerGreater<char *>(bOutP[outB++], cpLiP[1], cpLiP[0], isVC);

    instP[pc++] = new BoolPointerGreater<char *>(bOutP[outB++], cpLiP[nullRegister], cpLiP[1], isVC);
    instP[pc++] = new BoolPointerGreater<char *>(bOutP[outB++], cpLiP[0], cpLiP[nullRegister], isVC);
    instP[pc++] = new BoolPointerGreater<char *>(bOutP[outB++], cpLiP[nullRegister], cpLiP[nullRegister], isVC);

    // greaterequal
    // assume that values allocated later are larger
    assert(output[pointerIdx].pData < output[pointerIdx+1].pData);
    instP[pc++] = new BoolPointerGreaterEqual<char *>(bOutP[outB++], cpLiP[0], cpLiP[0], isVC);
    instP[pc++] = new BoolPointerGreaterEqual<char *>(bOutP[outB++], cpLiP[0], cpLiP[1], isVC);
    instP[pc++] = new BoolPointerGreaterEqual<char *>(bOutP[outB++], cpLiP[1], cpLiP[0], isVC);

    instP[pc++] = new BoolPointerGreaterEqual<char *>(bOutP[outB++], cpLiP[nullRegister], cpLiP[1], isVC);
    instP[pc++] = new BoolPointerGreaterEqual<char *>(bOutP[outB++], cpLiP[0], cpLiP[nullRegister], isVC);
    instP[pc++] = new BoolPointerGreaterEqual<char *>(bOutP[outB++], cpLiP[nullRegister], cpLiP[nullRegister], isVC);

    // less
    // assume that values allocated later are larger
    assert(output[pointerIdx].pData < output[pointerIdx+1].pData);
    instP[pc++] = new BoolPointerLess<char *>(bOutP[outB++], cpLiP[0], cpLiP[0], isVC);
    instP[pc++] = new BoolPointerLess<char *>(bOutP[outB++], cpLiP[0], cpLiP[1], isVC);
    instP[pc++] = new BoolPointerLess<char *>(bOutP[outB++], cpLiP[1], cpLiP[0], isVC);
    instP[pc++] = new BoolPointerLess<char *>(bOutP[outB++], cpLiP[nullRegister], cpLiP[1], isVC);
    instP[pc++] = new BoolPointerLess<char *>(bOutP[outB++], cpLiP[0], cpLiP[nullRegister], isVC);
    instP[pc++] = new BoolPointerLess<char *>(bOutP[outB++], cpLiP[nullRegister], cpLiP[nullRegister], isVC);

    // lessequal
    // assume that values allocated later are larger
    assert(output[pointerIdx].pData < output[pointerIdx+1].pData);
    instP[pc++] = new BoolPointerLessEqual<char *>(bOutP[outB++], cpLiP[0], cpLiP[0], isVC);
    instP[pc++] = new BoolPointerLessEqual<char *>(bOutP[outB++], cpLiP[0], cpLiP[1], isVC);
    instP[pc++] = new BoolPointerLessEqual<char *>(bOutP[outB++], cpLiP[1], cpLiP[0], isVC);
    instP[pc++] = new BoolPointerLessEqual<char *>(bOutP[outB++], cpLiP[nullRegister], cpLiP[1], isVC);
    instP[pc++] = new BoolPointerLessEqual<char *>(bOutP[outB++], cpLiP[0], cpLiP[nullRegister], isVC);
    instP[pc++] = new BoolPointerLessEqual<char *>(bOutP[outB++], cpLiP[nullRegister], cpLiP[nullRegister], isVC);

    // isnull
    instP[pc++] = new BoolPointerIsNull<char *>(bOutP[outB++], cpLiP[0], isVC);
    instP[pc++] = new BoolPointerIsNull<char *>(bOutP[outB++], cpLiP[nullRegister], isVC); 

    // isnotnull
    instP[pc++] = new BoolPointerIsNotNull<char *>(bOutP[outB++], cpLiP[0], isVC);
    instP[pc++] = new BoolPointerIsNotNull<char *>(bOutP[outB++], cpLiP[nullRegister], isVC);

    // tonull
    instP[pc++] = new PointerToNull<char *>(cpOutP[outCp++], isVC);

    // putsize
    instP[pc++] = new PointerPutSize<char *>(cpOutP[outCp], lLiP[0], isVC);
    instP[pc++] = new PointerPutSize<char *>(cpOutP[outCp+1], lLiP[1], isVC);
    instP[pc++] = new PointerPutSize<char *>(cpOutP[outCp+2], lLiP[2], isVC);
    // putsize w/round trip through cached register set
    instP[pc++] = new PointerPutSize<char *>(cpLoP[localCp], lLiP[0], isVC);

    // getsize
    instP[pc++] = new PointerGetSize<char *>(lOutP[outL++], cpOutP[outCp], isVC);
    instP[pc++] = new PointerGetSize<char *>(lOutP[outL++], cpOutP[outCp+1], isVC);
    instP[pc++] = new PointerGetSize<char *>(lOutP[outL++], cpOutP[outCp+2], isVC);
    instP[pc++] = new PointerGetSize<char *>(lOutP[outL++], cpLiP[0], isVC);
    // getsize w/round trip through cached register set
    instP[pc++] = new PointerGetSize<char *>(lOutP[outL++], cpLoP[localCp], isVC);
    instP[pc++] = new PointerGetSize<char *>(lOutP[outL++], cpLoP[localCp+1], isVC);

    // getmaxsize
    instP[pc++] = new PointerGetMaxSize<char *>(lOutP[outL++], cpOutP[outCp], isVC);
    instP[pc++] = new PointerGetMaxSize<char *>(lOutP[outL++], cpLiP[0], isVC);
    // getmaxsize w/round trip through cached register set
    instP[pc++] = new PointerGetMaxSize<char *>(lOutP[outL++], cpLoP[localCp], isVC);
    instP[pc++] = new PointerGetMaxSize<char *>(lOutP[outL++], cpLoP[localCp+1], isVC);

    outCp+=3;
    localCp+=2;

    instP[pc++] = new ReturnInstruction();
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

    string out;
    for (i = 0; i < pc; i++) {
        instP[i]->describe(out, true);
        printf("[%2d] %s\n", i, out.c_str());
    }

    // Print out the output tuple
    printf("Output Tuple\n");
    tuplePrinter.print(cout, tupleDesc, output);
    cout << endl;

    outCp = 0;       // now indexes into output tuple, not outputregisterref
    outB = boolIdx;  // now indexes into output tuple, not outputregisterref
    outL = ulongIdx; // now indexes into output tuple, not outputregisterref

    // add
    if (output[outCp].pData != literal[pointerIdx + 0].pData) fail("pointeradd1", __LINE__);
    if (output[outCp++].cbData != bufferlen - 0) fail("pointeradd2", __LINE__);
    
    if ((reinterpret_cast<const char *>(output[outCp].pData)) != 
        ((reinterpret_cast<const char *>(literal[pointerIdx + 1].pData)) +
         *(reinterpret_cast<const int32_t *>(literal[ulongIdx + 1].pData))))
        fail("pointeradd3", __LINE__);
    if (output[outCp++].cbData != 
        bufferlen - *(reinterpret_cast<const int32_t *>(literal[ulongIdx + 1].pData)))
        fail("pointeradd4", __LINE__);

    if ((reinterpret_cast<const char *>(output[outCp].pData)) != 
        ((reinterpret_cast<const char *>(literal[pointerIdx + 2].pData)) +
         *(reinterpret_cast<const int32_t *>(literal[ulongIdx + 2].pData))))
        fail("pointeradd5", __LINE__);
    if (output[outCp++].cbData != 
        bufferlen - *(reinterpret_cast<const int32_t *>(literal[ulongIdx + 2].pData)))
        fail("pointeradd6", __LINE__);

    if (output[outCp].pData != NULL) fail("pointeradd7", __LINE__);
    if (output[outCp++].cbData != 0) fail("pointeradd8", __LINE__);
    if (output[outCp].pData != NULL) fail("pointeradd9", __LINE__);
    if (output[outCp++].cbData != 0) fail("pointeradd10", __LINE__);
    if (output[outCp].pData != NULL) fail("pointeradd11", __LINE__);
    if (output[outCp++].cbData != 0) fail("pointeradd12", __LINE__);

    // sub
    if (output[outCp].pData != literal[pointerIdx + 0].pData) fail("pointersub1", __LINE__);
    if (output[outCp++].cbData != bufferlen + 0) fail("pointersub2", __LINE__);

    if ((reinterpret_cast<const char *>(output[outCp].pData)) != 
        ((reinterpret_cast<const char *>(literal[pointerIdx + 1].pData))))
        fail("pointersub3", __LINE__);
    if (output[outCp++].cbData != bufferlen) fail("pointersub4", __LINE__);

    if ((reinterpret_cast<const char *>(output[outCp].pData)) != 
        ((reinterpret_cast<const char *>(literal[pointerIdx + 2].pData)))) 
        fail("pointersub5", __LINE__);
    if (output[outCp++].cbData != bufferlen) fail("pointersub6", __LINE__);

    if (output[outCp].pData != NULL) fail("pointersub7", __LINE__);
    if (output[outCp++].cbData != 0) fail("pointersub8", __LINE__);
    if (output[outCp].pData != NULL) fail("pointersub9", __LINE__);
    if (output[outCp++].cbData != 0) fail("pointersub10", __LINE__);
    if (output[outCp].pData != NULL) fail("pointersub11", __LINE__);
    if (output[outCp++].cbData != 0) fail("pointersub12", __LINE__);

    // move
    if (output[outCp].pData != literal[pointerIdx + 2].pData) fail("pointermove1", __LINE__);
    if (output[outCp++].cbData != bufferlen) fail("pointermove2", __LINE__);

    if (output[outCp].pData != NULL) fail("pointermove3", __LINE__);
    if (output[outCp++].cbData != 0) fail("pointermove4", __LINE__);

    if ((reinterpret_cast<const char *>(output[outCp].pData)) != 
        ((reinterpret_cast<const char *>(literal[pointerIdx + 3].pData)))) 
        fail("pointermove5", __LINE__);
    if (output[outCp++].cbData != bufferlen) fail("pointermove6", __LINE__);

    if (output[outCp].pData != NULL) fail("pointermove7", __LINE__);
    if (output[outCp++].cbData != 0) fail("pointermove8", __LINE__);

    
    // equal
    if (*(output[outB++].pData) != true) fail("pointerequal1", __LINE__);
    if (*(output[outB++].pData) != false) fail("pointerequal2", __LINE__);
    if (*(output[outB++].pData) != false) fail("pointerequal3", __LINE__);

    if (output[outB++].pData != NULL) fail("pointerequal4", __LINE__);
    if (output[outB++].pData != NULL) fail("pointerequal5", __LINE__);
    if (output[outB++].pData != NULL) fail("pointerequal6", __LINE__);

    // notequal
    if (*(output[outB++].pData) != false) fail("pointernotequal1", __LINE__);
    if (*(output[outB++].pData) != true) fail("pointernotequal2", __LINE__);
    if (*(output[outB++].pData) != true) fail("pointernotequal3", __LINE__);

    if (output[outB++].pData != NULL) fail("pointernotequal4", __LINE__);
    if (output[outB++].pData != NULL) fail("pointernotequal5", __LINE__);
    if (output[outB++].pData != NULL) fail("pointernotequal6", __LINE__);

    // greater
    if (*(output[outB++].pData) != false) fail("pointergreater1", __LINE__);
    if (*(output[outB++].pData) != false) fail("pointergreater2", __LINE__);
    if (*(output[outB++].pData) != true) fail("pointergreater3", __LINE__);

    if (output[outB++].pData != NULL) fail("pointergreater11", __LINE__);
    if (output[outB++].pData != NULL) fail("pointergreater12", __LINE__);
    if (output[outB++].pData != NULL) fail("pointergreater13", __LINE__);

    // greaterequal
    if (*(output[outB++].pData) != true) fail("pointergreaterequal1", __LINE__);
    if (*(output[outB++].pData) != false) fail("pointergreaterequal2", __LINE__);
    if (*(output[outB++].pData) != true) fail("pointergreaterequal3", __LINE__);

    if (output[outB++].pData != NULL) fail("pointergreaterequal14", __LINE__);
    if (output[outB++].pData != NULL) fail("pointergreaterequal15", __LINE__);
    if (output[outB++].pData != NULL) fail("pointergreaterequal16", __LINE__);

    // less
    if (*(output[outB++].pData) != false) fail("pointerless1", __LINE__);
    if (*(output[outB++].pData) != true) fail("pointerless2", __LINE__);
    if (*(output[outB++].pData) != false) fail("pointerless3", __LINE__);

    if (output[outB++].pData != NULL) fail("pointerless4", __LINE__);
    if (output[outB++].pData != NULL) fail("pointerless5", __LINE__);
    if (output[outB++].pData != NULL) fail("pointerless6", __LINE__);

    // lessequal
    if (*(output[outB++].pData) != true) fail("pointerlessequal1", __LINE__);
    if (*(output[outB++].pData) != true) fail("pointerlessequal2", __LINE__);
    if (*(output[outB++].pData) != false) fail("pointerlessequal3", __LINE__);

    if (output[outB++].pData != NULL) fail("pointerlessequal5", __LINE__);
    if (output[outB++].pData != NULL) fail("pointerlessequal6", __LINE__);
    if (output[outB++].pData != NULL) fail("pointerlessequal7", __LINE__);

    // isnull
    if (*(output[outB++].pData) != false) fail("pointerisnull1", __LINE__);
    if (*(output[outB++].pData) != true) fail("pointerisnull2", __LINE__);

    // isnotnull
    if (*(output[outB++].pData) != true) fail("pointerisnotnull1", __LINE__);
    if (*(output[outB++].pData) != false) fail("pointerisnotnull2", __LINE__);

    // tonull
    if (output[outCp].pData != NULL) fail("pointertonull1", __LINE__);
    if (output[outCp++].cbData != 0) fail("pointertonull2", __LINE__);

    // putsize
    // getsize
    if (*(output[outL++].pData) != 0) fail("pointergetsize1", __LINE__);
    if (*(output[outL++].pData) != 1) fail("pointergetsize2", __LINE__);
    if (*(output[outL++].pData) != 2) fail("pointergetsize3", __LINE__);
    if (*(output[outL++].pData) != bufferlen) fail("pointergetsize4", __LINE__);
    if (*(output[outL++].pData) != 0) fail("pointergetsize5", __LINE__);
    if (*(output[outL++].pData) != bufferlen) fail("pointergetsize6", __LINE__);

    // getmaxsize
    if (*(output[outL++].pData) != bufferlen) fail("pointergetsize7", __LINE__);
    if (*(output[outL++].pData) != bufferlen) fail("pointergetsize8", __LINE__);
    if (*(output[outL++].pData) != bufferlen) fail("pointergetsize9", __LINE__);
    if (*(output[outL++].pData) != bufferlen) fail("pointergetsize10", __LINE__);
    

    cout << "Calculator Warnings: " << c.warnings() << endl;

    delete [] cpInP;
    delete [] cpOutP;
    delete [] cpLoP;
    delete [] cpLiP;
    delete [] lInP;
    delete [] lOutP;
    delete [] lLoP;
    delete [] lLiP;
    delete [] bInP;
    delete [] bOutP;
    delete [] bLoP;
    delete [] bLiP;
    for (i = 0; i < lastPC; i++) {
        delete instP[i];
    }
    delete [] instP;
}

void
unitTestWarnings()
{
    printf("=========================================================\n");
    printf("=========================================================\n");
    printf("=====\n");
    printf("=====     unitTestWarnings()\n");
    printf("=====\n");
    printf("=========================================================\n");
    printf("=========================================================\n");

    bool isNullable = true;    // Can tuple contain nulls?
    int i, registersize = 3;

    TupleDescriptor tupleDesc;
    tupleDesc.clear();

    // Build up a description of what we'd like the tuple to look like
    StandardTypeDescriptorFactory typeFactory;
    int idx = 0;

    int floatIdx = idx;
    for (i=0;i < registersize; i++) {
        // floats
        StoredTypeDescriptor const &typeDesc = typeFactory.newDataType(STANDARD_TYPE_REAL);
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
    tupleAccessorFixedLiteral.compute(tupleDesc, TUPLE_FORMAT_ALL_NOT_NULL_AND_FIXED);
    tupleAccessorFixedInput.compute(tupleDesc, TUPLE_FORMAT_ALL_NOT_NULL_AND_FIXED);
    tupleAccessorFixedOutput.compute(tupleDesc, TUPLE_FORMAT_ALL_NOT_NULL_AND_FIXED);
    tupleAccessorFixedLocal.compute(tupleDesc, TUPLE_FORMAT_ALL_NOT_NULL_AND_FIXED);
    tupleAccessorFixedStatus.compute(tupleDesc, TUPLE_FORMAT_ALL_NOT_NULL_AND_FIXED);

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
    tupleAccessorFixedLiteral.setCurrentTupleBuf(pTupleBufFixedLiteral.get());
    tupleAccessorFixedInput.setCurrentTupleBuf(pTupleBufFixedInput.get());
    tupleAccessorFixedOutput.setCurrentTupleBuf(pTupleBufFixedOutput.get());
    tupleAccessorFixedLocal.setCurrentTupleBuf(pTupleBufFixedLocal.get());
    tupleAccessorFixedStatus.setCurrentTupleBuf(pTupleBufFixedStatus.get());

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

    // Set up some useful literals
    for(i=0; i < registersize; i++) {
        *(reinterpret_cast<float *>(const_cast<PBuffer>(literal[i].pData))) = i * 0.5;
        *(reinterpret_cast<float *>(const_cast<PBuffer>(output[i].pData))) = i * 2 + 1;
        *(reinterpret_cast<float *>(const_cast<PBuffer>(input[i].pData))) = i * 5.5 + 1;
        *(reinterpret_cast<float *>(const_cast<PBuffer>(local[i].pData))) = i * 3.3 + 1;
    }

    // Print out the nullable tuple
    TuplePrinter tuplePrinter;
    printf("Literals\n");
    tuplePrinter.print(cout, tupleDesc, literal);
    cout << endl;
    printf("\nInput\n");
    tuplePrinter.print(cout, tupleDesc, input);
    cout << endl;
    printf("\nOutput\n");
    tuplePrinter.print(cout, tupleDesc, output);
    cout << endl;
    printf("\nLocal\n");
    tuplePrinter.print(cout, tupleDesc, local);
    cout << endl;


    // predefine register references. a real compiler wouldn't do
    // something so regular and pre-determined. a compiler would
    // probably build these on the fly as it built each instruction.
    // predefine register references. a real compiler wouldn't do
    // something so regular and pre-determined
    RegisterRef<float> **fInP, **fOutP, **fLoP, **fLiP;

    fInP = new RegisterRef<float>*[registersize];
    fOutP = new RegisterRef<float>*[registersize];
    fLoP = new RegisterRef<float>*[registersize];
    fLiP = new RegisterRef<float>*[registersize];

    // Set up the Calculator
    Calculator c(0,0,0,0,0,0);
    c.outputRegisterByReference(false);

    // set up register references to symbolically point to 
    // their corresponding storage locations -- makes for easy test case
    // generation. again, a compiler wouldn't do things in quite
    // this way.
    for (i=0; i < registersize; i++) {
        fInP[i] = new RegisterRef<float>(RegisterReference::EInput,
                                         floatIdx + i,
                                         STANDARD_TYPE_REAL);
        c.appendRegRef(fInP[i]);
        fOutP[i] = new RegisterRef<float>(RegisterReference::EOutput,
                                          floatIdx + i,
                                          STANDARD_TYPE_REAL);
        c.appendRegRef(fOutP[i]);
        fLoP[i] = new RegisterRef<float>(RegisterReference::ELocal,
                                         floatIdx + i,
                                         STANDARD_TYPE_REAL);
        c.appendRegRef(fLoP[i]);
        fLiP[i] = new RegisterRef<float>(RegisterReference::ELiteral,
                                         floatIdx + i, 
                                         STANDARD_TYPE_REAL);
        c.appendRegRef(fLiP[i]);
    }


    // Set up storage for instructions
    // a real compiler would probably cons up instructions and insert them
    // directly into the calculator. keep an array of the instructions at
    // this level to allow printing of the program after execution, and other
    // debugging
    Instruction **instP;
    instP = new (Instruction *)[200];
    int pc = 0, outF = 0;
    
    StandardTypeDescriptorOrdinal isFloat = STANDARD_TYPE_REAL;

    // Force a warning
    instP[pc++] = new NativeDiv<float>(fOutP[outF++], fLiP[2], fLiP[0], isFloat);
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

    string out;
    for (i = 0; i < pc; i++) {
        instP[i]->describe(out, true);
        printf("[%2d] %s\n", i, out.c_str());
    }

    // Print out the output tuple
    printf("Output Tuple\n");
    tuplePrinter.print(cout, tupleDesc, output);
    cout << endl;

    cout << "Calculator Warnings: " << c.warnings() << endl;

    deque<CalcMessage>::iterator iter = c.mWarnings.begin();
    if (iter->pc != 0) fail("warning:pc", __LINE__);
    string expectederror("22012");
    if (expectederror.compare(iter->str)) 
        fail("warning:div by zero failed string wasn't as expected", __LINE__);
    string expectedwarningstring("[0]:PC=0 Code=22012 ");
    cout << "|" << expectedwarningstring << "|" << endl;
    
    if (expectedwarningstring.compare(c.warnings()))
        fail("warning:warning string wasn't as expected", __LINE__);

    // Replace the literal '0' with something benign
    pc = 0;
    outF = 0;
    instP[pc++] = new NativeDiv<float>(fOutP[outF++], fLiP[2], fLiP[2], isFloat);
    *(reinterpret_cast<float *>(const_cast<PBuffer>(literal[0].pData))) = 2;

    // Out[0] is now null, due to the div by zero error
    // Hack output tuple to something re-runable
    float horriblehack = 88;
    reinterpret_cast<float *>(const_cast<PBuffer>(output[0].pData)) = &horriblehack;

    printf("Rerunning calculator\n");
    
    c.bind(&input, &output);
    c.exec();

    cout << "Calculator Warnings: " << c.warnings() << endl;

    if (!c.mWarnings.empty()) 
        fail("warning:warning deque has data", __LINE__);
    if (c.warnings().compare(""))
        fail("warning:warning string empty", __LINE__);

    delete [] fInP;
    delete [] fOutP;
    delete [] fLoP;
    delete [] fLiP;

    for (i = 0; i < lastPC; i++) {
        delete instP[i];
    }
    delete [] instP;

}

void
unitTestPointerCache()
{
    printf("=========================================================\n");
    printf("=========================================================\n");
    printf("=====\n");
    printf("=====     unitTestPointerCache()\n");
    printf("=====\n");
    printf("=========================================================\n");
    printf("=========================================================\n");

    bool isNullable = true;    // Can tuple contain nulls?
    int i, registersize = 10;

    TupleDescriptor tupleDesc;
    tupleDesc.clear();

    // Build up a description of what we'd like the tuple to look like
    StandardTypeDescriptorFactory typeFactory;
    int idx = 0;

    int doubleIdx = idx;
    for (i=0;i < registersize; i++) {
        // doubles
        StoredTypeDescriptor const &typeDesc = typeFactory.newDataType(STANDARD_TYPE_DOUBLE);
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
    tupleAccessorFixedLiteral.compute(tupleDesc, TUPLE_FORMAT_ALL_NOT_NULL_AND_FIXED);
    tupleAccessorFixedInput.compute(tupleDesc, TUPLE_FORMAT_ALL_NOT_NULL_AND_FIXED);
    tupleAccessorFixedOutput.compute(tupleDesc, TUPLE_FORMAT_ALL_NOT_NULL_AND_FIXED);
    tupleAccessorFixedLocal.compute(tupleDesc, TUPLE_FORMAT_ALL_NOT_NULL_AND_FIXED);
    tupleAccessorFixedStatus.compute(tupleDesc, TUPLE_FORMAT_ALL_NOT_NULL_AND_FIXED);

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
    tupleAccessorFixedLiteral.setCurrentTupleBuf(pTupleBufFixedLiteral.get());
    tupleAccessorFixedInput.setCurrentTupleBuf(pTupleBufFixedInput.get());
    tupleAccessorFixedOutput.setCurrentTupleBuf(pTupleBufFixedOutput.get());
    tupleAccessorFixedLocal.setCurrentTupleBuf(pTupleBufFixedLocal.get());
    tupleAccessorFixedStatus.setCurrentTupleBuf(pTupleBufFixedStatus.get());

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

    // Set up some useful literals
    for(i=0; i < registersize; i++) {
        *(reinterpret_cast<double *>(const_cast<PBuffer>(literal[i].pData))) = i * 0.5;
        *(reinterpret_cast<double *>(const_cast<PBuffer>(output[i].pData))) = i * 2 + 1;
        *(reinterpret_cast<double *>(const_cast<PBuffer>(input[i].pData))) = i * 5.5 + 1;
        *(reinterpret_cast<double *>(const_cast<PBuffer>(local[i].pData))) = i * 3.3 + 1;
    }

    // Print out the nullable tuple
    TuplePrinter tuplePrinter;
    printf("Literals\n");
    tuplePrinter.print(cout, tupleDesc, literal);
    cout << endl;
    printf("\nInput\n");
    tuplePrinter.print(cout, tupleDesc, input);
    cout << endl;
    printf("\nOutput\n");
    tuplePrinter.print(cout, tupleDesc, output);
    cout << endl;
    printf("\nLocal\n");
    tuplePrinter.print(cout, tupleDesc, local);
    cout << endl;


    // predefine register references. a real compiler wouldn't do
    // something so regular and pre-determined. a compiler would
    // probably build these on the fly as it built each instruction.
    // predefine register references. a real compiler wouldn't do
    // something so regular and pre-determined
    RegisterRef<double> **fInP, **fOutP, **fLoP, **fLiP;

    fInP = new RegisterRef<double>*[registersize];
    fOutP = new RegisterRef<double>*[registersize];
    fLoP = new RegisterRef<double>*[registersize];
    fLiP = new RegisterRef<double>*[registersize];

    // Set up the Calculator
    Calculator c(0,0,0,0,0,0);
    c.outputRegisterByReference(false);

    // set up register references to symbolically point to 
    // their corresponding storage locations -- makes for easy test case
    // generation. again, a compiler wouldn't do things in quite
    // this way.
    for (i=0; i < registersize; i++) {
        fInP[i] = new RegisterRef<double>(RegisterReference::EInput,
                                          doubleIdx + i,
                                          STANDARD_TYPE_DOUBLE);
        c.appendRegRef(fInP[i]);
        fOutP[i] = new RegisterRef<double>(RegisterReference::EOutput, 
                                           doubleIdx + i,
                                           STANDARD_TYPE_DOUBLE);
        c.appendRegRef(fOutP[i]);
        fLoP[i] = new RegisterRef<double>(RegisterReference::ELocal,
                                          doubleIdx + i, 
                                          STANDARD_TYPE_DOUBLE);
        c.appendRegRef(fLoP[i]);
        fLiP[i] = new RegisterRef<double>(RegisterReference::ELiteral,
                                          doubleIdx + i,
                                          STANDARD_TYPE_DOUBLE);
        c.appendRegRef(fLiP[i]);
    }


    // Set up storage for instructions
    // a real compiler would probably cons up instructions and insert them
    // directly into the calculator. keep an array of the instructions at
    // this level to allow printing of the program after execution, and other
    // debugging
    Instruction **instP;
    instP = new (Instruction *)[200];
    int pc = 0, outF = 0, liF = 0;
    
    
    StandardTypeDescriptorOrdinal isDouble = STANDARD_TYPE_DOUBLE;

    // copy some of the literals into the output register
    for (i = 0; i < (registersize / 2) - 1 ; i++) {
        instP[pc++] = new NativeMove<double>(fOutP[outF++], fLiP[liF++], isDouble);
    }
    // copy some of the locals into the output register
    for (i = 0; i < (registersize / 2) - 1 ; i++) {
        instP[pc++] = new NativeMove<double>(fOutP[outF++], fLoP[liF++], isDouble);
    }
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

    string out;
    for (i = 0; i < pc; i++) {
        instP[i]->describe(out, true);
        printf("[%2d] %s\n", i, out.c_str());
    }

    // Print out the output tuple
    printf("Output Tuple\n");
    tuplePrinter.print(cout, tupleDesc, output);
    cout << endl;

    cout << "Calculator Warnings: " << c.warnings() << endl;

    outF = liF = 0;
    for (i = 0; i < (registersize / 2) - 1 ; i++) {
        if (*(reinterpret_cast<double *>(const_cast<PBuffer>(output[outF++].pData)))
            != reinterpret_cast<double>(i * 0.5)) {
            fail("pointercache1", __LINE__);
        }
    }
    for (i = 0; i < (registersize / 2) - 1 ; i++) {
        if ((*(reinterpret_cast<double *>(const_cast<PBuffer>(output[outF++].pData)))
             - reinterpret_cast<double>(outF * 3.3 + 1)) > 0.000001) {
            fail("pointercache2", __LINE__);
        }
    }
    
    // OK, now be mean and yank the literals right out from under
    // Calculator. The memory is still allocated and available
    // for the cached pointers. Note that the calculator will have
    // no reason to reset these pointers as they weren't re-pointed
    // or set to null
    for(i=0; i < registersize; i++) {
        const_cast<PBuffer>(literal[i].pData) = NULL;
        const_cast<PBuffer>(local[i].pData) = NULL;
    }

    printf("Rerunning calculator\n");
    
    c.bind(&input, &output);
    c.exec();

    outF = liF = 0;
    for (i = 0; i < (registersize / 2) - 1 ; i++) {
        if (*(reinterpret_cast<double *>(const_cast<PBuffer>(output[outF++].pData)))
            != reinterpret_cast<double>(i * 0.5)) {
            fail("pointercache3", __LINE__);
        }
    }
    for (i = 0; i < (registersize / 2) - 1 ; i++) {
        if ((*(reinterpret_cast<double *>(const_cast<PBuffer>(output[outF++].pData)))
             - reinterpret_cast<double>(outF * 3.3 + 1)) > 0.000001) {
            fail("pointercache4", __LINE__);
        }
    }

    cout << "Calculator Warnings: " << c.warnings() << endl;

    delete [] fInP;
    delete [] fOutP;
    delete [] fLoP;
    delete [] fLiP;

    for (i = 0; i < lastPC; i++) {
        delete instP[i];
    }
    delete [] instP;

}

void
unitTestNullableLocal()
{
    printf("=========================================================\n");
    printf("=========================================================\n");
    printf("=====\n");
    printf("=====     unitTestNullableLocal()\n");
    printf("=====\n");
    printf("=========================================================\n");
    printf("=========================================================\n");

    bool isNullable = true;    // Can tuple contain nulls?
    int i, registersize = 10;
    static int bufferlen = 8;

    TupleDescriptor tupleDesc;
    tupleDesc.clear();

    // Build up a description of what we'd like the tuple to look like
    StandardTypeDescriptorFactory typeFactory;
    int idx = 0;

    const int pointerIdx = idx;
    for (i=0;i < registersize; i++) {
        // pointers in first "half"
        StoredTypeDescriptor const &typeDesc = typeFactory.newDataType(STANDARD_TYPE_VARCHAR);
        // tell descriptor the size 
        tupleDesc.push_back(TupleAttributeDescriptor(typeDesc,
                                                     isNullable,
                                                     bufferlen));
        idx++;
    
    }
    const int boolIdx = idx;
    for (i=0;i < registersize; i++) {
        // booleans in second "half"
        StoredTypeDescriptor const &typeDesc = typeFactory.newDataType(STANDARD_TYPE_UINT_8);
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
    tupleAccessorFixedLiteral.compute(tupleDesc, TUPLE_FORMAT_ALL_NOT_NULL_AND_FIXED);
    tupleAccessorFixedInput.compute(tupleDesc, TUPLE_FORMAT_ALL_NOT_NULL_AND_FIXED);
    tupleAccessorFixedOutput.compute(tupleDesc, TUPLE_FORMAT_ALL_NOT_NULL_AND_FIXED);
    tupleAccessorFixedLocal.compute(tupleDesc, TUPLE_FORMAT_ALL_NOT_NULL_AND_FIXED);
    tupleAccessorFixedStatus.compute(tupleDesc, TUPLE_FORMAT_ALL_NOT_NULL_AND_FIXED);

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
    tupleAccessorFixedLiteral.setCurrentTupleBuf(pTupleBufFixedLiteral.get());
    tupleAccessorFixedInput.setCurrentTupleBuf(pTupleBufFixedInput.get());
    tupleAccessorFixedOutput.setCurrentTupleBuf(pTupleBufFixedOutput.get());
    tupleAccessorFixedLocal.setCurrentTupleBuf(pTupleBufFixedLocal.get());
    tupleAccessorFixedStatus.setCurrentTupleBuf(pTupleBufFixedStatus.get());

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

    // Set up some useful literals
    for(i=0; i < registersize; i++) {
        char num[16];
        sprintf(num, "%04d", i);
        char* ptr = reinterpret_cast<char *>(const_cast<PBuffer>(literal[i].pData));
        memset(ptr, 'C', bufferlen); // VARCHAR is not null terminated
        memcpy(ptr, num, 4); // copy number, but not null

        // Put some data other tuples as well
        ptr = reinterpret_cast<char *>(const_cast<PBuffer>(input[i].pData));
        memset(ptr, 'I', bufferlen); // VARCHAR is not null terminated
        memcpy(ptr, num, 4); // copy number, but not null

        ptr = reinterpret_cast<char *>(const_cast<PBuffer>(output[i].pData));
        memset(ptr, 'O', bufferlen); // VARCHAR is not null terminated
        memcpy(ptr, num, 4); // copy number, but not null

        ptr = reinterpret_cast<char *>(const_cast<PBuffer>(local[i].pData));
        memset(ptr, 'L', bufferlen); // VARCHAR is not null terminated
        memcpy(ptr, num, 4); // copy number, but not null
    }

    int falseIdx = 0;
    int trueIdx = 1;
    i = registersize;
    *(reinterpret_cast<bool *>(const_cast<PBuffer>(literal[i].pData))) = false;
    for (i++; i < registersize*2; i++) {
        *(reinterpret_cast<bool *>(const_cast<PBuffer>(literal[i].pData))) = true;
    }

    // null out last element of each type
    int nullidx = registersize-1;
    literal[nullidx].pData = NULL;
    input[nullidx].pData = NULL;
    output[nullidx].pData = NULL;
    local[nullidx].pData = NULL;

    // Print out the nullable tuple
    TuplePrinter tuplePrinter;
    printf("Literals\n");
    tuplePrinter.print(cout, tupleDesc, literal);
    cout << endl;
    printf("\nInput\n");
    tuplePrinter.print(cout, tupleDesc, input);
    cout << endl;
    printf("\nOutput\n");
    tuplePrinter.print(cout, tupleDesc, output);
    cout << endl;
    printf("\nLocal\n");
    tuplePrinter.print(cout, tupleDesc, local);
    cout << endl;

    // predefine register references. a real compiler wouldn't do
    // something so regular and pre-determined. a compiler would
    // probably build these on the fly as it built each instruction.
    // predefine register references. a real compiler wouldn't do
    // something so regular and pre-determined
    RegisterRef<char *> **cpInP, **cpOutP, **cpLiP;
    RegisterRef<bool> **bInP, **bOutP, **bLiP;

    RegisterRef<char *> **cpLoP;
    RegisterRef<bool> **bLoP;

    cpInP = new RegisterRef<char *>*[registersize];
    cpOutP = new RegisterRef<char *>*[registersize];
    cpLoP = new RegisterRef<char *>*[registersize];
    cpLiP = new RegisterRef<char *>*[registersize];

    bInP = new RegisterRef<bool>*[registersize];
    bOutP = new RegisterRef<bool>*[registersize];
    bLoP = new RegisterRef<bool>*[registersize];
    bLiP = new RegisterRef<bool>*[registersize];

    // Set up the Calculator
    Calculator c(0,0,0,0,0,0);
    c.outputRegisterByReference(false);

    // set up register references to symbolically point to 
    // their corresponding storage locations -- makes for easy test case
    // generation. again, a compiler wouldn't do things in quite
    // this way.
    for (i=0; i < registersize; i++) {
        cpInP[i] = new RegisterRef<char *>(RegisterReference::EInput,
                                           pointerIdx + i,
                                           STANDARD_TYPE_VARCHAR);
        c.appendRegRef(cpInP[i]);
        cpOutP[i] = new RegisterRef<char *>(RegisterReference::EOutput,
                                            pointerIdx + i,
                                            STANDARD_TYPE_VARCHAR);
        c.appendRegRef(cpOutP[i]);
        cpLoP[i] = new RegisterRef<char *>(RegisterReference::ELocal, 
                                           pointerIdx + i,
                                           STANDARD_TYPE_VARCHAR);
        c.appendRegRef(cpLoP[i]);
        cpLiP[i] = new RegisterRef<char *>(RegisterReference::ELiteral, 
                                           pointerIdx + i,
                                           STANDARD_TYPE_VARCHAR);
        c.appendRegRef(cpLiP[i]);

        bInP[i] = new RegisterRef<bool>(RegisterReference::EInput,
                                        boolIdx + i,
                                        STANDARD_TYPE_BOOL);
        c.appendRegRef(bInP[i]);
        bOutP[i] = new RegisterRef<bool>(RegisterReference::EOutput,
                                         boolIdx + i, 
                                         STANDARD_TYPE_BOOL);
        c.appendRegRef(bOutP[i]);
        bLoP[i] = new RegisterRef<bool>(RegisterReference::ELocal,
                                        boolIdx + i,
                                        STANDARD_TYPE_BOOL);
        c.appendRegRef(bLoP[i]);
        bLiP[i] = new RegisterRef<bool>(RegisterReference::ELiteral,
                                        boolIdx + i,
                                        STANDARD_TYPE_BOOL);
        c.appendRegRef(bLiP[i]);
    }

    // Set up storage for instructions
    // a real compiler would probably cons up instructions and insert them
    // directly into the calculator. keep an array of the instructions at
    // this level to allow printing of the program after execution, and other
    // debugging
    Instruction **instP;
    instP = new (Instruction *)[200];
    int pc = 0, outCp = 0, outB = 0;
    StandardTypeDescriptorOrdinal isVC = STANDARD_TYPE_VARCHAR;
    
    // set success flag to false
    instP[pc++] = new BoolMove(bOutP[0], bLiP[falseIdx]);

    // test booleans and thus all natives

    // check that boolean local register 0 is not null    
    instP[pc++] = new BoolIsNotNull(bLoP[1], bLoP[0]);
    instP[pc] = new JumpTrue(pc+2, bLoP[1]); pc++;
    instP[pc++] = new ReturnInstruction();
    // write something into non-null 0
    // will cause crash if 0 happened to be null
    instP[pc++] = new BoolMove(bLoP[0], bLiP[trueIdx]);
    // set local 0 to null
    instP[pc++] = new BoolToNull(bLoP[0]);
    // check local 0 is null
    instP[pc++] = new BoolIsNull(bLoP[2], bLoP[0]);
    instP[pc] = new JumpTrue(pc+2, bLoP[2]); pc++;
    instP[pc++] = new ReturnInstruction();

    // test pointers

    // check that pointer local register 0 is not null    
    instP[pc++] = new BoolPointerIsNotNull<char *>(bLoP[3], cpLoP[0], isVC);
    instP[pc] = new JumpTrue(pc+2, bLoP[3]); pc++;
    instP[pc++] = new ReturnInstruction();
    // copy local 0 to output register, so we can see it
    instP[pc++] = new PointerMove<char *>(cpOutP[0], cpLoP[0], isVC);
    // write something into non-null 0
    // will cause crash if 0 happened to be null
    instP[pc++] = new PointerMove<char *>(cpLoP[0], cpLiP[1], isVC);
    // set local 0 to null
    instP[pc++] = new PointerToNull<char *>(cpLoP[0], isVC);
    // copy local 0 to output register, so we can see it
    instP[pc++] = new PointerMove<char *>(cpOutP[1], cpLoP[0], isVC);
    // check local 0 is null
    instP[pc++] = new BoolPointerIsNull<char *>(bLoP[4], cpLoP[0], isVC);
    instP[pc] = new JumpTrue(pc+2, bLoP[4]); pc++;
    instP[pc++] = new ReturnInstruction();

    // set success
    instP[pc++] = new BoolMove(bOutP[0], bLiP[trueIdx]);
    instP[pc++] = new ReturnInstruction();
    int lastPC = pc;

    for (i = 0; i < pc; i++) {
        c.appendInstruction(instP[i]);
    }

    printf("first run\n");
    
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
    
    printf("after first run\n");

    string out;
    for (i = 0; i < pc; i++) {
        instP[i]->describe(out, true);
        printf("[%2d] %s\n", i, out.c_str());
    }

    // Print out the output tuple
    printf("Output Tuple\n");
    tuplePrinter.print(cout, tupleDesc, output);
    cout << endl;
    printf("Local Tuple\n");
    tuplePrinter.print(cout, tupleDesc, local);
    cout << endl;

    outCp = 0; // now indexes into output tuple, not outputregisterref
    outB = boolIdx;  // now indexs into output tuple, not outputregisterref
    
    // check status flag in output
    if (*(output[boolIdx].pData) != true) fail("nullablelocal1", __LINE__);
    // check that actual pointer was not nulled out
    if (local[boolIdx].pData == NULL) fail("nullablelocal2", __LINE__);

    // make sure that previously null pointers weren't somehow 'un-nulled'
    if (literal[nullidx].pData != NULL) fail("nullablelocal3", __LINE__);
    if (input[nullidx].pData != NULL) fail("nullablelocal4", __LINE__);
    if (output[nullidx].pData != NULL) fail("nullablelocal5", __LINE__);
    if (local[nullidx].pData != NULL) fail("nullablelocal6", __LINE__);


    printf("second run\n");
    
    c.bind(&input, &output);
    c.exec();

    printf("after second run\n");

    // Print out the output tuple
    printf("Output Tuple\n");
    tuplePrinter.print(cout, tupleDesc, output);
    cout << endl;
    printf("Local Tuple\n");
    tuplePrinter.print(cout, tupleDesc, local);
    cout << endl;


    // check status flag in output
    if (*(output[boolIdx].pData) != true) fail("nullablelocal7", __LINE__);
    // check that actual pointer was not nulled out
    if (local[boolIdx].pData == NULL) fail("nullablelocal8", __LINE__);

    // make sure that previously null pointers weren't somehow 'un-nulled'
    if (literal[nullidx].pData != NULL) fail("nullablelocal9", __LINE__);
    if (input[nullidx].pData != NULL) fail("nullablelocal10", __LINE__);
    if (output[nullidx].pData != NULL) fail("nullablelocal11", __LINE__);
    if (local[nullidx].pData != NULL) fail("nullablelocal12", __LINE__);

    delete [] cpInP;
    delete [] cpOutP;
    delete [] cpLoP;
    delete [] cpLiP;

    delete [] bInP;
    delete [] bOutP;
    delete [] bLoP;
    delete [] bLiP;
    for (i = 0; i < lastPC; i++) {
        delete instP[i];
    }
    delete [] instP;
}

void
unitTestStatusRegister()
{
    printf("=========================================================\n");
    printf("=========================================================\n");
    printf("=====\n");
    printf("=====     unitTestStatusRegister()\n");
    printf("=====\n");
    printf("=========================================================\n");
    printf("=========================================================\n");

    bool isNullable = true;    // Can tuple contain nulls?
    int i, registersize = 10;

    TupleDescriptor tupleDesc;
    tupleDesc.clear();

    // Build up a description of what we'd like the tuple to look like
    StandardTypeDescriptorFactory typeFactory;
    int idx = 0;

    int u_int16Idx = idx;
    for (i=0;i < registersize; i++) {
        // u_int16 (short)
        StoredTypeDescriptor const &typeDesc = typeFactory.newDataType(STANDARD_TYPE_UINT_16);
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
    tupleAccessorFixedLiteral.compute(tupleDesc, TUPLE_FORMAT_ALL_NOT_NULL_AND_FIXED);
    tupleAccessorFixedInput.compute(tupleDesc, TUPLE_FORMAT_ALL_NOT_NULL_AND_FIXED);
    tupleAccessorFixedOutput.compute(tupleDesc, TUPLE_FORMAT_ALL_NOT_NULL_AND_FIXED);
    tupleAccessorFixedLocal.compute(tupleDesc, TUPLE_FORMAT_ALL_NOT_NULL_AND_FIXED);
    tupleAccessorFixedStatus.compute(tupleDesc, TUPLE_FORMAT_ALL_NOT_NULL_AND_FIXED);

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
    tupleAccessorFixedLiteral.setCurrentTupleBuf(pTupleBufFixedLiteral.get());
    tupleAccessorFixedInput.setCurrentTupleBuf(pTupleBufFixedInput.get());
    tupleAccessorFixedOutput.setCurrentTupleBuf(pTupleBufFixedOutput.get());
    tupleAccessorFixedLocal.setCurrentTupleBuf(pTupleBufFixedLocal.get());
    tupleAccessorFixedStatus.setCurrentTupleBuf(pTupleBufFixedStatus.get());

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

    // Set up some useful literals
    for(i=0; i < registersize; i++) {
        *(reinterpret_cast<uint16_t *>(const_cast<PBuffer>(literal[i].pData))) = i;
        *(reinterpret_cast<uint16_t *>(const_cast<PBuffer>(output[i].pData))) = i * 2 + 1;
        *(reinterpret_cast<uint16_t *>(const_cast<PBuffer>(input[i].pData))) = i * 5 + 2;
        *(reinterpret_cast<uint16_t *>(const_cast<PBuffer>(local[i].pData))) = i * 10 + 3;
        *(reinterpret_cast<uint16_t *>(const_cast<PBuffer>(status[i].pData))) = i * 15 + 4;
    }

    // Print out the nullable tuple
    TuplePrinter tuplePrinter;
    printf("Literals\n");
    tuplePrinter.print(cout, tupleDesc, literal);
    cout << endl;
    printf("\nInput\n");
    tuplePrinter.print(cout, tupleDesc, input);
    cout << endl;
    printf("\nOutput\n");
    tuplePrinter.print(cout, tupleDesc, output);
    cout << endl;
    printf("\nLocal\n");
    tuplePrinter.print(cout, tupleDesc, local);
    printf("\nStatus\n");
    tuplePrinter.print(cout, tupleDesc, status);
    cout << endl;


    // predefine register references. a real compiler wouldn't do
    // something so regular and pre-determined. a compiler would
    // probably build these on the fly as it built each instruction.
    // predefine register references. a real compiler wouldn't do
    // something so regular and pre-determined
    RegisterRef<uint16_t> **fInP, **fOutP, **fLoP, **fLiP, **fStP;

    fInP = new RegisterRef<uint16_t>*[registersize];
    fOutP = new RegisterRef<uint16_t>*[registersize];
    fLoP = new RegisterRef<uint16_t>*[registersize];
    fLiP = new RegisterRef<uint16_t>*[registersize];
    fStP = new RegisterRef<uint16_t>*[registersize];

    // Set up the Calculator
    Calculator c(0,0,0,0,0,0);
    c.outputRegisterByReference(false);

    // set up register references to symbolically point to 
    // their corresponding storage locations -- makes for easy test case
    // generation. again, a compiler wouldn't do things in quite
    // this way.
    for (i=0; i < registersize; i++) {
        fInP[i] = new RegisterRef<uint16_t>(RegisterReference::EInput,
                                            u_int16Idx + i,
                                            STANDARD_TYPE_UINT_16);
        c.appendRegRef(fInP[i]);
        fOutP[i] = new RegisterRef<uint16_t>(RegisterReference::EOutput,
                                             u_int16Idx + i, 
                                             STANDARD_TYPE_UINT_16);
        c.appendRegRef(fOutP[i]);
        fLoP[i] = new RegisterRef<uint16_t>(RegisterReference::ELocal,
                                            u_int16Idx + i,
                                            STANDARD_TYPE_UINT_16);
        c.appendRegRef(fLoP[i]);
        fLiP[i] = new RegisterRef<uint16_t>(RegisterReference::ELiteral, 
                                            u_int16Idx + i,
                                            STANDARD_TYPE_UINT_16);
        c.appendRegRef(fLiP[i]);
        fStP[i] = new RegisterRef<uint16_t>(RegisterReference::EStatus,
                                            u_int16Idx + i,
                                            STANDARD_TYPE_UINT_16);
        c.appendRegRef(fStP[i]);
    }


    // Set up storage for instructions
    // a real compiler would probably cons up instructions and insert them
    // directly into the calculator. keep an array of the instructions at
    // this level to allow printing of the program after execution, and other
    // debugging
    Instruction **instP;
    instP = new (Instruction *)[200];
    int pc = 0, statusS = 0, liS = 0;

    StandardTypeDescriptorOrdinal isU_Int16 = STANDARD_TYPE_UINT_16;

    // copy some of the literals into the status register
    for (i = 0; i < registersize - 1 ; i++) {
        instP[pc++] = new NativeMove<uint16_t>(fStP[statusS++], fLiP[liS++], isU_Int16);
    }

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

    string out;
    for (i = 0; i < pc; i++) {
        instP[i]->describe(out, true);
        printf("[%2d] %s\n", i, out.c_str());
    }

    // Print out the output tuple
    printf("Output Tuple\n");
    tuplePrinter.print(cout, tupleDesc, output);
    cout << endl;
    printf("Status Tuple\n");
    tuplePrinter.print(cout, tupleDesc, status);
    cout << endl;

    cout << "Calculator Warnings: " << c.warnings() << endl;

    statusS = liS = 0;
    for (i = 0; i < registersize - 1 ; i++) {
        if (*(reinterpret_cast<uint16_t *>(const_cast<PBuffer>(status[statusS++].pData)))
            != static_cast<uint16_t>(i)) {
            fail("statusregister1", __LINE__);
        }
    }
    
    delete [] fInP;
    delete [] fOutP;
    delete [] fLoP;
    delete [] fLiP;
    delete [] fStP;

    for (i = 0; i < lastPC; i++) {
        delete instP[i];
    }
    delete [] instP;

}

int main(int argc, char* argv[])
{
    ProgramName = argv[0];

    CalcInit::instance();

    unitTestBool();
    unitTestLong();
    unitTestFloat();
    unitTestWarnings();
    unitTestPointer();
    unitTestPointerCache();
    unitTestNullableLocal();
    unitTestStatusRegister();

    printf("all tests passed\n");
    exit(0);
}

