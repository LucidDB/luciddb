/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2004 The Eigenbase Project
// Copyright (C) 2004 SQLstream, Inc.
// Copyright (C) 2009 Dynamo BI Corporation
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
#include "fennel/calculator/BoolInstruction.h"
#include "fennel/calculator/BoolNativeInstruction.h"
#include "fennel/calculator/Calculator.h"
#include "fennel/calculator/IntegralNativeInstruction.h"
#include "fennel/calculator/JumpInstruction.h"
#include "fennel/calculator/NativeInstruction.h"
#include "fennel/calculator/NativeNativeInstruction.h"
#include "fennel/calculator/ReturnInstruction.h"
#include "fennel/calculator/ExtendedInstruction.h"
#include "fennel/calculator/InstructionCommon.h"
#include <stdlib.h>
#include <stdio.h>
#include <string>
#include <boost/scoped_array.hpp>
#include <boost/test/unit_test_suite.hpp>
#include <limits>

#include <math.h>
using namespace std;
using namespace fennel;
char *ProgramName;
void
fail(const char *str, int line) {
    assert(ProgramName);
    assert(str);
    printf("%s: unit test failed: |%s| line %d\n", ProgramName, str, line);
    exit(-1);
}
// ----------------------------------------------------------------------
// conversion functions
// ----------------------------------------------------------------------
void convertDoubleToFloat(
    RegisterRef<float>* regOut,
    RegisterRef<double>* regIn)
{
    regOut->value((float)regIn->value());
}
void convertFloatToDouble(
    RegisterRef<double>* regOut,
    RegisterRef<float>* regIn)
{
    regOut->value((double)regIn->value());
}
void convertFloatToInt(
    RegisterRef<int>* regOut,
    RegisterRef<float>* regIn)
{
    regOut->value((int)regIn->value());
}
void convertIntToFloat(
    RegisterRef<float>* regOut,
    RegisterRef<int>* regIn)
{
    regOut->value((float)regIn->value());
}
/**
 * Assigns "input" * 10 ^ "exponent" to "result".
 */
void convertDecimal(
    RegisterRef<int32_t>* resultReg,
    RegisterRef<int32_t>* inputReg,
    RegisterRef<int32_t>* exponentReg)
{
    int32_t in = inputReg->value();
    int32_t exp = exponentReg->value();
    int32_t result = in;
    if (exp < 0) {
        while (exp++ < 0) {
            result /= 10;
        }
    } else {
        while (exp-- > 0) {
            result *= 10;
        }
    }
    resultReg->value(result);
}

void convertStringToExactNumber(
    RegisterRef<int32_t>* regOut,
    RegisterRef<char *>* regIn)
{
#if 0
    // TODO: Wrap this code in
    uint srcL = regIn->getS();
    // TODO: Change the following proof-of-concept code into
    // TODO: something real.
    char *nullTermStr = new char[srcL + 1];
    nullTermStr[srcL + 1] = 0;
    memcpy(nullTermStr, regIn->pointer(), srcL);
    regOut->value(strtol(nullTermStr, 0, 10));
    delete [] nullTermStr;
#endif
#if 0

    // TODO: Nope this is a disaster JR 6/07 (valueToString() returns "Unimpl");
    const char *pString = regIn->valueToString().c_str();
    assert(pString);
    int iValue = atoi(pString);
    regOut->value(iValue);
#endif

    // Try original pointer casting code updated to new class interface
    // This code is the same as above
    uint srcL = regIn->stringLength();
    char *nullTermStr = new char[srcL + 1];
    nullTermStr[srcL] = 0;
    memcpy(nullTermStr, regIn->pointer(), srcL);
    regOut->value(strtol(nullTermStr, 0, 10));
    delete [] nullTermStr;
}

#if 0
// TODO: JR 6/07 removing this
void convertExactNumberToString(
    RegisterRef<char *>* regOut,
    RegisterRef<int>* regIn)
{
#if 1
    // TODO: Change the following proof-of-concept code into
    // TODO: something real.
    char *nullTermStr = new char[256];
    sprintf(nullTermStr, "%d", regIn->value());

    uint dstL = regOut->storage();
    uint newL = strlen(nullTermStr);

    printf("dstL = %d  newL = %d\n", dstL, newL);

    if (newL > dstL) {
        // TODO: Must check right space padding to see what, if
        // anything valid is truncated before going all wild and
        // throwing exception
        assert(0);        // TODO: Must have a valid pc here!
        // SQL99 Part 2 Section 22.1 22-001 "string data, right truncation"
        memcpy(regOut->pointer(), nullTermStr, dstL);
        regOut->putS(dstL);
        throw CalcMessage("22001", 0); // TODO: PC is bogus
        printf("ConvertExactNumberToString\n");
        assert(newL <= dstL);
    }

    regOut->putS(newL);
    memcpy(regOut->pointer(), nullTermStr, newL);
    delete [] nullTermStr;
#endif
#if 0
// TODO: JR 6/07 ... valueToStringis not implemented yet ...
// re-enabled the above ...
    const char *pString = regIn->valueToString().c_str();
    assert(pString);
    regOut->value(const_cast<char*>(pString));
#endif
}
#endif

void convertStringToFloat(
    RegisterRef<float>* regOut,
    RegisterRef<char *>* regIn)
{
    //*regOut = strtof(*regIn, (char **)NULL);
}

void convertFloatToString(
    RegisterRef<char *>* regOut,
    RegisterRef<float>* regIn)
{
    //*str = itoa ((int) *approx);
}

void convertStringToDouble(
    RegisterRef<double>* regOut,
    RegisterRef<char *>* regIn)
{
    //*regOut = strtod(*regIn, (char **)NULL);
}

void convertDoubleToString(
    RegisterRef<char *>* regOut,
    RegisterRef<double>* regIn)
{}


// ----------------------------------------------------------------------
// -- Test driver
// ----------------------------------------------------------------------
/**
 * Subclass of Calculator specialized for testing extended
 * instructions. The Calculator initializes itself with registers
 * appropriate to the instruction, the caller then sets the register
 * values, and executes the program, and checks the result.
 */
class TestCalculator : public Calculator {
    TupleDescriptor _tupleDescLiteral;
    TupleDescriptor _tupleDescInput;
    TupleDescriptor _tupleDescOutput;
    TupleDescriptor _tupleDescLocal;
    TupleDescriptor _tupleDescStatus;
    bool _isNullable;
    ExtendedInstructionDef *_instrDef;
    TupleAccessor _tupleAccessorLiteral;
    TupleAccessor _tupleAccessorInput;
    TupleAccessor _tupleAccessorOutput;
    TupleAccessor _tupleAccessorLocal;
    TupleAccessor _tupleAccessorStatus;
    boost::scoped_array<FixedBuffer> _pTupleBufLiteral;
    boost::scoped_array<FixedBuffer> _pTupleBufInput;
    boost::scoped_array<FixedBuffer> _pTupleBufOutput;
    boost::scoped_array<FixedBuffer> _pTupleBufLocal;
    boost::scoped_array<FixedBuffer> _pTupleBufStatus;
    TupleData _tupleDataLiteral;
    TupleData _tupleDataInput;
    TupleData _tupleDataOutput;
    TupleData _tupleDataLocal;
    TupleData _tupleDataStatus;
public:
    TestCalculator(
        DynamicParamManager *pdpm,
        bool isNullable,
        ExtendedInstructionDef *instrDef)
        : Calculator(pdpm, 0, 0, 0, 0, 0, 0),
          _isNullable(isNullable),
          _instrDef(instrDef),
          _pTupleBufLiteral(NULL),
          _pTupleBufInput(NULL),
          _pTupleBufOutput(NULL),
          _pTupleBufLocal(NULL),
          _pTupleBufStatus(NULL),
          _tupleDataLiteral(),
          _tupleDataInput(),
          _tupleDataOutput(),
          _tupleDataLocal(),
          _tupleDataStatus()
    {
        setUp();
    }

    void setUp() {
        const vector<StandardTypeDescriptorOrdinal> parameterTypes =
            _instrDef->getParameterTypes();
        // assume first parameter is out, rest are in
        StandardTypeDescriptorFactory typeFactory;
        for (uint i = 0; i < parameterTypes.size(); i++) {
            StoredTypeDescriptor const &typeDesc =
                typeFactory.newDataType(parameterTypes[i]);
            if (i == 0) {
                // 0th parameter is "OUT"
                _tupleDescOutput.push_back(
                    TupleAttributeDescriptor(typeDesc, _isNullable));
            } else if (i > 0) {
                // other parameters are "IN"
                _tupleDescInput.push_back(
                    TupleAttributeDescriptor(typeDesc, _isNullable));
            }
        }
        // Create a tuple accessor from the description
        //
        // Note: Must use a NOT_NULL_AND_FIXED accessor when creating
        // a tuple out of the air like this, otherwise unmarshal()
        // does not know what to do. If you need a STANDARD type tuple
        // that supports nulls, it has to be built as a copy.
        _tupleAccessorLiteral.compute(
            _tupleDescLiteral, TUPLE_FORMAT_ALL_FIXED);
        _tupleAccessorInput.compute(_tupleDescInput, TUPLE_FORMAT_ALL_FIXED);
        _tupleAccessorOutput.compute(_tupleDescOutput, TUPLE_FORMAT_ALL_FIXED);
        _tupleAccessorLocal.compute(_tupleDescLocal, TUPLE_FORMAT_ALL_FIXED);
        _tupleAccessorStatus.compute(_tupleDescStatus, TUPLE_FORMAT_ALL_FIXED);
        // Allocate memory for the tuple
        _pTupleBufLiteral.reset(
            new FixedBuffer[_tupleAccessorLiteral.getMaxByteCount()]);
        _pTupleBufInput.reset(
            new FixedBuffer[_tupleAccessorInput.getMaxByteCount()]);
        _pTupleBufOutput.reset(
            new FixedBuffer[_tupleAccessorOutput.getMaxByteCount()]);
        _pTupleBufLocal.reset(
            new FixedBuffer[_tupleAccessorLocal.getMaxByteCount()]);
        _pTupleBufStatus.reset(
            new FixedBuffer[_tupleAccessorStatus.getMaxByteCount()]);
        // Link memory to accessor
        _tupleAccessorLiteral.setCurrentTupleBuf(
            _pTupleBufLiteral.get(), false);
        _tupleAccessorInput.setCurrentTupleBuf(_pTupleBufInput.get(), false);
        _tupleAccessorOutput.setCurrentTupleBuf(_pTupleBufOutput.get(), false);
        _tupleAccessorLocal.setCurrentTupleBuf(_pTupleBufLocal.get(), false);
        _tupleAccessorStatus.setCurrentTupleBuf(_pTupleBufStatus.get(), false);
        // Create a vector of TupleDatum objects based on the
        // description we built
        _tupleDataLiteral.compute(_tupleDescLiteral);
        _tupleDataInput.compute(_tupleDescInput);
        _tupleDataOutput.compute(_tupleDescOutput);
        _tupleDataLocal.compute(_tupleDescLocal);
        _tupleDataStatus.compute(_tupleDescStatus);
        // Do something mysterious. Probably binding pointers in the
        // accessor to items in the TupleData vector.
        _tupleAccessorLiteral.unmarshal(_tupleDataLiteral);
        _tupleAccessorInput.unmarshal(_tupleDataInput);
        _tupleAccessorOutput.unmarshal(_tupleDataOutput);
        _tupleAccessorLocal.unmarshal(_tupleDataLocal);
    }

    template <typename T>
    void setInput(int index, T *valP)
    {
        // reinterpret_cast<T *>(const_cast<PBuffer>(
        //     _tupleDataInput[index].pData)) = valP;
        _tupleDataInput[index].pData = reinterpret_cast<const uint8_t *>(valP);
        if (true) {
            // Print out the nullable tuple
            TuplePrinter tuplePrinter;
            printf("Literals\n");
            tuplePrinter.print(cout, _tupleDescLiteral, _tupleDataLiteral);
            printf("\nInput\n");
            tuplePrinter.print(cout, _tupleDescInput, _tupleDataInput);
            cout << endl;
            printf("\nOutput\n");
            tuplePrinter.print(cout, _tupleDescOutput, _tupleDataOutput);
            cout << endl;
            printf("\nLocal\n");
            tuplePrinter.print(cout, _tupleDescLocal, _tupleDataLocal);
            cout << endl;
        }
    }
    template <typename T>
    void setInput(int index, T *valP, TupleStorageByteLength length)
    {
        // reinterpret_cast<T *>(const_cast<PBuffer>(
        //     _tupleDataInput[index].pData)) = valP;
        _tupleDataInput[index].pData = reinterpret_cast<const uint8_t *>(valP);
        _tupleDataInput[index].cbData = length;
        if (true) {
            // Print out the nullable tuple
            TuplePrinter tuplePrinter;
            printf("Literals\n");
            tuplePrinter.print(cout, _tupleDescLiteral, _tupleDataLiteral);
            printf("\nInput\n");
            tuplePrinter.print(cout, _tupleDescInput, _tupleDataInput);
            cout << endl;
            printf("\nOutput\n");
            tuplePrinter.print(cout, _tupleDescOutput, _tupleDataOutput);
            cout << endl;
            printf("\nLocal\n");
            tuplePrinter.print(cout, _tupleDescLocal, _tupleDataLocal);
            cout << endl;
        }
    }

    template <typename T>
    void setOutput(
        int index,
        T *valP,
        TupleStorageByteLength cbData,
        TupleStorageByteLength cbStorage)
    {
        // reinterpret_cast<T *>(const_cast<PBuffer>(
        //     _tupleDataOutput[index].pData)) = valP;
        _tupleDataOutput[index].pData = reinterpret_cast<const uint8_t *>(valP);
        _tupleDataOutput[index].cbData = cbData;
        _tupleDescOutput[index].cbStorage = cbStorage;
        if (true) {
            // Print out the nullable tuple
            TuplePrinter tuplePrinter;
            printf("Literals\n");
            tuplePrinter.print(cout, _tupleDescLiteral, _tupleDataLiteral);
            printf("\nInput\n");
            tuplePrinter.print(cout, _tupleDescInput, _tupleDataInput);
            cout << endl;
            printf("\nOutput\n");
            tuplePrinter.print(cout, _tupleDescOutput, _tupleDataOutput);
            cout << endl;
            printf("\nLocal\n");
            tuplePrinter.print(cout, _tupleDescLocal, _tupleDataLocal);
            cout << endl;
        }
    }
    void printOutput() {
        TuplePrinter tuplePrinter;
        printf("\nOutput\n");
        tuplePrinter.print(cout, _tupleDescOutput, _tupleDataOutput);
        cout << endl;
    }
    void bind()
    {
        Calculator::bind(
            RegisterReference::ELiteral,
            &_tupleDataLiteral,
            _tupleDescLiteral);
        Calculator::bind(
            RegisterReference::EInput,
            &_tupleDataInput,
            _tupleDescInput);
        Calculator::bind(
            RegisterReference::EOutput,
            &_tupleDataOutput,
            _tupleDescOutput);
        Calculator::bind(
            RegisterReference::ELocal,
            &_tupleDataLocal,
            _tupleDescLocal);
        Calculator::bind(
            RegisterReference::EStatus,
            &_tupleDataStatus,
            _tupleDescStatus);
    }

    template <typename T>
    void getOutput(
        int i,
        T &val)
    {
        val = *(reinterpret_cast<T *>(const_cast<PBuffer>(
            _tupleDataOutput[i].pData)));
    }

    template <typename T>
    void getOutputP(
        int i,
        T &val)
    {
        val = (reinterpret_cast<T>(const_cast<PBuffer>(
            _tupleDataOutput[i].pData)));
    }
};
// ----------------------------------------------------------------------
// tests
//---------------------------------------------

void printTestHeader(const char *msg)
{
    printf("=========================================================\n");
    printf("=========================================================\n");
    printf("=====\n");
    printf("=====     ");
    printf("%s", msg);
    printf("\n");
    printf("=====\n");
    printf("=========================================================\n");
    printf("=========================================================\n");
}

void testConvertDoubleToFloat(double val, float expected)
{
    printTestHeader("testConvertDoubleToFloat()");
    ExtendedInstructionTable table;
    vector<StandardTypeDescriptorOrdinal> parameterTypes;
    // define a function
    parameterTypes.resize(2);
    parameterTypes[0] = STANDARD_TYPE_REAL;
    parameterTypes[1] = STANDARD_TYPE_DOUBLE;
    table.add(
        "convert",
        parameterTypes,
        (ExtendedInstruction2<float, double>*) NULL,
        &convertDoubleToFloat);
    // lookup a function
    ExtendedInstructionDef *pDef = table["convert(r,d)"];
    assert(pDef != NULL);
    assert(pDef->getName() == string("convert"));
    assert(pDef->getParameterTypes().size() == 2);
    // lookup non-existent, should return NULL
    ExtendedInstructionDef *pNonExistentDef = table["convert(d,r)"];
    assert(pNonExistentDef == NULL);
    // Set up the Calculator
    DynamicParamManager dpm;
    TestCalculator c(&dpm, true, pDef);
    c.setInput(0, &val);
    // setup registers
    vector<RegisterReference *> regRefs(2);
    regRefs[0] = new RegisterRef<float>(
        RegisterReference::EOutput, 0,
        STANDARD_TYPE_REAL);
    regRefs[1] = new RegisterRef<double>(
        RegisterReference::EInput, 0,
        STANDARD_TYPE_DOUBLE);
    c.appendRegRef(regRefs[0]);
    c.appendRegRef(regRefs[1]);
    c.bind();
    // create an instruction
    //ExtendedInstruction *pInstr = pDef->createInstruction(&c, regRefs);
    ExtendedInstruction *pInstr = pDef->createInstruction(regRefs);
    assert(pInstr != NULL);
    // execute it
    c.appendInstruction(pInstr);
    c.exec();
    c.printOutput();
    float f;
    c.getOutput(0, f);
    cout << f << endl;
    assert(fabs(expected - f) < 0.0001);
};

void testConvertFloatToDouble(float val, double expected)
{
    printTestHeader("testConvertFloatToDouble()");
    ExtendedInstructionTable table;
    vector<StandardTypeDescriptorOrdinal> parameterTypes;
    // define a function
    parameterTypes.resize(2);
    parameterTypes[0] = STANDARD_TYPE_DOUBLE;
    parameterTypes[1] = STANDARD_TYPE_REAL;
    table.add(
        "convert",
        parameterTypes,
        (ExtendedInstruction2<double, float>*) NULL,
        &convertFloatToDouble);
    // lookup a function
    ExtendedInstructionDef *pDef = table["convert(d,r)"];
    assert(pDef != NULL);
    assert(pDef->getName() == string("convert"));
    assert(pDef->getParameterTypes().size() == 2);
    // Set up the Calculator
    DynamicParamManager dpm;
    TestCalculator c(&dpm, true, pDef);
    c.setInput(0, &val);
    // setup registers
    vector<RegisterReference *> regRefs(2);
    regRefs[0] = new RegisterRef<double>(
        RegisterReference::EOutput, 0,
        STANDARD_TYPE_DOUBLE);
    regRefs[1] = new RegisterRef<float>(
        RegisterReference::EInput, 0,
        STANDARD_TYPE_REAL);
    c.appendRegRef(regRefs[0]);
    c.appendRegRef(regRefs[1]);
    c.bind();
    // create an instruction
    //ExtendedInstruction *pInstr = pDef->createInstruction(&c, regRefs);
    ExtendedInstruction *pInstr = pDef->createInstruction(regRefs);
    assert(pInstr != NULL);
    // execute it
    c.appendInstruction(pInstr);
    c.exec();
    c.printOutput();
    double d;
    c.getOutput(0, d);
    cout << d << endl;
    assert(fabs(expected - d) < 0.0001);
};


void testConvertFloatToIntTypes(const char * const str, float val, int expected)
{
    printTestHeader("testConvertFloatToIntTypes()");
    ExtendedInstructionTable table;
    vector<StandardTypeDescriptorOrdinal> parameterTypes;
    // define a function
    parameterTypes.resize(2);
    parameterTypes[0] = StandardTypeDescriptor::fromString(str);
    parameterTypes[1] = STANDARD_TYPE_REAL;
    table.add(
        "convert",
        parameterTypes,
        (ExtendedInstruction2<int, float>*) NULL,
        &convertFloatToInt);
    // lookup a function
    string s("convert(");
    s += str;
    s += ",r)";
    cout << s << endl;
    ExtendedInstructionDef *pDef = table[s];
    assert(pDef != NULL);
    assert(pDef->getName() == string("convert"));
    assert(pDef->getParameterTypes().size() == 2);
    // lookup non-existent, should return NULL
    ExtendedInstructionDef *pNonExistentDef = table["convert(d,r)"];
    assert(pNonExistentDef == NULL);
    // Set up the Calculator
    DynamicParamManager dpm;
    TestCalculator c(&dpm, true, pDef);
    c.setInput(0, &val);
    // setup registers
    vector<RegisterReference *> regRefs(2);
    regRefs[0] = new RegisterRef<int>(
        RegisterReference::EOutput, 0,
        STANDARD_TYPE_INT_32);
    regRefs[1] = new RegisterRef<float>(
        RegisterReference::EInput, 0,
        STANDARD_TYPE_REAL);
    c.appendRegRef(regRefs[0]);
    c.appendRegRef(regRefs[1]);
    c.bind();
    // create an instruction
    //ExtendedInstruction *pInstr = pDef->createInstruction(&c, regRefs);
    ExtendedInstruction *pInstr = pDef->createInstruction(regRefs);
    assert(pInstr != NULL);
    // execute it
    c.appendInstruction(pInstr);
    c.exec();
    c.printOutput();
    int d;
    c.getOutput(0, d);
    cout << d << endl;
    assert(expected == d);
};

void testConvertIntTypesToFloat(const char * const str, int val, float expected)
{
    printTestHeader("testConvertIntTypesToFloat()");
    ExtendedInstructionTable table;
    vector<StandardTypeDescriptorOrdinal> parameterTypes;
    // define a function
    parameterTypes.resize(2);
    parameterTypes[0] = STANDARD_TYPE_REAL;
    parameterTypes[1] = StandardTypeDescriptor::fromString(str);
    table.add(
        "convert",
        parameterTypes,
        (ExtendedInstruction2<float, int>*) NULL,
        &convertIntToFloat);
    // lookup a function
    string s("convert(r,");
    s += str;
    s += ")";
    cout << s << endl;
    ExtendedInstructionDef *pDef = table[s];
    assert(pDef != NULL);
    assert(pDef->getName() == string("convert"));
    assert(pDef->getParameterTypes().size() == 2);
    // Set up the Calculator
    DynamicParamManager dpm;
    TestCalculator c(&dpm, true, pDef);
    c.setInput(0, &val);
    // setup registers
    vector<RegisterReference *> regRefs(2);
    regRefs[0] = new RegisterRef<float>(
        RegisterReference::EOutput, 0,
        STANDARD_TYPE_REAL);
    regRefs[1] = new RegisterRef<int>(
        RegisterReference::EInput, 0,
        STANDARD_TYPE_INT_32);
    c.appendRegRef(regRefs[0]);
    c.appendRegRef(regRefs[1]);
    c.bind();
    // create an instruction
    //ExtendedInstruction *pInstr = pDef->createInstruction(&c, regRefs);
    ExtendedInstruction *pInstr = pDef->createInstruction(regRefs);
    assert(pInstr != NULL);
    // execute it
    c.appendInstruction(pInstr);
    c.exec();
    c.printOutput();
    float f;
    c.getOutput(0, f);
    cout << f << endl;
    assert(expected == f);
};

void testConvertDecimal(const char * const str, int val, int exp, int expected)
{
    printTestHeader("testConvertDecimal()");
    ExtendedInstructionTable table;
    vector<StandardTypeDescriptorOrdinal> parameterTypes;
    // define a function
    parameterTypes.resize(3);
    parameterTypes[0] = StandardTypeDescriptor::fromString(str);
    parameterTypes[1] = StandardTypeDescriptor::fromString(str);
    parameterTypes[2] = STANDARD_TYPE_INT_8;
    table.add(
        "convert",
        parameterTypes,
        (ExtendedInstruction3<int32_t, int32_t, int32_t>*) NULL,
        &convertDecimal);
    // lookup a function
    string s("convert(");
    s += str;
    s += ",";
    s += str;
    s += ",";
    s += "s1";
    s += ")";
    cout << s << endl;
    ExtendedInstructionDef *pDef = table[s];
    assert(pDef != NULL);
    assert(pDef->getName() == string("convert"));
    assert(pDef->getParameterTypes().size() == 3);
    // Set up the Calculator
    DynamicParamManager dpm;
    TestCalculator c(&dpm, true, pDef);
    c.setInput(0, &val);
    c.setInput(1, &exp);
    // setup registers
    vector<RegisterReference *> regRefs(3);
    regRefs[0] = new RegisterRef<int>(
        RegisterReference::EOutput, 0,
        STANDARD_TYPE_INT_32);
    regRefs[1] = new RegisterRef<int>(
        RegisterReference::EInput, 0,
        STANDARD_TYPE_INT_32);
    regRefs[2] = new RegisterRef<int>(
        RegisterReference::EInput, 1,
        STANDARD_TYPE_INT_32);
    c.appendRegRef(regRefs[0]);
    c.appendRegRef(regRefs[1]);
    c.appendRegRef(regRefs[2]);
    c.bind();
    // create an instruction
    //ExtendedInstruction *pInstr = pDef->createInstruction(&c, regRefs);
    ExtendedInstruction *pInstr = pDef->createInstruction(regRefs);
    assert(pInstr != NULL);
    // execute it
    c.appendInstruction(pInstr);
    c.exec();
    c.printOutput();
    int i;
    c.getOutput(0, i);
    //cout << i << endl;
    assert(abs(expected - i)<0.00001);
};

void testConvertStringToExactNumber(const char *str, int expected)
{
    printTestHeader("testConvertStringToExactNumber()");
    ExtendedInstructionTable table;
    vector<StandardTypeDescriptorOrdinal> parameterTypes;
    // define a function
    parameterTypes.resize(2);
    parameterTypes[0] = STANDARD_TYPE_INT_32;
    parameterTypes[1] = STANDARD_TYPE_VARCHAR;

    table.add(
        "convert",
        parameterTypes,
        (ExtendedInstruction2<int32_t, char *>*) NULL,
        &convertStringToExactNumber);

    // lookup a function
    ExtendedInstructionDef *pDef = table["convert(s4,vc)"];
    assert(pDef != NULL);
    assert(pDef->getName() == string("convert"));
    assert(pDef->getParameterTypes().size() == 2);

    // Set up the Calculator
    DynamicParamManager dpm;
    TestCalculator c(&dpm, true, pDef);
    c.setInput(0, str, strlen(str));

    // setup registers
    vector<RegisterReference *> regRefs(2);
    regRefs[0] = new RegisterRef<int32_t>(
        RegisterReference::EOutput, 0,
        STANDARD_TYPE_INT_32);
    regRefs[1] = new RegisterRef<char *>(
        RegisterReference::EInput, 0,
        STANDARD_TYPE_VARCHAR);
    c.appendRegRef(regRefs[0]);
    c.appendRegRef(regRefs[1]);
    c.bind();

    // create an instruction
    //ExtendedInstruction *pInstr = pDef->createInstruction(&c, regRefs);
    ExtendedInstruction *pInstr = pDef->createInstruction(regRefs);
    assert(pInstr != NULL);
    // execute it
    c.appendInstruction(pInstr);
    c.exec();
    c.printOutput();
    int i;
    c.getOutput(0, i);
    assert(i == expected);
    cout << i << endl;
}

#if 0
// TODO .... JR 6/07 removing this
void testConvertExactNumberToString(int num, char *expected)
{
    printTestHeader("testConvertExactNumberToString()");
    ExtendedInstructionTable table;
    vector<StandardTypeDescriptorOrdinal> parameterTypes;
    // define a function
    parameterTypes.resize(2);
    parameterTypes[0] = STANDARD_TYPE_VARCHAR;
    parameterTypes[1] = STANDARD_TYPE_INT_32;

    table.add(
        "convert",
        parameterTypes,
        (ExtendedInstruction2<char *, int32_t>*) NULL,
        &convertExactNumberToString);

    // lookup a function
    ExtendedInstructionDef *pDef = table["convert(vc,s4)"];
    assert(pDef != NULL);
    assert(pDef->getName() == string("convert"));
    assert(pDef->getParameterTypes().size() == 2);

    // Set up the Calculator
    DynamicParamManager dpm;
    TestCalculator c(&dpm, true, pDef);
    c.setInput(0, &num);
    int destLen = strlen(expected);
    char *buf = new char[destLen*2];
    memset(buf, 'X', destLen*2); // no null terminator
    c.setOutput(0, buf, destLen*2, destLen*2);

    // setup registers
    vector<RegisterReference *> regRefs(2);
    regRefs[0] = new RegisterRef<int32_t>(
        RegisterReference::EOutput, 0,
        STANDARD_TYPE_VARCHAR);
    regRefs[1] = new RegisterRef<char *>(
        RegisterReference::EInput, 0,
        STANDARD_TYPE_INT_32);
    c.appendRegRef(regRefs[0]);
    c.appendRegRef(regRefs[1]);
    c.bind();

    // create an instruction
    //ExtendedInstruction *pInstr = pDef->createInstruction(&c, regRefs);
    ExtendedInstruction *pInstr = pDef->createInstruction(regRefs);
    assert(pInstr != NULL);
    // execute it
    c.appendInstruction(pInstr);
    c.exec();
    c.printOutput();
    char *outP;
    c.getOutputP(0, outP);
    assert(outP == buf);
    assert(!strncmp(outP, expected, destLen));
    outP[destLen] = 0;
    cout << outP << endl;
}
#endif
void testStringToApproximateNumber(char *str, float expected)
{
}
void testApproximateNumberToString(float expected, char *str)
{
}
void testStringToDate(char *str, long long expected)
{
}
void testDateToString(long long d, char *expected)
{
}

// ----------------------------------------------------------------------
// test cases
// ----------------------------------------------------------------------
int main(int argc, char *argv[])
{
    ProgramName = argv[0];
    testConvertDoubleToFloat((double) 100.33, (float) 100.33);
    testConvertFloatToDouble((float) 33.3378, (double) 33.3378);
    testConvertFloatToIntTypes("s1", (float) 45.65, 45);
    testConvertFloatToIntTypes("u1", (float) 45.65, 45);
    testConvertFloatToIntTypes("s2", (float) 45.65, 45);
    testConvertFloatToIntTypes("u2", (float) 45.65, 45);
    testConvertFloatToIntTypes("s4", (float) 45.65, 45);
    testConvertFloatToIntTypes("u4", (float) 45.65, 45);
    testConvertFloatToIntTypes("s8", (float) 45.65, 45);
    testConvertFloatToIntTypes("u8", (float) 45.65, 45);
    testConvertIntTypesToFloat("s1", 4565, (float) 4565);
    testConvertIntTypesToFloat("u1", 4565, (float) 4565);
    testConvertIntTypesToFloat("s2", 4565, (float) 4565);
    testConvertIntTypesToFloat("u2", 4565, (float) 4565);
    testConvertIntTypesToFloat("s4", 4565, (float) 4565);
    testConvertIntTypesToFloat("u4", 4565, (float) 4565);
    testConvertIntTypesToFloat("s8", 4565, (float) 4565);
    testConvertIntTypesToFloat("u8", 4565, (float) 4565);
    testConvertDecimal("s2", 123, 3, 123000);

    testConvertStringToExactNumber("123", 123);
//    testConvertExactNumberToString(123, "123"); -- JR 6/07 removing this
    printf("all tests passed\n");
    exit(0);
}

boost::unit_test_framework::test_suite *init_unit_test_suite(int,char **)
{
    return NULL;
}

// End testCalcExtended.cpp
