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
*/

#include "fennel/common/CommonPreamble.h"
#include "fennel/calc/CalcCommon.h"
#include "fennel/calc/CalcAssembler.h"
#include "fennel/tuple/TuplePrinter.h"

#include <fstream.h>
#include <strstream.h>
#include <iomanip.h>

using namespace fennel;

char *ProgramName;
bool verbose = true;

class CalcAssemblerTestCase 
{
public:
    explicit 
    CalcAssemblerTestCase(uint line, const char* desc, const char* code)
        : mDescription(desc), mProgram(code), mAssemblerError(NULL), 
          mInputTuple(NULL), mExpectedOutputTuple(NULL), mInputBuf(NULL), mExpOutputBuf(NULL),
          mFailed(false), mAssembled(false), mID(++testID), mLine(line)
    { }

    ~CalcAssemblerTestCase() 
    {
        if (mInputTuple) delete mInputTuple;
        if (mExpectedOutputTuple) delete mExpectedOutputTuple;
        if (mInputBuf) delete[] mInputBuf;
        if (mExpOutputBuf) delete[] mExpOutputBuf;
    }

    static uint getFailedNumber()
    {
        return nFailed;
    }

    static uint getPassedNumber()
    {
        return nPassed;
    }

    static uint getTestNumber()
    {
        return testID;
    }

    void fail(const char *str) 
    {
        assert(ProgramName);
        assert(str);
        assert(mDescription);
        printf("%s: unit test %d failed: | %s | %s | line %d\n", ProgramName, mID, mDescription, str, mLine);
        mFailed = true;
        nFailed++;
    }

    void passed(const char *exp, const char* actual)
    {
        assert(ProgramName);
        assert(exp);
        assert(actual);
        assert(mDescription);
        if (verbose) 
        {
            printf("%s: unit test %d passed: | %s | Got \"%s\""
                   "| Expected \"%s\" | line %d\n", 
                   ProgramName, mID, mDescription, actual, exp, mLine);
        }
        nPassed++;
    }

    bool assemble() 
    {
        assert(!mFailed && !mAssembled);
        assert(mProgram != NULL);
        try {
            mCalc.assemble(mProgram);

            TupleDescriptor inputTupleDesc = mCalc.getInputRegisterDescriptor();
            TupleDescriptor outputTupleDesc = mCalc.getOutputRegisterDescriptor();

            mInputTuple = CalcAssembler::createTupleData(inputTupleDesc, &mInputBuf);
            mExpectedOutputTuple = CalcAssembler::createTupleData(outputTupleDesc, &mExpOutputBuf);

            // Successfully assembled the test program
            mAssembled = true;

            if (mAssemblerError) {
                // Hmmm, we were expecting an error while assembling 
                // What happened? 
                string errorStr = "Program assembled, although expecting error: ";
                errorStr += mAssemblerError;
                fail(errorStr.c_str());
            }
        }
        catch (CalcAssemblerException& ex) 
        {
            if (mAssemblerError) {
                // We are expecting an error
                // Things okay

                // Let's check if the error message is kind of what we expected
                if (ex.getMessage().find(mAssemblerError) == string::npos)
                {
                    // Error message doesn't have the expected string
                    string errorStr = "Error message mismatch: ";
                    errorStr += "Got \"" + ex.getMessage() + "\"";
                    errorStr += " | ";
                    errorStr += "Expected \"";  
                    errorStr += mAssemblerError;
                    errorStr += "\"";
                    fail(errorStr.c_str());
                }
                else {
                    // Error message is okay
                    passed(mAssemblerError, ex.getMessage().c_str());
                }
            }
            else {
                string errorStr = "Error assembling program: ";
                errorStr += ex.getMessage();
                fail(errorStr.c_str());

                if (verbose) {
                    // Dump out program
                    cout << "Program Code: " << endl;
                    cout << ex.getCode() << endl;
                    cout << "Program Snippet: " << endl;
                    cout << ex.getCodeSnippet() << endl;
                }
            }
        }

        // Everything good?
        return (mAssembled && !mFailed);
    }

    static string tupleToString(TupleDescriptor const& tupleDesc, TupleData* tuple)
    {
        assert(tuple != NULL);
        ostringstream ostr("");
        TuplePrinter tuplePrinter;
        tuplePrinter.print(ostr, tupleDesc, *tuple);
        return ostr.str();
    }

    bool test()
    {
        assert(mAssembled && !mFailed);

        TupleDescriptor inputTupleDesc = mCalc.getInputRegisterDescriptor();
        TupleDescriptor outputTupleDesc = mCalc.getOutputRegisterDescriptor();
        
        FixedBuffer* outputBuf;
        TupleData* outputTuple = CalcAssembler::createTupleData(outputTupleDesc, &outputBuf);
        
        mCalc.bind(mInputTuple, outputTuple);
    
        mCalc.exec();
    
        if (!mCalc.mWarnings.empty()) fail("Calculator warnings");

        // For now, let's just use the string representation of the tuples for comparison
        string instr = tupleToString(inputTupleDesc, mInputTuple);
        string outstr = tupleToString(outputTupleDesc, outputTuple);
        string expoutstr = tupleToString(outputTupleDesc, mExpectedOutputTuple);

        if (expoutstr == outstr)
        {
            // Everything good and matches
            string resStr = instr + " -> " + outstr;
            passed(expoutstr.c_str(), resStr.c_str());
        }
        else {
            string errorStr = "Calculator result: " ;
            errorStr += instr + " -> " + outstr + "(Expected = " + expoutstr + ")";
            fail(errorStr.c_str());
        }
        delete outputTuple;
        delete[] outputBuf;
        return true;
    }

    void expectAssemblerError(const char* err)
    {
        mAssemblerError = err;
    }


    template <typename T>
    void setTupleDatum(TupleDatum& datum, T value)
    {
        *(reinterpret_cast<T*>(const_cast<PBuffer>(datum.pData))) = value;    
    }

    template <typename T>
    void setTupleDatum(TupleDatum& datum, TupleAttributeDescriptor& desc, T* buf, uint buflen)
    {
        assert(buf != NULL);
        StoredTypeDescriptor::Ordinal type = desc.pTypeDescriptor->getOrdinal();

        T* ptr = reinterpret_cast<T*>(const_cast<PBuffer>(datum.pData));
        switch (type) {
        case STANDARD_TYPE_CHAR:
        case STANDARD_TYPE_BINARY:
            // Fixed length storage
            assert(buflen <= datum.cbData);
            memset(ptr, 0, datum.cbData); // Fill with 0
            memcpy(ptr, buf, buflen);
            break;

        case STANDARD_TYPE_VARCHAR:
        case STANDARD_TYPE_VARBINARY:
            // Variable length storage
            assert(buflen <= desc.cbStorage);
            memset(ptr, 'I', desc.cbStorage); // Fill with junk
            memcpy(ptr, buf, buflen);
            datum.cbData = buflen;
            break;

        default: assert(0);
        }
    }

    template <typename T>
    void setInput(uint index, T value)
    {
        assert(mInputTuple != NULL);
        assert(index < mInputTuple->size());
        setTupleDatum((*mInputTuple)[index], value);
    }

    template <typename T>
    void setInput(uint index, T* buf, uint buflen)
    {
        assert(mInputTuple != NULL);
        assert(index < mInputTuple->size());
        TupleDescriptor inputTupleDesc = mCalc.getInputRegisterDescriptor();
        setTupleDatum((*mInputTuple)[index], inputTupleDesc[index], buf, buflen);
    }

    template <typename T>
    void setExpectedOutput(uint index, T value)
    {
        assert(mExpectedOutputTuple != NULL);
        assert(index < mExpectedOutputTuple->size());
        setTupleDatum((*mExpectedOutputTuple)[index], value);
    }

    template <typename T>
    void setExpectedOutput(uint index, T* buf, uint buflen)
    {
        assert(mExpectedOutputTuple != NULL);
        assert(index < mExpectedOutputTuple->size());
        TupleDescriptor outputTupleDesc = mCalc.getOutputRegisterDescriptor();
        setTupleDatum((*mExpectedOutputTuple)[index], outputTupleDesc[index], buf, buflen);
    }

    static string getHexString(const char* buf)
    {
        assert(buf != NULL);
        ostringstream ostr;
        uint buflen = strlen(buf);
        for (uint i=0; i<buflen; i++) {
            ostr << hex << setw(2) << setfill('0') << (int) buf[i];
        }
        return ostr.str();
    }

protected:
    const char* mDescription;
    const char* mProgram;
    const char* mAssemblerError;
    TupleData* mInputTuple;
    TupleData* mExpectedOutputTuple;
    FixedBuffer* mInputBuf;
    FixedBuffer* mExpOutputBuf;
    bool mFailed;
    bool mAssembled;
    uint mID;
    uint mLine;

    Calculator mCalc;
    static uint testID;
    static uint nFailed;
    static uint nPassed;
};

uint CalcAssemblerTestCase::testID = 0;
uint CalcAssemblerTestCase::nFailed = 0;
uint CalcAssemblerTestCase::nPassed = 0;

void testLiteralBinding()
{
    // Test invalid literals
    // Test overflow of u2
    CalcAssemblerTestCase testCase1(__LINE__, "OVERFLOW U2", 
                                    "O u2; C u2; V 777777; T; ADD O0, C0, C0;");
    testCase1.expectAssemblerError("bad numeric cast");
    testCase1.assemble();

    // Test binding a float to a u2
    CalcAssemblerTestCase testCase2(__LINE__, "BADVALUE U2", 
                                    "O u2; C u2; V 2451.342; T; ADD O0, C0, C0;");
    testCase2.expectAssemblerError("Invalid value");
    testCase2.assemble();

    // Test binding a negative number to a u4
    CalcAssemblerTestCase testCase3(__LINE__, "NEGVALUE U4", 
                                    "O u4; C u4; V -513; T; ADD O0, C0, C0;");
    testCase3.expectAssemblerError("Invalid value");
    testCase3.assemble();

    // Test binding a valid u2 that is out of range for a s2
    CalcAssemblerTestCase testCase4(__LINE__, "NEGVALUE U4", 
                                    "O s2; C s2; V 40000; T; ADD O0, C0, C0;");
    testCase4.expectAssemblerError("bad numeric cast");
    testCase4.assemble();

    // Test invalid literal index
    CalcAssemblerTestCase testCase5(__LINE__, "BAD INDEX", "I s1; C s1; V 1, 2;");
    testCase5.expectAssemblerError("out of bounds");
    testCase5.assemble();

    CalcAssemblerTestCase testCase6(__LINE__, "LREG/VAL MISMATCH", "I bo; C s1, s4; V 1;");
    testCase6.expectAssemblerError("Error binding literal");
    testCase6.assemble();

    // Test Literal registers not specified
    CalcAssemblerTestCase testCase7(__LINE__, "LREG/VAL MISMATCH", "I s1, s4; V 1, 4; T; RETURN;");
    testCase7.expectAssemblerError("");
    testCase7.assemble();

    // Test binding a 2 to a boolean
    CalcAssemblerTestCase testCase8(__LINE__, "BOOL = 2", "I bo; C bo; V 2;");
    testCase8.expectAssemblerError("Invalid value");
    testCase8.assemble();

    // Test bind a string (char)
    string teststr9;
    teststr9 = "O c,4, u8; C c,4, u8; V 0x";
    teststr9 += CalcAssemblerTestCase::getHexString("test");
    teststr9 += ", 60000000; T; MOVE O0, C0; MOVE O1, C1;";
    CalcAssemblerTestCase testCase9(__LINE__, "STRING (CHAR) = \"test\"", teststr9.c_str());
    if (testCase9.assemble()) {
        testCase9.setExpectedOutput<char>(0, "test", 4);
        testCase9.setExpectedOutput<uint64_t>(1, 60000000);
        testCase9.test();
    }
 
    // Test bind a string (varchar)
    string teststr10;
    teststr10 = "O vc,8; C vc,8; V 0x";
    teststr10 += CalcAssemblerTestCase::getHexString("short");
    teststr10 += "; T; MOVE O0, C0;";
    CalcAssemblerTestCase testCase10(__LINE__, "STRING (VARCHAR) = \"short\"", teststr10.c_str());
    if (testCase10.assemble()) {
        testCase10.setExpectedOutput<char>(0, "short", 5);
        testCase10.test();
    }

    // Test bind a string (varchar) that's too long
    string teststr11;
    teststr11 = "O vc,8, u8; C vc,8, u8; V 0x";
    teststr11 += CalcAssemblerTestCase::getHexString("muchtoolongstring");
    teststr11 += "; T; MOVE O0, C0;";
    
    CalcAssemblerTestCase testCase11(__LINE__, "STRING (VARCHAR) TOO LONG", teststr11.c_str());
    testCase11.expectAssemblerError("too long");
    testCase11.assemble();

    // Test bind a binary string (binary) that's too short
    string teststr12;
    teststr12 = "O c,100, u8; C c,100, u8; V 0x";
    teststr12 += CalcAssemblerTestCase::getHexString("binarytooshort");
    teststr12 += ", 60000000; T; MOVE O0, C0; MOVE O1, C1;";
    CalcAssemblerTestCase testCase12(__LINE__, "STRING (BINARY) TOO SHORT", teststr12.c_str());
    testCase12.expectAssemblerError("not equal");
    testCase12.assemble();
}

void testAdd()
{
    CalcAssemblerTestCase testCase1(__LINE__, "ADD U4", "I u4, u4;\nO u4;\nT;\nADD O0, I0, I1;");
    if (testCase1.assemble()) {
        testCase1.setInput<uint32_t>(0, 100);
        testCase1.setInput<uint32_t>(1, 4030);
        testCase1.setExpectedOutput<uint32_t>(0, 4130);
        testCase1.test();
    }

    CalcAssemblerTestCase testCase2(__LINE__, "ADD BAD TYPE", 
                                    "I u2, u4;\nO u4;\nT;\nADD O0, I0, I1;");
    testCase2.expectAssemblerError("Invalid type");
    testCase2.assemble();

    CalcAssemblerTestCase testCase3(__LINE__, "ADD O0 I0", 
                                    "I u2, u4;\nO u4;\nT;\nADD O0, I0;");
    testCase3.expectAssemblerError("Error instantiating instruction");
    testCase3.assemble();

    CalcAssemblerTestCase testCase4(__LINE__, "ADD FLOAT", "I r, r;\nO r, r;\n"
                                    "C r, r;\nV 0.3, -2.3;\n"
                                    "T;\nADD O0, I0, C0;\nADD O1, I1, C1;");
    if (testCase4.assemble()) {
        testCase4.setInput<float>(0, 200);
        testCase4.setInput<float>(1, 3000);
        testCase4.setExpectedOutput<float>(0, 200+0.3);
        testCase4.setExpectedOutput<float>(1, 3000-2.3);
        testCase4.test();
    }
}

void testBool()
{
    CalcAssemblerTestCase testCase1(__LINE__, "AND 1 1", "O bo;\nC bo, bo;\nV 1, 1; T;\n"
                                    "AND O0, C0, C1;");
    if (testCase1.assemble()) {
        testCase1.setExpectedOutput<bool>(0, true);
        testCase1.test();
    }

    CalcAssemblerTestCase testCase2(__LINE__, "EQ 1 0", "O bo;\nC bo, bo;\nV 1, 0; T;\n"
                                    "EQ O0, C0, C1;");
    if (testCase2.assemble()) {
        testCase2.setExpectedOutput<bool>(0, false);
        testCase2.test();
    }
}

void testPointer()
{
    CalcAssemblerTestCase testCase1(__LINE__, "CHAR EQ", "I c,10, c,10;\nO bo, bo;\nT;\n"
                                    "EQ O0, I0, I1; EQ O1, I0, I0;");
    if (testCase1.assemble()) {
        testCase1.setInput<char>(0, "test", 4);
        testCase1.setInput<char>(1, "junk", 4);
        testCase1.setExpectedOutput<bool>(0, false);
        testCase1.setExpectedOutput<bool>(1, true);
        testCase1.test();
    }

    CalcAssemblerTestCase testCase2(__LINE__, "VARCHAR EQ/ADD", "I vc,255, vc,255;\n"
                                    "O bo, bo, vc,255, u4;\nC u4; V 10;T;\n"
                                    "EQ O0, I0, I1; EQ O1, I0, I0; ADD O2, I0, C0; GETS O3, I0;");
    if (testCase2.assemble()) {
        testCase2.setInput<char>(0, "test varchar equal and add ....", 31);
        testCase2.setInput<char>(1, "junk", 4);
        testCase2.setExpectedOutput<bool>(0, false);
        testCase2.setExpectedOutput<bool>(1, true);
        testCase2.setExpectedOutput<char>(2, "ar equal and add ....", 21);
        testCase2.setExpectedOutput<uint32_t>(3, 31);
        testCase2.test();
    }
}

void testJump()
{
    // Test valid Jump True
    CalcAssemblerTestCase testCase1(__LINE__, "JUMP TRUE", 
                                    "I u2, u2;\nO u2, u2;\nL bo;\n"
                                    "C u2, u2;\nV 0, 1;\nT;\n"
                                    "MOVE O0, C0;\nMOVE O1, C0;\n"
                                    "ADD O0, O0, I0;\nADD O1, O1, C1;\n"
                                    "LT L0, O1, I1;\nJMPT @2, L0;\n");
    if (testCase1.assemble()) {
        testCase1.setInput<uint16_t>(0, 3);
        testCase1.setInput<uint16_t>(1, 4);
        testCase1.setExpectedOutput<uint16_t>(0, 12);
        testCase1.setExpectedOutput<uint16_t>(1, 4);
        testCase1.test();
    }

    // Test Jumping to invalid PC
    CalcAssemblerTestCase testCase2(__LINE__, "INVALID PC", "I u2; O u2; T; JMP @10;");
    testCase2.expectAssemblerError("Invalid PC");
    testCase2.assemble();

    // Test Jump False a a valid PC that is later on in the program
    CalcAssemblerTestCase testCase3(__LINE__, "VALID PC", "I u2; O u2;\n"
                                    "C bo; V 0; T;\n"
                                    "MOVE O0, I0;\n"
                                    "JMPF @4, C0;\n"
                                    "ADD  O0, O0, I0;\n"
                                    "ADD  O0, O0, I0;\n"
                                    "ADD  O0, O0, I0;\n");
    if (testCase3.assemble()) {
        testCase3.setInput<uint16_t>(0, 15);
        testCase3.setExpectedOutput<uint16_t>(0, 30);
        testCase3.test();
    }    
}

void testReturn()
{
    // Test the return instruction
    CalcAssemblerTestCase testCase1(__LINE__, "RETURN", 
                                    "I u2;\nO u2;\n"
                                    "T;\n"
                                    "MOVE O0, I0;\n"
                                    "RETURN;\n"
                                    "ADD O0, I0, I0;\n");
    if (testCase1.assemble()) {
        testCase1.setInput<uint16_t>(0, 100);
        testCase1.setExpectedOutput<uint16_t>(0, 100);
        testCase1.test();
    }
}

void convertFloatToInt(Calculator *pCalc,
                       RegisterRef<int>* regOut,
                       RegisterRef<float>* regIn)
{
    regOut->putV((int)regIn->getV());
}

void testExtended()
{
    // Test valid function
    ExtendedInstructionTable* table = InstructionFactory::getExtendedInstructionTable();
    assert(table != NULL);
    
    vector<StandardTypeDescriptorOrdinal> parameterTypes;

    // define a function
    parameterTypes.resize(2);
    parameterTypes[0] = STANDARD_TYPE_UINT_32;
    parameterTypes[1] = STANDARD_TYPE_REAL;
    table->add("convert", 
               parameterTypes, 
               (ExtendedInstruction2<int32_t, float>*) NULL,
               &convertFloatToInt);

    // define test case
    CalcAssemblerTestCase testCase1(__LINE__, "CONVERT FLOAT TO INT",
                                    "I r; O u4;\n"
                                    "T;\n"
                                    "CALL 'convert(O0, I0);\n");
    if (testCase1.assemble()) {
        testCase1.setInput<float>(0, 53.34);
        testCase1.setExpectedOutput<uint32_t>(0, 53);
        testCase1.test();
    }    

    CalcAssemblerTestCase testCase2(__LINE__, "CONVERT INT TO FLOAT (NOT REGISTERED)",
                                    "I u4; O r;\n"
                                    "T;\n"
                                    "CALL 'convert(O0, I0);\n");
    testCase2.expectAssemblerError("not registered");
    testCase2.assemble();
}

void testInvalidPrograms()
{
    CalcAssemblerTestCase testCase1(__LINE__, "JUNK", "Junk");
    testCase1.expectAssemblerError("parse error");
    testCase1.assemble();    

    // Test unregistered instruction
    CalcAssemblerTestCase testCase2(__LINE__, "UNKNOWN INST", "I u2, u4;\nO u4;\nT;\nBAD O0, I0;");
    testCase2.expectAssemblerError("not a registered native instruction");
    testCase2.assemble();

    // Test known instruction - but not registered for that type
    CalcAssemblerTestCase testCase3(__LINE__, "AND float", "I d, d;\nO d;\nT;\nAND O0, I0, I1;");
    testCase3.expectAssemblerError("not a registered instruction");
    testCase3.assemble();

    CalcAssemblerTestCase testCase4(__LINE__, "BAD SIGNATURE", 
                                    "I u2, u4;\nO u4;\nT;\nBAD O0, I0, I0, I1;");
    testCase4.expectAssemblerError("parse error");
    testCase4.assemble();

    CalcAssemblerTestCase testCase5(__LINE__, "BAD INST", "I u2, u4;\nO u4;\nT;\nklkdfw;");
    testCase5.expectAssemblerError("parse error");
    testCase5.assemble();

    // Test invalid register index
    CalcAssemblerTestCase testCase6(__LINE__, "BAD REG INDEX", "I u2, u4;\nO u4;\nT;\nADD O0, I0, I12;");
    testCase6.expectAssemblerError("out of bounds");
    testCase6.assemble();

    // Test invalid register index
    CalcAssemblerTestCase testCase7(__LINE__, "BAD REG INDEX", "I u2, u4;\nO u4;\nT;\n"
                                    "ADD O0, I0, I888888888888888888888888888888888888888888;");
    testCase7.expectAssemblerError("out of range");
    testCase7.assemble();

    // TODO: Test extremely long programs and stuff
}

void testAssembler()
{
    testLiteralBinding();
    testInvalidPrograms();
    testAdd();
    testBool();
    testPointer();
    testReturn();
    testJump();
    testExtended();
}

int main (int argc, char **argv)
{
    ProgramName = argv[0];
    InstructionFactory inst();
    InstructionFactory::registerInstructions();
  
    try {
        testAssembler();
    }
    catch (exception& ex)
    {
        cerr << ex.what() << endl;
    } 

    cout << CalcAssemblerTestCase::getPassedNumber() << "/" 
         << CalcAssemblerTestCase::getTestNumber() << " tests passed" << endl;
    return CalcAssemblerTestCase::getFailedNumber(); 
}

// End TestCalcAssembler
