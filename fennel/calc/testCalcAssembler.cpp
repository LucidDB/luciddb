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
#include "fennel/calc/StringToHex.h"
#include "fennel/tuple/TuplePrinter.h"

#include <fstream.h>
#include <strstream.h>
#include <iomanip.h>

using namespace fennel;

char *ProgramName;
bool verbose = false;
bool showProgram = true;


template <typename T>
class RegisterTestInfo
{
public:
    enum ERegisterCheck {
        EINVALID = -1,
        ENULL = 0,
        EVALID = 1
    };

    explicit
    RegisterTestInfo(string desc, TProgramCounter pc):
        mDesc(desc), mCheckValue(EINVALID), mPC(pc)
    {
    }

    explicit
    RegisterTestInfo(string desc, T v, TProgramCounter pc):
        mDesc(desc), mValue(v), mCheckValue(EVALID), mPC(pc)
    {
    }

    explicit
    RegisterTestInfo(string desc, T* pV, TProgramCounter pc):
        mDesc(desc), mPC(pc)
    {
        if (pV == NULL) {
            mCheckValue = ENULL;
        }
        else {
            mValue = *pV;
            mCheckValue = EVALID;
        }
    }

    void setTupleDatum(TupleDatum& datum)
    {
        switch (mCheckValue)
        {
        case EINVALID:
            // Nothing to set
            break;
        case ENULL:
            // Set to NULL
            datum.pData = NULL;
            break;
        case EVALID:
            assert(datum.pData != NULL);
            (*reinterpret_cast<T*>(const_cast<PBuffer>(datum.pData))) = mValue;
             break;
        default:
            assert(false);
        }
    }

    bool checkTupleDatum(TupleDatum& datum)
    {
        switch (mCheckValue)
        {
        case EINVALID:
            // Nothing to check against, okay
            return true;
        case ENULL:
            // Check for null
            return (datum.pData == NULL);
        case EVALID:
            if (datum.pData == NULL) return false;
            return (*reinterpret_cast<const T*>(datum.pData) == mValue);
        default:
            assert(false);
        }
    }

    string toString()
    {
        ostringstream ostr("");
        switch (mCheckValue)
        {
        case EINVALID:
            break;
        case ENULL:
            ostr << "(NULL) for test ";
            break;
        case EVALID:
            ostr << "(" << mValue << ") for test ";
             break;
        default:
            assert(false);
        }

        ostr << "'" << mDesc << "'" << " at PC=" << mPC;
        return ostr.str();
    }

    ~RegisterTestInfo() {}

    // Description of this test
    string          mDesc;

    // Expected value of this register
    T mValue;
    
    // Should we check the value?  Is it suppose to be NULL?
    ERegisterCheck  mCheckValue;

    // PC associated with setting this register
    TProgramCounter mPC;
};

class CalcChecker
{
public:
    virtual bool checkResult(Calculator& calc, TupleData& output) = 0;
};

template <typename T>
class CalcTestInfo: public CalcChecker
{
public:
    explicit
    CalcTestInfo(StandardTypeDescriptorOrdinal type): typeOrdinal(type) {}
    virtual ~CalcTestInfo() {}
    
    void add(string desc, T* pV, TProgramCounter pc, uint line = 0)
    {
        if (line)
        {
            ostringstream ostr("");
            ostr << " Line: " << line;
            desc += ostr.str();
        }
        mOutRegInfo.push_back( RegisterTestInfo<T>(desc, pV, pc) );
    }

    void add(string desc, T v, TProgramCounter pc, uint line = 0)
    {
        if (line)
        {
            ostringstream ostr("");
            ostr << " Line: " << line;
            desc += ostr.str();
        }
        mOutRegInfo.push_back( RegisterTestInfo<T>(desc, v, pc) );
    }

    void addRegister(string desc, TProgramCounter pc)
    {
        mOutRegInfo.push_back( RegisterTestInfo<T>(desc, pc) );
    }

    void addWarning(const char* msg, TProgramCounter pc)
    {
        mWarnings.push_back( CalcMessage(msg, pc) );
    }

    void add(string desc, const char* error, TProgramCounter pc, uint line = 0)
    {
        if (line)
        {
            ostringstream ostr("");
            ostr << " Line: " << line;
            desc += ostr.str();
        }
        addRegister(desc, pc);
        if (error != NULL)
            addWarning(error, pc);
    }

    void setTupleData(TupleData& tuple)
    {
        assert(tuple.size() == mOutRegInfo.size());

        // Check type of output registers and value
        for (uint i=0; i < tuple.size(); i++)
        {
            mOutRegInfo[i].setTupleDatum(tuple[i]);
        }
    }

    virtual bool checkResult(Calculator& calc, TupleData& outputTuple)
    {
        TupleDescriptor outputTupleDesc = calc.getOutputRegisterDescriptor();
        // Verify the output descriptor
        assert(outputTupleDesc.size() == outputTuple.size());

        // Check number of output registers
        if (outputTupleDesc.size() != mOutRegInfo.size())
        {
            cout << "Mismatch of register number" << endl;
            return false;
        }

        // Check type of output registers and value
        for (uint i=0; i < outputTupleDesc.size(); i++)
        {
            if (outputTupleDesc[i].pTypeDescriptor->getOrdinal() != 
                static_cast<StoredTypeDescriptor::Ordinal>(typeOrdinal))
            {
                cout << "Type ordinal mismatch" << endl;
                return false;
            }

            if (!mOutRegInfo[i].checkTupleDatum(outputTuple[i]))
            {
                cout << "Tuple datum mismatch: Register " << i 
                     << " should be " << mOutRegInfo[i].toString()
                     << ". " << endl;
                return false;
            }
        }

        // Check warnings
        if (calc.mWarnings.size() != mWarnings.size())
        {
            cout << "# of warnings should be " << mWarnings.size() 
                 << " not " << calc.mWarnings.size() << endl;
            return false;
        }

        for (uint i=0; i < mWarnings.size(); i++)
        {
            if (calc.mWarnings[i].mPc != mWarnings[i].mPc)
            {
                cout << "Warning expected at PC=" << mWarnings[i].mPc
                     << ". Got warning at PC="
                     << calc.mWarnings[i].mPc << endl;
                return false;
            }
            if (calc.mWarnings[i].mStr != mWarnings[i].mStr)
            {
                cout << "Message should be " << mWarnings[i].mStr 
                     << " not " << calc.mWarnings[i].mStr << " at PC="
                     << mWarnings[i].mPc << endl;
                return false;
            }
        }
        
        return true;
    }

public:
    // Vector of expected output register values
    vector< RegisterTestInfo<T> > mOutRegInfo;

    // Vector of expected warnings
    deque<CalcMessage> mWarnings;

    StandardTypeDescriptorOrdinal typeOrdinal;
};

class CalcAssemblerTestCase 
{
public:
    static const uint MAX_WIDTH = 512;

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

    void fail(const char *exp, const char* actual) 
    {
        assert(ProgramName);
        assert(exp);
        assert(actual);
        assert(mDescription);
        printf("%s: unit test %d failed: | %s | Got \"%s\""
               "| Expected \"%s\" | line %d\n", 
               ProgramName, mID, mDescription, actual, exp, mLine);
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
                string errorStr = "Error assembling program: ";
                errorStr += mAssemblerError;
                fail(errorStr.c_str(), "Program assembled.");
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
                    fail(mAssemblerError, ex.getMessage().c_str());
                }
                else {
                    // Error message is okay
                    passed(mAssemblerError, ex.getMessage().c_str());
                }
            }
            else {
                string errorStr = "Error assembling program: ";
                errorStr += ex.getMessage();
                fail("Success assembling program", errorStr.c_str());

                if (showProgram) {
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

    bool test(CalcChecker* pChecker = NULL)
    {
        assert(mAssembled);
        bool res = true;

        TupleDescriptor inputTupleDesc = mCalc.getInputRegisterDescriptor();
        TupleDescriptor outputTupleDesc = mCalc.getOutputRegisterDescriptor();
        
        FixedBuffer* outputBuf;
        TupleData* outputTuple = CalcAssembler::createTupleData(outputTupleDesc, &outputBuf);
        
        mCalc.bind(mInputTuple, outputTuple);
    
        mCalc.exec();
    
        string instr = tupleToString(inputTupleDesc, mInputTuple);
        string outstr = tupleToString(outputTupleDesc, outputTuple);
        string expoutstr = tupleToString(outputTupleDesc, mExpectedOutputTuple);
        
        if (pChecker == NULL)
        {
            // For now, let's just use the string representation of the tuples for comparison
            res = (expoutstr == outstr);

            // Make sure there are no warnings
            if (!mCalc.mWarnings.empty()) {
                res = false;
                fail(expoutstr.c_str(), "Calculator warnings");
            }
        }
        else {
            res = pChecker->checkResult(mCalc, *outputTuple);
        }

        if (res)
        {
            // Everything good and matches
            string resStr = instr + " -> " + outstr;
            passed(expoutstr.c_str(), resStr.c_str());
        }
        else {
            string errorStr = "Calculator result: " ;
            errorStr += instr + " -> " + outstr;
            fail(expoutstr.c_str(), errorStr.c_str());
        }

        delete outputTuple;
        delete[] outputBuf;
        return res;
    }

    void expectAssemblerError(const char* err)
    {
        mAssemblerError = err;
    }

    void   writeMaxData(TupleDatum &datum, uint typeOrdinal);
    void   writeMinData(TupleDatum &datum, uint typeOrdinal);
    string toLiteralString(TupleDatum &datum, uint typeOrdinal);

    void setTupleDatumMax(TupleDatum& datum, TupleAttributeDescriptor& desc)
    {
        StoredTypeDescriptor::Ordinal type = desc.pTypeDescriptor->getOrdinal();
        writeMaxData(datum, type);
    }

    void setTupleDatumMin(TupleDatum& datum, TupleAttributeDescriptor& desc)
    {
        StoredTypeDescriptor::Ordinal type = desc.pTypeDescriptor->getOrdinal();
        writeMinData(datum, type);
    }

    void setTupleDatumNull(TupleDatum& datum)
    {
        datum.pData = NULL;
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

    void setInputMin(uint index)
    {
        assert(mInputTuple != NULL);
        assert(index < mInputTuple->size());
        TupleDescriptor inputTupleDesc = mCalc.getInputRegisterDescriptor();
        setTupleDatumMin((*mInputTuple)[index], inputTupleDesc[index]);
    }

    void setInputMax(uint index)
    {
        assert(mInputTuple != NULL);
        assert(index < mInputTuple->size());
        TupleDescriptor inputTupleDesc = mCalc.getInputRegisterDescriptor();
        setTupleDatumMax((*mInputTuple)[index], inputTupleDesc[index]);
    }

    void setInputNull(uint index)
    {
        assert(mInputTuple != NULL);
        assert(index < mInputTuple->size());
        setTupleDatumNull((*mInputTuple)[index]);
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

    string getInput(uint index)
    {
        assert(mInputTuple != NULL);
        assert(index < mInputTuple->size());
        TupleDescriptor inputTupleDesc = mCalc.getInputRegisterDescriptor();
        return toLiteralString((*mInputTuple)[index], 
                               inputTupleDesc[index].pTypeDescriptor->getOrdinal());
    }

    TupleData* getInputTuple()
    {
        return mInputTuple;
    }

    /* Functions for setting the expected output */
    void setExpectedOutputNull(uint index)
    {
        assert(mExpectedOutputTuple != NULL);
        assert(index < mExpectedOutputTuple->size());
        setTupleDatumNull((*mExpectedOutputTuple)[index]);
    }

    void setExpectedOutputMax(uint index)
    {
        assert(mExpectedOutputTuple != NULL);
        assert(index < mExpectedOutputTuple->size());
        TupleDescriptor outputTupleDesc = mCalc.getOutputRegisterDescriptor();
        setTupleDatumMax((*mExpectedOutputTuple)[index], outputTupleDesc[index]);
    }
    void setExpectedOutputMin(uint index)
    {
        assert(mExpectedOutputTuple != NULL);
        assert(index < mExpectedOutputTuple->size());
        TupleDescriptor outputTupleDesc = mCalc.getOutputRegisterDescriptor();
        setTupleDatumMin((*mExpectedOutputTuple)[index], outputTupleDesc[index]);
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

    string getExpectedOutput(uint index)
    {
        assert(mExpectedOutputTuple != NULL);
        assert(index < mExpectedOutputTuple->size());
        TupleDescriptor outputTupleDesc = mCalc.getOutputRegisterDescriptor();
        return toLiteralString((*mExpectedOutputTuple)[index], 
                               outputTupleDesc[index].pTypeDescriptor->getOrdinal());
    }

    TupleData* getExpectedOutputTuple()
    {
        return mExpectedOutputTuple;
    }

    bool failed()
    {
        return mFailed;
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

class CalcAssemblerTest 
{
protected:
    void testAdd();

    void testBool();
    void testPointer();
    void testReturn();
    void testJump();
    void testExtended();

    void testLiteralBinding();
    void testInvalidPrograms();

    void testStandardTypes();

    void testBoolInstructions(StandardTypeDescriptorOrdinal type);

    template <typename T>
    void testNativeInstructions(StandardTypeDescriptorOrdinal type);

    string getTypeString(StandardTypeDescriptorOrdinal type, uint arraylen = 0);
    string createRegisterString(string s, uint n, char c = ',');
    void addBinaryInstructions(ostringstream& ostr,
                               string opcode,
                               uint& outreg,
                               uint n);

    void addUnaryInstructions(ostringstream& ostr,
                              string opcode,
                              uint& outreg,
                              uint n);

public:
    explicit
    CalcAssemblerTest()
    {
    }
 
    virtual ~CalcAssemblerTest()
    {
    }
 
    void testAssembler();
};

// Converts a tuple data to its literal string representation in the
// assembler.
// NOTE: This may be different from its normal string representation
//       using the TuplePrinter.
string CalcAssemblerTestCase::toLiteralString(TupleDatum &datum, 
                                              uint typeOrdinal)
{
    ostringstream ostr("");
    if (datum.pData != NULL)
    {
        switch(typeOrdinal) {
        case STANDARD_TYPE_BOOL:
            if (*reinterpret_cast<const bool*>(datum.pData))
                ostr << "1";
            else ostr << "0";
            break;
        case STANDARD_TYPE_INT_8:
            ostr << (int) (*reinterpret_cast<const int8_t*>(datum.pData));
            break;
        case STANDARD_TYPE_UINT_8:
            ostr << (int) (*reinterpret_cast<const uint8_t*>(datum.pData));
            break;
        case STANDARD_TYPE_INT_16:
            ostr << (*reinterpret_cast<const int16_t*>(datum.pData));
            break;
        case STANDARD_TYPE_UINT_16:
            ostr << (*reinterpret_cast<const uint16_t*>(datum.pData));
        break;
        case STANDARD_TYPE_INT_32:
            ostr << (*reinterpret_cast<const int32_t*>(datum.pData));
            break;
        case STANDARD_TYPE_UINT_32:
            ostr << (*reinterpret_cast<const uint32_t*>(datum.pData));
        break;
        case STANDARD_TYPE_INT_64:
            ostr << (*reinterpret_cast<const int64_t*>(datum.pData));
            break;
        case STANDARD_TYPE_UINT_64:
            ostr << (*reinterpret_cast<const uint64_t*>(datum.pData));
            break;
        case STANDARD_TYPE_REAL:
            ostr << (*reinterpret_cast<const float*>(datum.pData));
        break;
        case STANDARD_TYPE_DOUBLE:
            ostr << (*reinterpret_cast<const double*>(datum.pData));
            break;
        case STANDARD_TYPE_BINARY:
        case STANDARD_TYPE_CHAR:
        case STANDARD_TYPE_VARCHAR:
        case STANDARD_TYPE_VARBINARY:
            ostr << "0x";
            ostr << stringToHex( (reinterpret_cast<const char*>(datum.pData)),
                                  datum.cbData);
            break;
        default:
            assert(false);
        }
    }
    return ostr.str();
}

// Copied from TupleTest::writeMaxData
void CalcAssemblerTestCase::writeMaxData(TupleDatum &datum, uint typeOrdinal)
{
    PBuffer pData = const_cast<PBuffer>(datum.pData);
    switch(typeOrdinal) {
    case STANDARD_TYPE_BOOL:
        *(reinterpret_cast<bool *>(pData)) = true;
        break;
    case STANDARD_TYPE_INT_8:
        *(reinterpret_cast<int8_t *>(pData)) =
            std::numeric_limits<int8_t>::max();
        break;
    case STANDARD_TYPE_UINT_8:
        *(reinterpret_cast<uint8_t *>(pData)) =
            std::numeric_limits<uint8_t>::max();
        break;
    case STANDARD_TYPE_INT_16:
        *(reinterpret_cast<int16_t *>(pData)) =
            std::numeric_limits<int16_t>::max();
        break;
    case STANDARD_TYPE_UINT_16:
        *(reinterpret_cast<uint16_t *>(pData)) =
            std::numeric_limits<uint16_t>::max();
        break;
    case STANDARD_TYPE_INT_32:
        *(reinterpret_cast<int32_t *>(pData)) =
            std::numeric_limits<int32_t>::max();
        break;
    case STANDARD_TYPE_UINT_32:
        *(reinterpret_cast<uint32_t *>(pData)) =
            std::numeric_limits<uint32_t>::max();
        break;
    case STANDARD_TYPE_INT_64:
        *(reinterpret_cast<int64_t *>(pData)) =
            std::numeric_limits<int64_t>::max();
        break;
    case STANDARD_TYPE_UINT_64:
        *(reinterpret_cast<uint64_t *>(pData)) =
            std::numeric_limits<uint64_t>::max();
        break;
    case STANDARD_TYPE_REAL:
        *(reinterpret_cast<float *>(pData)) =
            std::numeric_limits<float>::max();
        break;
    case STANDARD_TYPE_DOUBLE:
        *(reinterpret_cast<double *>(pData)) =
            std::numeric_limits<double>::max();
        break;
    case STANDARD_TYPE_BINARY:
        memset(pData,0xFF,datum.cbData);
        break;
    case STANDARD_TYPE_CHAR:
        memset(pData,'z',datum.cbData);
        break;
    case STANDARD_TYPE_VARCHAR:
        datum.cbData = MAX_WIDTH;
        memset(pData,'z',datum.cbData);
        break;
    case STANDARD_TYPE_VARBINARY:
        datum.cbData = MAX_WIDTH;
        memset(pData,0xFF,datum.cbData);
        break;
    default:
        assert(false);
    }
}

// Copied from TupleTest::writeMinData
void CalcAssemblerTestCase::writeMinData(TupleDatum &datum,uint typeOrdinal)
{
    PBuffer pData = const_cast<PBuffer>(datum.pData);
    switch(typeOrdinal) {
    case STANDARD_TYPE_BOOL:
        *(reinterpret_cast<bool *>(pData)) = false;
        break;
    case STANDARD_TYPE_INT_8:
        *(reinterpret_cast<int8_t *>(pData)) =
            std::numeric_limits<int8_t>::min();
        break;
    case STANDARD_TYPE_UINT_8:
        *(reinterpret_cast<uint8_t *>(pData)) =
            std::numeric_limits<uint8_t>::min();
        break;
    case STANDARD_TYPE_INT_16:
        *(reinterpret_cast<int16_t *>(pData)) =
            std::numeric_limits<int16_t>::min();
        break;
    case STANDARD_TYPE_UINT_16:
        *(reinterpret_cast<uint16_t *>(pData)) =
            std::numeric_limits<uint16_t>::min();
        break;
    case STANDARD_TYPE_INT_32:
        *(reinterpret_cast<int32_t *>(pData)) =
            std::numeric_limits<int32_t>::min();
        break;
    case STANDARD_TYPE_UINT_32:
        *(reinterpret_cast<uint32_t *>(pData)) =
            std::numeric_limits<uint32_t>::min();
        break;
    case STANDARD_TYPE_INT_64:
        *(reinterpret_cast<int64_t *>(pData)) =
            std::numeric_limits<int64_t>::min();
        break;
    case STANDARD_TYPE_UINT_64:
        *(reinterpret_cast<uint64_t *>(pData)) =
            std::numeric_limits<uint64_t>::min();
        break;
    case STANDARD_TYPE_REAL:
        *(reinterpret_cast<float *>(pData)) =
            std::numeric_limits<float>::min();
        break;
    case STANDARD_TYPE_DOUBLE:
        *(reinterpret_cast<double *>(pData)) =
            std::numeric_limits<double>::min();
        break;
    case STANDARD_TYPE_BINARY:
        memset(pData,0,datum.cbData);
        break;
    case STANDARD_TYPE_CHAR:
        memset(pData,'A',datum.cbData);
        break;
    case STANDARD_TYPE_VARCHAR:
    case STANDARD_TYPE_VARBINARY:
        datum.cbData = 0;
        break;
    default:
        assert(false);
    }
}

string CalcAssemblerTest::getTypeString(StandardTypeDescriptorOrdinal type, uint arraylen)
{
    string typestr = StandardTypeDescriptor::toString(type);
    if (StandardTypeDescriptor::isArray(type)) 
    {
        ostringstream size("");
        size << "," << arraylen;
        typestr += size.str();
    }
    return typestr;
}

string CalcAssemblerTest::createRegisterString(string s, uint n, char c)
{
    ostringstream ostr("");
    for (uint i=0; i<n; i++)
    {
        if (i > 0)
            ostr << c;
        ostr << s;
    }
    return ostr.str();
}

void CalcAssemblerTest::addUnaryInstructions(ostringstream& ostr,
                                             string opcode,
                                             uint& outreg,
                                             uint n)
{
    for (uint i=0; i<n; i++)
        ostr << opcode << " O" << outreg++ << ", I"
             << i << ";" << endl;
}
                                        
void CalcAssemblerTest::addBinaryInstructions(ostringstream& ostr,
                                              string opcode,
                                              uint& outreg,
                                              uint n)
{
    for (uint i=0; i<n; i++)
        for (uint j=0; j<n; j++)
            ostr << opcode << " O" << outreg++ << ", I"
                 << i << ", I" << j << ";" << endl;
}

/** 
 * Test instructions that returns a native type
 * @param type Type of the operands
 * Instructions tested are:
 * - For all native types: ADD, SUB, MUL, NEG
 * - For integral types:   MOD, AND, OR, SHFL, SHFR
 * Input registers:
 * I0 = min
 * I1 = max
 * I2 = NULL
 * I3 = 10
 */
template <typename T>
void CalcAssemblerTest::testNativeInstructions(StandardTypeDescriptorOrdinal type)
{
    string typestr = getTypeString(type, CalcAssemblerTestCase::MAX_WIDTH);
    uint inregs = 4;

    // Form instruction string
    ostringstream instostr("");
    uint outreg = 0;
    CalcTestInfo<T> expectedCalcOut(type);
    TProgramCounter pc = 0;
    T* pNULL = NULL;
    T  zero  = 0;
    T  min = std::numeric_limits<T>::min();
    T  max = std::numeric_limits<T>::max();
    T  mid = 10;

    // TODO: Overflows/Underflows (different for unsigned/signed)

    // Test ADD
    addBinaryInstructions(instostr, "ADD", outreg, inregs);
    string addstr = string("ADD ") + typestr;
    expectedCalcOut.add(addstr, (T) (min+min), pc++, __LINE__);    // I0 + I0 (min + min)
    expectedCalcOut.add(addstr, (T) (min+max), pc++, __LINE__);    // I0 + I1 (min + max)
    expectedCalcOut.add(addstr, pNULL,   pc++, __LINE__);          // I0 + I2 (min + NULL)
    expectedCalcOut.add(addstr, (T) (min+mid),  pc++, __LINE__);   // I0 + I3 (min + 10)

    expectedCalcOut.add(addstr, (T) (max+min), pc++, __LINE__);    // I1 + I0 (max + min)
    expectedCalcOut.add(addstr, (T) (max+max), pc++, __LINE__);    // I1 + I1 (max + max)
    expectedCalcOut.add(addstr, pNULL,   pc++, __LINE__);          // I1 + I2 (max + NULL)
    expectedCalcOut.add(addstr, (T) (max+mid), pc++, __LINE__);    // I1 + I3 (max + 10)

    expectedCalcOut.add(addstr, pNULL, pc++, __LINE__);      // I2 + I0 (NULL + min)
    expectedCalcOut.add(addstr, pNULL, pc++, __LINE__);      // I2 + I1 (NULL + max)
    expectedCalcOut.add(addstr, pNULL, pc++, __LINE__);      // I2 + I2 (NULL + NULL)
    expectedCalcOut.add(addstr, pNULL, pc++, __LINE__);      // I2 + I3 (NULL + 10)

    expectedCalcOut.add(addstr, (T) (mid+min), pc++, __LINE__);    // I3 + I0 (10 + min)
    expectedCalcOut.add(addstr, (T) (mid+max), pc++, __LINE__);    // I3 + I1 (10 + max)
    expectedCalcOut.add(addstr, pNULL, pc++, __LINE__);            // I3 + I2 (10 + NULL)
    expectedCalcOut.add(addstr, (T) (mid+mid), pc++, __LINE__);    // I3 + I3 (10 + 10)
    
    // Test SUB
    addBinaryInstructions(instostr, "SUB", outreg, inregs);
    string substr = string("SUB ") + typestr;
    expectedCalcOut.add(substr, (T) (min-min), pc++, __LINE__);    // I0 - I0 (min - min)
    expectedCalcOut.add(substr, (T) (min-max), pc++, __LINE__);    // I0 - I1 (min - max)
    expectedCalcOut.add(substr, pNULL,   pc++, __LINE__);          // I0 - I2 (min - NULL)
    expectedCalcOut.add(substr, (T) (min-mid),  pc++, __LINE__);   // I0 - I3 (min - 10)

    expectedCalcOut.add(substr, (T) (max-min), pc++, __LINE__);    // I1 - I0 (max - min)
    expectedCalcOut.add(substr, (T) (max-max), pc++, __LINE__);    // I1 - I1 (max - max)
    expectedCalcOut.add(substr, pNULL,   pc++, __LINE__);          // I1 - I2 (max - NULL)
    expectedCalcOut.add(substr, (T) (max-mid), pc++, __LINE__);    // I1 - I3 (max - 10)

    expectedCalcOut.add(substr, pNULL, pc++, __LINE__);      // I2 - I0 (NULL - min)
    expectedCalcOut.add(substr, pNULL, pc++, __LINE__);      // I2 - I1 (NULL - max)
    expectedCalcOut.add(substr, pNULL, pc++, __LINE__);      // I2 - I2 (NULL - NULL)
    expectedCalcOut.add(substr, pNULL, pc++, __LINE__);      // I2 - I3 (NULL - 10)

    expectedCalcOut.add(substr, (T) (mid-min), pc++, __LINE__);    // I3 - I0 (10 - min)
    expectedCalcOut.add(substr, (T) (mid-max), pc++, __LINE__);    // I3 - I1 (10 - max)
    expectedCalcOut.add(substr, pNULL, pc++, __LINE__);            // I3 - I2 (10 - NULL)
    expectedCalcOut.add(substr, (T) (mid-mid), pc++, __LINE__);    // I3 - I3 (10 - 10)

    // Test MUL
    addBinaryInstructions(instostr, "MUL", outreg, inregs);
    string mulstr = string("MUL ") + typestr;
    expectedCalcOut.add(mulstr, (T) (min*min), pc++, __LINE__);    // I0 * I0 (min * min)
    expectedCalcOut.add(mulstr, (T) (min*max), pc++, __LINE__);    // I0 * I1 (min * max)
    expectedCalcOut.add(mulstr, pNULL,   pc++, __LINE__);          // I0 * I2 (min * NULL)
    expectedCalcOut.add(mulstr, (T) (min*mid),  pc++, __LINE__);   // I0 * I3 (min * 10)

    expectedCalcOut.add(mulstr, (T) (max*min), pc++, __LINE__);    // I1 * I0 (max * min)
    expectedCalcOut.add(mulstr, (T) (max*max), pc++, __LINE__);    // I1 * I1 (max * max)
    expectedCalcOut.add(mulstr, pNULL,   pc++, __LINE__);          // I1 * I2 (max * NULL)
    expectedCalcOut.add(mulstr, (T) (max*mid), pc++, __LINE__);    // I1 * I3 (max * 10)

    expectedCalcOut.add(mulstr, pNULL, pc++, __LINE__);      // I2 * I0 (NULL * min)
    expectedCalcOut.add(mulstr, pNULL, pc++, __LINE__);      // I2 * I1 (NULL * max)
    expectedCalcOut.add(mulstr, pNULL, pc++, __LINE__);      // I2 * I2 (NULL * NULL)
    expectedCalcOut.add(mulstr, pNULL, pc++, __LINE__);      // I2 * I3 (NULL * 10)

    expectedCalcOut.add(mulstr, (T) (mid*min), pc++, __LINE__);    // I3 * I0 (10 * min)
    expectedCalcOut.add(mulstr, (T) (mid*max), pc++, __LINE__);    // I3 * I1 (10 * max)
    expectedCalcOut.add(mulstr, pNULL, pc++, __LINE__);            // I3 * I2 (10 * NULL)
    expectedCalcOut.add(mulstr, (T) (mid*mid), pc++, __LINE__);    // I3 * I3 (10 * 10)

    // Test DIV
    const char* divbyzero = "22012";
    addBinaryInstructions(instostr, "DIV", outreg, inregs);
    string divstr = string("DIV ") + typestr;
    if (min != zero)
        expectedCalcOut.add(divstr, (T) (min/min), pc++, __LINE__); // I0 / I0 (min / min)
    else expectedCalcOut.add(divstr, divbyzero, pc++, __LINE__);    // I0 / I0 (min / min)
    expectedCalcOut.add(divstr, (T) (min/max), pc++, __LINE__);     // I0 / I1 (min / max)
    expectedCalcOut.add(divstr, pNULL,   pc++, __LINE__);           // I0 / I2 (min / NULL)
    expectedCalcOut.add(divstr, (T) (min/mid),  pc++, __LINE__);    // I0 / I3 (min / 10)

    if (min != zero)
        expectedCalcOut.add(divstr, (T) (max/min), pc++, __LINE__); // I1 / I0 (max / min)
    else expectedCalcOut.add(divstr, divbyzero, pc++, __LINE__);    // I1 / I0 (max / min)
    expectedCalcOut.add(divstr, (T) (max/max), pc++, __LINE__);     // I1 / I1 (max / max)
    expectedCalcOut.add(divstr, pNULL,   pc++, __LINE__);           // I1 / I2 (max / NULL)
    expectedCalcOut.add(divstr, (T) (max/mid), pc++, __LINE__);    // I1 / I3 (max / 10)

    expectedCalcOut.add(divstr, pNULL, pc++, __LINE__);      // I2 / I0 (NULL / min)
    expectedCalcOut.add(divstr, pNULL, pc++, __LINE__);      // I2 / I1 (NULL / max)
    expectedCalcOut.add(divstr, pNULL, pc++, __LINE__);      // I2 / I2 (NULL / NULL)
    expectedCalcOut.add(divstr, pNULL, pc++, __LINE__);      // I2 / I3 (NULL / 10)

    if (min != zero)
        expectedCalcOut.add(divstr, (T) (mid/min), pc++); // I3 / I0 (mid / min)
    else expectedCalcOut.add(divstr, divbyzero, pc++);    // I3 / I0 (mid / min)
    expectedCalcOut.add(divstr, (T) (mid/max), pc++);     // I3 / I1 (10 / max)
    expectedCalcOut.add(divstr, pNULL, pc++);             // I3 / I2 (10 / NULL)
    expectedCalcOut.add(divstr, (T) (mid/mid), pc++);     // I3 / I3 (10 / 10)

    // Test NEG
    addUnaryInstructions(instostr, "NEG", outreg, inregs);
    string negstr = string("NEG ") + typestr;
    expectedCalcOut.add(negstr, (T) (-min), pc++, __LINE__);    // - I0 (- min)
    expectedCalcOut.add(negstr, (T) (-max), pc++, __LINE__);    // - I1 (- max)
    expectedCalcOut.add(negstr, pNULL,      pc++, __LINE__);    // - I2 (- NULL)
    expectedCalcOut.add(negstr, (T) (-mid), pc++, __LINE__);    // - I3 (- 10)

    if (StandardTypeDescriptor::isIntegralNative(type))
    {
        // Test MOD
        string modstr = string("MOD ") + typestr;
        addBinaryInstructions(instostr, "MOD", outreg, inregs);
        if (min != zero)
            expectedCalcOut.add(modstr, (T) (min%min), pc++, __LINE__); // I0 % I0 (min % min)
        else expectedCalcOut.add(modstr, divbyzero, pc++, __LINE__);    // I0 % I0 (min % min)
        expectedCalcOut.add(modstr, (T) (min%max), pc++, __LINE__);    // I0 % I1 (min % max)
        expectedCalcOut.add(modstr, pNULL,   pc++, __LINE__);          // I0 % I2 (min % NULL)
        expectedCalcOut.add(modstr, (T) (min%mid),  pc++, __LINE__);   // I0 % I3 (min % 10)
        
        if (min != zero)
            expectedCalcOut.add(modstr, (T) (max%min), pc++, __LINE__); // I1 % I0 (max % min)
        else expectedCalcOut.add(modstr, divbyzero, pc++, __LINE__);    // I1 % I0 (max % min)
        expectedCalcOut.add(modstr, (T) (max%max), pc++, __LINE__);    // I1 % I1 (max % max)
        expectedCalcOut.add(modstr, pNULL,   pc++, __LINE__);          // I1 % I2 (max % NULL)
        expectedCalcOut.add(modstr, (T) (max%mid), pc++, __LINE__);    // I1 % I3 (max % 10)
        
        expectedCalcOut.add(modstr, pNULL, pc++, __LINE__);      // I2 % I0 (NULL % min)
        expectedCalcOut.add(modstr, pNULL, pc++, __LINE__);      // I2 % I1 (NULL % max)
        expectedCalcOut.add(modstr, pNULL, pc++, __LINE__);      // I2 % I2 (NULL % NULL)
        expectedCalcOut.add(modstr, pNULL, pc++, __LINE__);      // I2 % I3 (NULL % 10)
        
        if (min != zero)
            expectedCalcOut.add(modstr, (T) (mid%min), pc++, __LINE__); // I3 % I0 (mid % min)
        else expectedCalcOut.add(modstr, divbyzero, pc++, __LINE__);    // I3 % I0 (mid % min)
        expectedCalcOut.add(modstr, (T) (mid%max), pc++, __LINE__);    // I3 % I1 (10 % max)
        expectedCalcOut.add(modstr, pNULL, pc++, __LINE__);            // I3 % I2 (10 % NULL)
        expectedCalcOut.add(modstr, (T) (mid%mid), pc++, __LINE__);    // I3 % I3 (10 % 10)

        // Test AND
        string andstr = string("AND ") + typestr;
        addBinaryInstructions(instostr, "AND", outreg, inregs);
        expectedCalcOut.add(andstr, (T) (min&min), pc++, __LINE__);    // I0 & I0 (min & min)
        expectedCalcOut.add(andstr, (T) (min&max), pc++, __LINE__);    // I0 & I1 (min & max)
        expectedCalcOut.add(andstr, pNULL,   pc++, __LINE__);          // I0 & I2 (min & NULL)
        expectedCalcOut.add(andstr, (T) (min&mid),  pc++, __LINE__);   // I0 & I3 (min & 10)
        
        expectedCalcOut.add(andstr, (T) (max&min), pc++, __LINE__);    // I1 & I0 (max & min)
        expectedCalcOut.add(andstr, (T) (max&max), pc++, __LINE__);    // I1 & I1 (max & max)
        expectedCalcOut.add(andstr, pNULL,   pc++, __LINE__);          // I1 & I2 (max & NULL)
        expectedCalcOut.add(andstr, (T) (max&mid), pc++, __LINE__);    // I1 & I3 (max & 10)
        
        expectedCalcOut.add(andstr, pNULL, pc++, __LINE__);      // I2 & I0 (NULL & min)
        expectedCalcOut.add(andstr, pNULL, pc++, __LINE__);      // I2 & I1 (NULL & max)
        expectedCalcOut.add(andstr, pNULL, pc++, __LINE__);      // I2 & I2 (NULL & NULL)
        expectedCalcOut.add(andstr, pNULL, pc++, __LINE__);      // I2 & I3 (NULL & 10)
        
        expectedCalcOut.add(andstr, (T) (mid&min), pc++, __LINE__);    // I3 & I0 (10 & min)
        expectedCalcOut.add(andstr, (T) (mid&max), pc++, __LINE__);    // I3 & I1 (10 & max)
        expectedCalcOut.add(andstr, pNULL, pc++, __LINE__);            // I3 & I2 (10 & NULL)
        expectedCalcOut.add(andstr, (T) (mid&mid), pc++, __LINE__);    // I3 & I3 (10 & 10)
    
        // Test OR
        string orstr = string("OR ") + typestr;
        addBinaryInstructions(instostr, "OR", outreg, inregs);
        expectedCalcOut.add(orstr, (T) (min|min), pc++, __LINE__);    // I0 | I0 (min | min)
        expectedCalcOut.add(orstr, (T) (min|max), pc++, __LINE__);    // I0 | I1 (min | max)
        expectedCalcOut.add(orstr, pNULL,   pc++, __LINE__);          // I0 | I2 (min | NULL)
        expectedCalcOut.add(orstr, (T) (min|mid),  pc++, __LINE__);   // I0 | I3 (min | 10)
        
        expectedCalcOut.add(orstr, (T) (max|min), pc++, __LINE__);    // I1 | I0 (max | min)
        expectedCalcOut.add(orstr, (T) (max|max), pc++, __LINE__);    // I1 | I1 (max | max)
        expectedCalcOut.add(orstr, pNULL,   pc++, __LINE__);          // I1 | I2 (max | NULL)
        expectedCalcOut.add(orstr, (T) (max|mid), pc++, __LINE__);    // I1 | I3 (max | 10)
        
        expectedCalcOut.add(orstr, pNULL, pc++, __LINE__);      // I2 | I0 (NULL | min)
        expectedCalcOut.add(orstr, pNULL, pc++, __LINE__);      // I2 | I1 (NULL | max)
        expectedCalcOut.add(orstr, pNULL, pc++, __LINE__);      // I2 | I2 (NULL | NULL)
        expectedCalcOut.add(orstr, pNULL, pc++, __LINE__);      // I2 | I3 (NULL | 10)
        
        expectedCalcOut.add(orstr, (T) (mid|min), pc++, __LINE__);    // I3 | I0 (10 | min)
        expectedCalcOut.add(orstr, (T) (mid|max), pc++, __LINE__);    // I3 | I1 (10 | max)
        expectedCalcOut.add(orstr, pNULL, pc++, __LINE__);            // I3 | I2 (10 | NULL)
        expectedCalcOut.add(orstr, (T) (mid|mid), pc++, __LINE__);    // I3 | I3 (10 | 10)

        // Test SHFL
        string shflstr = string("SHFL ") + typestr;
        addBinaryInstructions(instostr, "SHFL", outreg, inregs);
        expectedCalcOut.add(shflstr, (T) (min<<min), pc++, __LINE__);    // I0 << I0 (min << min)
        expectedCalcOut.add(shflstr, (T) (min<<max), pc++, __LINE__);    // I0 << I1 (min << max)
        expectedCalcOut.add(shflstr, pNULL,   pc++, __LINE__);           // I0 << I2 (min << NULL)
        expectedCalcOut.add(shflstr, (T) (min<<mid),  pc++, __LINE__);   // I0 << I3 (min << 10)
        
        expectedCalcOut.add(shflstr, (T) (max<<min), pc++, __LINE__);    // I1 << I0 (max << min)
        expectedCalcOut.add(shflstr, (T) (max<<max), pc++, __LINE__);    // I1 << I1 (max << max)
        expectedCalcOut.add(shflstr, pNULL,   pc++, __LINE__);           // I1 << I2 (max << NULL)
        expectedCalcOut.add(shflstr, (T) (max<<mid), pc++, __LINE__);    // I1 << I3 (max << 10)
        
        expectedCalcOut.add(shflstr, pNULL, pc++, __LINE__);      // I2 << I0 (NULL << min)
        expectedCalcOut.add(shflstr, pNULL, pc++, __LINE__);      // I2 << I1 (NULL << max)
        expectedCalcOut.add(shflstr, pNULL, pc++, __LINE__);      // I2 << I2 (NULL << NULL)
        expectedCalcOut.add(shflstr, pNULL, pc++, __LINE__);      // I2 << I3 (NULL << 10)
        
        expectedCalcOut.add(shflstr, (T) (mid<<min), pc++, __LINE__);    // I3 << I0 (10 << min)
        expectedCalcOut.add(shflstr, (T) (mid<<max), pc++, __LINE__);    // I3 << I1 (10 << max)
        expectedCalcOut.add(shflstr, pNULL, pc++, __LINE__);             // I3 << I2 (10 << NULL)
        expectedCalcOut.add(shflstr, (T) (mid<<mid), pc++, __LINE__);    // I3 << I3 (10 << 10)

        // Test SHFR
        string shfrstr = string("SHFR ") + typestr;
        addBinaryInstructions(instostr, "SHFR", outreg, inregs);
        expectedCalcOut.add(shfrstr, (T) (min>>min), pc++, __LINE__);    // I0 >> I0 (min >> min)
        expectedCalcOut.add(shfrstr, (T) (min>>max), pc++, __LINE__);    // I0 >> I1 (min >> max)
        expectedCalcOut.add(shfrstr, pNULL,   pc++, __LINE__);           // I0 >> I2 (min >> NULL)
        expectedCalcOut.add(shfrstr, (T) (min>>mid),  pc++, __LINE__);   // I0 >> I3 (min >> 10)
        
        expectedCalcOut.add(shfrstr, (T) (max>>min), pc++, __LINE__);    // I1 >> I0 (max >> min)
        expectedCalcOut.add(shfrstr, (T) (max>>max), pc++, __LINE__);    // I1 >> I1 (max >> max)
        expectedCalcOut.add(shfrstr, pNULL,   pc++, __LINE__);           // I1 >> I2 (max >> NULL)
        expectedCalcOut.add(shfrstr, (T) (max>>mid), pc++, __LINE__);    // I1 >> I3 (max >> 10)
        
        expectedCalcOut.add(shfrstr, pNULL, pc++, __LINE__);      // I2 >> I0 (NULL >> min)
        expectedCalcOut.add(shfrstr, pNULL, pc++, __LINE__);      // I2 >> I1 (NULL >> max)
        expectedCalcOut.add(shfrstr, pNULL, pc++, __LINE__);      // I2 >> I2 (NULL >> NULL)
        expectedCalcOut.add(shfrstr, pNULL, pc++, __LINE__);      // I2 >> I3 (NULL >> 10)
        
        expectedCalcOut.add(shfrstr, (T) (mid>>min), pc++, __LINE__);    // I3 >> I0 (10 >> min)
        expectedCalcOut.add(shfrstr, (T) (mid>>max), pc++, __LINE__);    // I3 >> I1 (10 >> max)
        expectedCalcOut.add(shfrstr, pNULL, pc++, __LINE__);             // I3 >> I2 (10 >> NULL)
        expectedCalcOut.add(shfrstr, (T) (mid>>mid), pc++, __LINE__);    // I3 >> I3 (10 >> 10)
    }

    assert(outreg == static_cast<uint>(pc));

    // Form test string
    string testdesc = "testNativeInstructions: " + typestr;
    ostringstream testostr("");

    testostr << "I " << createRegisterString(typestr, inregs) << ";" << endl;
    testostr << "O " << createRegisterString(typestr, outreg) << ";" << endl;
    testostr << "T;" << endl;
    
    testostr << instostr.str();

    string teststr = testostr.str();

    CalcAssemblerTestCase testCase1(__LINE__, testdesc.c_str(),
                                    teststr.c_str());
    if (testCase1.assemble())
    {
        testCase1.setInputMin(0);
        testCase1.setInputMax(1);
        testCase1.setInputNull(2);
        testCase1.template setInput<T>(3, mid);
        TupleData* outputTuple = testCase1.getExpectedOutputTuple();
        assert(outputTuple != NULL);
        expectedCalcOut.setTupleData(*outputTuple);
        testCase1.test(&expectedCalcOut);
    }
}

/** 
 * Test instructions that returns a boolean
 * @param type Type of the operands
 * Instructions tested are:
 * - For all types:    EQ, NE, ISNULL, ISNOTNULL, GT, LT
 * - For booleans:     IS, ISNOT, NOT, AND, OR
 * - For non-booleans: GE, LE
 * Input registers:
 * I0 = min
 * I1 = max
 * I2 = NULL
 */
void CalcAssemblerTest::testBoolInstructions(StandardTypeDescriptorOrdinal type)
{
    string typestr = getTypeString(type, CalcAssemblerTestCase::MAX_WIDTH);
    string boolstr = getTypeString(STANDARD_TYPE_BOOL);
    uint inregs = 3;

    // Form instruction string
    ostringstream instostr("");
    uint outreg = 0;
    vector<bool*> boolout;
    bool bFalse  = false;
    bool bTrue   = true;
    bool* pFalse = &bFalse;
    bool* pTrue  = &bTrue;
    
    // Make copy of input registers to local registers
    instostr << "MOVE L0, I0;" << endl;
    instostr << "MOVE L1, I1;" << endl;
    instostr << "MOVE L2, I2;" << endl;

    // Test is null
    addUnaryInstructions(instostr, "ISNULL", outreg, inregs);
    boolout.push_back(pFalse); // I0 = min
    boolout.push_back(pFalse); // I1 = max
    boolout.push_back(pTrue);  // I2 = NULL

    // Test is not null
    addUnaryInstructions(instostr, "ISNOTNULL", outreg, inregs);
    boolout.push_back(pTrue);  // I0 = min
    boolout.push_back(pTrue);  // I1 = max
    boolout.push_back(pFalse); // I2 = NULL

    // Test equal
    // Check equal if the registers are different (use local)
    instostr << "EQ O" << outreg++ <<", L0, I0;" << endl; // true
    boolout.push_back(pTrue);
    instostr << "EQ O" << outreg++ <<", L1, I1;" << endl; // true
    boolout.push_back(pTrue);

    // Check equal using input registers
    addBinaryInstructions(instostr, "EQ", outreg, inregs);
    boolout.push_back(pTrue);  // I0, I0
    boolout.push_back(pFalse); // I0, I1
    boolout.push_back(NULL);   // I0, I2
    boolout.push_back(pFalse); // I1, I0
    boolout.push_back(pTrue);  // I1, I1
    boolout.push_back(NULL);   // I1, I2
    boolout.push_back(NULL);   // I2, I0
    boolout.push_back(NULL);   // I2, I1
    boolout.push_back(NULL);   // I2, I2

    // Test not equal
    // Check not equal if the registers are different (use local)
    instostr << "NE O" << outreg++ <<", L0, I0;" << endl; // false
    boolout.push_back(pFalse);
    instostr << "NE O" << outreg++ <<", L1, I1;" << endl; // false
    boolout.push_back(pFalse);

    // Check not equal using input registers
    addBinaryInstructions(instostr, "NE", outreg, inregs);
    boolout.push_back(pFalse); // I0, I0
    boolout.push_back(pTrue);  // I0, I1
    boolout.push_back(NULL);   // I0, I2
    boolout.push_back(pTrue);  // I1, I0
    boolout.push_back(pFalse); // I1, I1
    boolout.push_back(NULL);   // I1, I2
    boolout.push_back(NULL);   // I2, I0
    boolout.push_back(NULL);   // I2, I1
    boolout.push_back(NULL);   // I2, I2

    // Test greater than
    addBinaryInstructions(instostr, "GT", outreg, inregs);
    boolout.push_back(pFalse); // I0, I0
    boolout.push_back(pFalse); // I0, I1
    boolout.push_back(NULL);   // I0, I2
    boolout.push_back(pTrue);  // I1, I0
    boolout.push_back(pFalse); // I1, I1
    boolout.push_back(NULL);   // I1, I2
    boolout.push_back(NULL);   // I2, I0
    boolout.push_back(NULL);   // I2, I1
    boolout.push_back(NULL);   // I2, I2

    // Test less than
    addBinaryInstructions(instostr, "LT", outreg, inregs);
    boolout.push_back(pFalse); // I0, I0
    boolout.push_back(pTrue);  // I0, I1
    boolout.push_back(NULL);   // I0, I2
    boolout.push_back(pFalse); // I1, I0
    boolout.push_back(pFalse); // I1, I1
    boolout.push_back(NULL);   // I1, I2
    boolout.push_back(NULL);   // I2, I0
    boolout.push_back(NULL);   // I2, I1
    boolout.push_back(NULL);   // I2, I2

    if (type == STANDARD_TYPE_BOOL)
    {
        // Test NOT
        addUnaryInstructions(instostr, "NOT", outreg, inregs);
        boolout.push_back(pTrue);  // I0 = min = false
        boolout.push_back(pFalse); // I1 = max = true
        boolout.push_back(NULL);   // I2 = NULL

        // Test IS
        addBinaryInstructions(instostr, "IS", outreg, inregs);
        boolout.push_back(pTrue);  // I0, I0
        boolout.push_back(pFalse); // I0, I1
        boolout.push_back(pFalse); // I0, I2
        boolout.push_back(pFalse); // I1, I0
        boolout.push_back(pTrue);  // I1, I1
        boolout.push_back(pFalse); // I1, I2
        boolout.push_back(pFalse); // I2, I0
        boolout.push_back(pFalse); // I2, I1
        boolout.push_back(pTrue);  // I2, I2

        // Test IS NOT
        addBinaryInstructions(instostr, "ISNOT", outreg, inregs);
        boolout.push_back(pFalse); // I0, I0
        boolout.push_back(pTrue);  // I0, I1
        boolout.push_back(pTrue);  // I0, I2
        boolout.push_back(pTrue);  // I1, I0
        boolout.push_back(pFalse); // I1, I1
        boolout.push_back(pTrue);  // I1, I2
        boolout.push_back(pTrue);  // I2, I0
        boolout.push_back(pTrue);  // I2, I1
        boolout.push_back(pFalse); // I2, I2

        // Test AND
        addBinaryInstructions(instostr, "AND", outreg, inregs);
        boolout.push_back(pFalse); // I0, I0
        boolout.push_back(pFalse); // I0, I1
        boolout.push_back(pFalse); // I0, I2
        boolout.push_back(pFalse); // I1, I0
        boolout.push_back(pTrue);  // I1, I1
        boolout.push_back(NULL);   // I1, I2
        boolout.push_back(pFalse); // I2, I0
        boolout.push_back(NULL);   // I2, I1
        boolout.push_back(NULL);   // I2, I2

        // Test OR
        addBinaryInstructions(instostr, "OR", outreg, inregs);
        boolout.push_back(pFalse); // I0, I0
        boolout.push_back(pTrue);  // I0, I1
        boolout.push_back(NULL);   // I0, I2
        boolout.push_back(pTrue);  // I1, I0
        boolout.push_back(pTrue);  // I1, I1
        boolout.push_back(pTrue);  // I1, I2
        boolout.push_back(NULL);   // I2, I0
        boolout.push_back(pTrue);  // I2, I1
        boolout.push_back(NULL);   // I2, I2
    }
    else 
    {
        // Test GE
        addBinaryInstructions(instostr, "GE", outreg, inregs);
        boolout.push_back(pTrue);  // I0, I0
        boolout.push_back(pFalse); // I0, I1
        boolout.push_back(NULL);   // I0, I2
        boolout.push_back(pTrue);  // I1, I0
        boolout.push_back(pTrue);  // I1, I1
        boolout.push_back(NULL);   // I1, I2
        boolout.push_back(NULL);   // I2, I0
        boolout.push_back(NULL);   // I2, I1
        boolout.push_back(NULL);   // I2, I2
        
        // Test LE
        addBinaryInstructions(instostr, "LE", outreg, inregs);
        boolout.push_back(pTrue);  // I0, I0
        boolout.push_back(pTrue);  // I0, I1
        boolout.push_back(NULL);   // I0, I2
        boolout.push_back(pFalse); // I1, I0
        boolout.push_back(pTrue);  // I1, I1
        boolout.push_back(NULL);   // I1, I2
        boolout.push_back(NULL);   // I2, I0
        boolout.push_back(NULL);   // I2, I1
        boolout.push_back(NULL);   // I2, I2
    }

    assert(outreg == boolout.size());

    // Form test string
    string testdesc = "testBoolInstructions: " + typestr;
    ostringstream testostr("");

    testostr << "I " << createRegisterString(typestr, inregs) << ";" << endl;
    testostr << "L " << createRegisterString(typestr, inregs) << ";" << endl;
    testostr << "O " << createRegisterString(boolstr, outreg) << ";" << endl;
    testostr << "T;" << endl;
    
    testostr << instostr.str();

    string teststr = testostr.str();
    
    CalcAssemblerTestCase testCase1(__LINE__, testdesc.c_str(),
                                    teststr.c_str());
    if (testCase1.assemble())
    {
        testCase1.setInputMin(0);
        testCase1.setInputMax(1);
        testCase1.setInputNull(2);
        for (uint i=0; i<outreg; i++) {
            if (boolout[i])
                testCase1.setExpectedOutput<bool>(i, *boolout[i]);
            else testCase1.setExpectedOutputNull(i);
        }
        testCase1.test();
    }
}

void CalcAssemblerTest::testStandardTypes()
{
    string max[STANDARD_TYPE_END];
    string min[STANDARD_TYPE_END];
    string overflow[STANDARD_TYPE_END];
    string underflow[STANDARD_TYPE_END];

    min[STANDARD_TYPE_BOOL] = "0";
    max[STANDARD_TYPE_BOOL] = "1";
    underflow[STANDARD_TYPE_BOOL] = "-1";
    overflow[STANDARD_TYPE_BOOL] = "2";

    min[STANDARD_TYPE_INT_8] = "-128";
    max[STANDARD_TYPE_INT_8] = "127";
    min[STANDARD_TYPE_UINT_8] = "0";
    max[STANDARD_TYPE_UINT_8] = "255";

    underflow[STANDARD_TYPE_INT_8] = "-129";
    overflow[STANDARD_TYPE_INT_8] = "128";
    underflow[STANDARD_TYPE_UINT_8] = "-1";
    overflow[STANDARD_TYPE_UINT_8] = "256";

    min[STANDARD_TYPE_INT_16] = "-32768";
    max[STANDARD_TYPE_INT_16] = "32767";
    min[STANDARD_TYPE_UINT_16] = "0";
    max[STANDARD_TYPE_UINT_16] = "65535";

    underflow[STANDARD_TYPE_INT_16] = "-32769";
    overflow[STANDARD_TYPE_INT_16] = "32768";
    underflow[STANDARD_TYPE_UINT_16] = "-1";
    overflow[STANDARD_TYPE_UINT_16] = "65536";

    min[STANDARD_TYPE_INT_32] = "-2147483648";
    max[STANDARD_TYPE_INT_32] = "2147483647";
    min[STANDARD_TYPE_UINT_32] = "0";
    max[STANDARD_TYPE_UINT_32] = "4294967295";

    underflow[STANDARD_TYPE_INT_32] = "-2147483649";
    overflow[STANDARD_TYPE_INT_32] = "2147483648";
    underflow[STANDARD_TYPE_UINT_32] = "-1";
    overflow[STANDARD_TYPE_UINT_32] = "4294967296";

    min[STANDARD_TYPE_INT_64] = "-9223372036854775808";
    max[STANDARD_TYPE_INT_64] = "9223372036854775807";
    min[STANDARD_TYPE_UINT_64] = "0";
    max[STANDARD_TYPE_UINT_64] = "18446744073709551615";

    underflow[STANDARD_TYPE_INT_64] = "-9223372036854775809";
    overflow[STANDARD_TYPE_INT_64] = "9223372036854775808";
    underflow[STANDARD_TYPE_UINT_64] = "-1";
    overflow[STANDARD_TYPE_UINT_64] = "18446744073709551616";

    min[STANDARD_TYPE_REAL] = "1.17549e-38";
    max[STANDARD_TYPE_REAL] = "3.40282e+38";
    min[STANDARD_TYPE_DOUBLE] = "2.22507e-308";
    max[STANDARD_TYPE_DOUBLE] = "1.79769e+308";

    // TODO: What to do for underflow of floats/doubles? 
    // Looks like they are just turned into 0s.
    underflow[STANDARD_TYPE_REAL] = "1.17549e-46";
    overflow[STANDARD_TYPE_REAL] = "3.40282e+39";
    underflow[STANDARD_TYPE_DOUBLE] = "2.22507e-324";
    overflow[STANDARD_TYPE_DOUBLE] = "1.79769e+309";

    for (uint i = STANDARD_TYPE_MIN; i < STANDARD_TYPE_END; i++) 
    {
        // First test setting output = input (using move)
        // Also test tonull
        // For max, min, NULL
        StandardTypeDescriptorOrdinal type = static_cast<StandardTypeDescriptorOrdinal>(i);
        string typestr = getTypeString(type, CalcAssemblerTestCase::MAX_WIDTH);
        string testdesc = "testStandardTypes: " + typestr;
        ostringstream testostr("");
        testostr << "I " << createRegisterString(typestr, 3) << ";" << endl;
        testostr << "O " << createRegisterString(typestr, 4) << ";" << endl;
        testostr << "T;" << endl;
        testostr << "MOVE O0, I0;" << endl;
        testostr << "MOVE O1, I1;" << endl;
        testostr << "MOVE O2, I2;" << endl;
        testostr << "TONULL O3;" << endl;
        string teststr = testostr.str();

        CalcAssemblerTestCase testCase1(__LINE__, testdesc.c_str(),
                                        teststr.c_str());
        if (testCase1.assemble())
        {
            testCase1.setInputMin(0);
            testCase1.setExpectedOutputMin(0);
            testCase1.setInputMax(1);
            testCase1.setExpectedOutputMax(1);
            testCase1.setInputNull(2);
            testCase1.setExpectedOutputNull(2);
            testCase1.setExpectedOutputNull(3);
            testCase1.test();
        }

        if (!StandardTypeDescriptor::isArray(type))
        {
            // Verify what we think is the min/max is the min/max
            assert (testCase1.getInput(0) == min[type]);
            assert (testCase1.getInput(1) == max[type]);
        }

        // Now test literal binding for the type 
        // For min, max, NULL
        ostringstream testostr2("");
        testostr2 << "O " << createRegisterString(typestr, 3) << ";" << endl;
        testostr2 << "C " << createRegisterString(typestr, 3) << ";" << endl;
        testostr2 << "V " << testCase1.getInput(0) 
                  << ", " << testCase1.getInput(1)
                  << ", " << testCase1.getInput(2)
                  << ";" << endl; 
        testostr2 << "T;" << endl;
        testostr2 << "MOVE O0, C0;" << endl;
        testostr2 << "MOVE O1, C1;" << endl;
        testostr2 << "MOVE O2, C2;" << endl;
        string teststr2 = testostr2.str();
        
        CalcAssemblerTestCase testCase2(__LINE__, testdesc.c_str(),
                                        teststr2.c_str());
        if (testCase2.assemble())
        {
            testCase2.setExpectedOutputMin(0);
            testCase2.setExpectedOutputMax(1);
            testCase2.setExpectedOutputNull(2);
            testCase2.test();
        }

        if (!StandardTypeDescriptor::isArray(type))
        {
            // Now test literal binding for the type 
            // For overflow and underflow
            ostringstream testostr3("");
            testostr3 << "O " << createRegisterString(typestr, 1) << ";" << endl;
            testostr3 << "C " << createRegisterString(typestr, 1) << ";" << endl;
            testostr3 << "V " << underflow[type] << ";" << endl;
            testostr3 << "T;" << endl;
            testostr3 << "MOVE O0, C0;" << endl;

            string teststr3 = testostr3.str();
            
            CalcAssemblerTestCase testCase3(__LINE__, testdesc.c_str(),
                                            teststr3.c_str());
            if (type == STANDARD_TYPE_INT_64 || type == STANDARD_TYPE_DOUBLE)
                testCase3.expectAssemblerError("out of");
            else if (underflow[type] == "-1")
                testCase3.expectAssemblerError("Invalid value");
            else testCase3.expectAssemblerError("bad numeric cast");

            testCase3.assemble();

            // For overflow and underflow
            ostringstream testostr4("");
            testostr4 << "O " << createRegisterString(typestr, 1) << ";" << endl;
            testostr4 << "C " << createRegisterString(typestr, 1) << ";" << endl;
            testostr4 << "V " << overflow[type] << ";" << endl;
            testostr4 << "T;" << endl;
            testostr4 << "MOVE O0, C0;" << endl;

            string teststr4 = testostr4.str();
            
            CalcAssemblerTestCase testCase4(__LINE__, testdesc.c_str(),
                                            teststr4.c_str());
            if (type == STANDARD_TYPE_UINT_64 || type == STANDARD_TYPE_DOUBLE)
                testCase4.expectAssemblerError("out of range");
            else if (type == STANDARD_TYPE_BOOL)
                testCase4.expectAssemblerError("Invalid value");
            else testCase4.expectAssemblerError("bad numeric cast");

            testCase4.assemble();
        }

        testBoolInstructions(type);

        switch (type) {
        case STANDARD_TYPE_UINT_8:
            testNativeInstructions<uint8_t>(type);
            break;

        case STANDARD_TYPE_INT_8:
            testNativeInstructions<int8_t>(type);
            break;

        case STANDARD_TYPE_UINT_16:
            testNativeInstructions<uint16_t>(type);
            break;

        case STANDARD_TYPE_INT_16:
            testNativeInstructions<int16_t>(type);
            break;

        case STANDARD_TYPE_UINT_32:
            testNativeInstructions<uint32_t>(type);
            break;

        case STANDARD_TYPE_INT_32:
            testNativeInstructions<int32_t>(type);
            break;

        case STANDARD_TYPE_UINT_64:
            testNativeInstructions<uint64_t>(type);
            break;

        case STANDARD_TYPE_INT_64:
            testNativeInstructions<int64_t>(type);
            break;

#if 0
        case STANDARD_TYPE_REAL:
            testNativeInstructions<float>(type);
            break;

        case STANDARD_TYPE_DOUBLE:
            testNativeInstructions<double>(type);
            break;
#endif

        default:
            break;
        }
    }
}

void CalcAssemblerTest::testLiteralBinding()
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
    teststr9 += stringToHex("test");
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
    teststr10 += stringToHex("short");
    teststr10 += "; T; MOVE O0, C0;";
    CalcAssemblerTestCase testCase10(__LINE__, "STRING (VARCHAR) = \"short\"", teststr10.c_str());
    if (testCase10.assemble()) {
        testCase10.setExpectedOutput<char>(0, "short", 5);
        testCase10.test();
    }

    // Test bind a string (varchar) that's too long
    string teststr11;
    teststr11 = "O vc,8, u8; C vc,8, u8; V 0x";
    teststr11 += stringToHex("muchtoolongstring");
    teststr11 += "; T; MOVE O0, C0;";
    
    CalcAssemblerTestCase testCase11(__LINE__, "STRING (VARCHAR) TOO LONG", teststr11.c_str());
    testCase11.expectAssemblerError("too long");
    testCase11.assemble();

    // Test bind a binary string (binary) that's too short
    string teststr12;
    teststr12 = "O c,100, u8; C c,100, u8; V 0x";
    teststr12 += stringToHex("binarytooshort");
    teststr12 += ", 60000000; T; MOVE O0, C0; MOVE O1, C1;";
    CalcAssemblerTestCase testCase12(__LINE__, "STRING (BINARY) TOO SHORT", teststr12.c_str());
    testCase12.expectAssemblerError("not equal");
    testCase12.assemble();
}

void CalcAssemblerTest::testAdd()
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

void CalcAssemblerTest::testBool()
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

void CalcAssemblerTest::testPointer()
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

void CalcAssemblerTest::testJump()
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

void CalcAssemblerTest::testReturn()
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

void convertFloatToInt(RegisterRef<int>* regOut,
                       RegisterRef<float>* regIn)
{
    regOut->value((int)regIn->value());
}

void CalcAssemblerTest::testExtended()
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

void CalcAssemblerTest::testInvalidPrograms()
{
    const char* parse_error = "error";

    CalcAssemblerTestCase testCase1(__LINE__, "JUNK", "Junk");
    testCase1.expectAssemblerError(parse_error);
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
    testCase4.expectAssemblerError(parse_error);
    testCase4.assemble();

    CalcAssemblerTestCase testCase5(__LINE__, "BAD INST", "I u2, u4;\nO u4;\nT;\nklk34dfw;");
    testCase5.expectAssemblerError(parse_error);
    testCase5.assemble();

    // Test invalid register index
    CalcAssemblerTestCase testCase6(__LINE__, "BAD REG INDEX", "I u2, u4;\nO u4;\nT;\n\nADD O0, I0, I12;");
    testCase6.expectAssemblerError("out of bounds");
    testCase6.assemble();

    // Test invalid register index
    CalcAssemblerTestCase testCase7(__LINE__, "BAD REG INDEX", "I u2, u4;\nO u4;\nT;\n"
                                    "ADD O0, I0, I888888888888888888888888888888888888888888;");
    testCase7.expectAssemblerError("out of range");
    testCase7.assemble();

    // TODO: Test extremely long programs and stuff
}

void CalcAssemblerTest::testAssembler()
{
    testLiteralBinding();
    testInvalidPrograms();
    testAdd();
    testBool();
    testPointer();
    testReturn();
    testJump();
    testExtended();
    testStandardTypes();
}

int main (int argc, char **argv)
{
    ProgramName = argv[0];
    InstructionFactory inst();
    InstructionFactory::registerInstructions();
  
    try {
        CalcAssemblerTest test;
        test.testAssembler();
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
