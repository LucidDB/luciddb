/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2004-2005 Disruptive Tech
// Copyright (C) 2005-2005 The Eigenbase Project
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
#include "fennel/disruptivetech/calc/InstructionCommon.h"
#include "fennel/disruptivetech/calc/CalcAssembler.h"

#include <strstream>
#include "boost/cast.hpp"

FENNEL_BEGIN_CPPFILE("$Id$");

using namespace std;
using namespace boost;

CalcAssembler::~CalcAssembler()
{
    for (uint i = RegisterReference::EFirstSet; i < RegisterReference::ELastSet; i++) {
        if (mCalc->mRegisterTuple[i] == NULL)
        {
            // We did NOT successfully bind this register set to the calculator
            // Will need to delete it on our own
            if (mBuffers[i])
                delete[] mBuffers[i];

            if (mRegisterTupleData[i])
                delete mRegisterTupleData[i];
        }
    }
}

void CalcAssembler::init()
{
    mCurrentRegSet = RegisterReference::EUnknown;
    mLiteralIndex  = 0;
    mMaxPC         = 0;

    /* Initialize tuple descriptors and tuple data */
    for (uint i=RegisterReference::EFirstSet; i<RegisterReference::ELastSet; i++)
    {
        mRegisterSetDescriptor[i].clear();
        mRegisterTupleData[i] = NULL;
        mBuffers[i] = NULL;
    }
}

int CalcAssembler::assemble(const char* program)
{
    int res = 0;
    istringstream istr(program);
    mLexer.switch_streams(&istr, 0);
    try {
        assemble();
    }
    catch (CalcAssemblerException& ex)
    {
        ex.setCode(program);
        throw ex;
    }

    return res;
}

int CalcAssembler::assemble()
{
    int res = 0;
    try {
        mCalc->mIsAssembling = true;
        res = CalcYYparse((void*) this);
        if (res != 0)
            throw CalcAssemblerException("Error assembling program", getLexer().getLocation());

        // Done assembling - let's check the maximum PC (used in Jump instruction)
        checkPC(mMaxPC, mMaxPCLoc);
    }
    catch (CalcAssemblerException& ex) 
    {
        if (!ex.mLocValid)
            ex.setLocation(getLexer().getLocation());
        throw ex;
    }
    catch (FennelExcn& ex) {
        throw CalcAssemblerException(ex.getMessage(), getLexer().getLocation());
    }
    catch (exception& ex) {
        throw CalcAssemblerException(ex.what(), getLexer().getLocation());
    }
        
    return res;
}

void CalcAssembler::setTupleDatum(StandardTypeDescriptorOrdinal type,
                                  TupleDatum& tupleDatum,
                                  TupleAttributeDescriptor& desc /* Unused */,
                                  PConstBuffer buffer)
{
    tupleDatum.pData = buffer;
}

void CalcAssembler::setTupleDatum(StandardTypeDescriptorOrdinal type,
                                  TupleDatum& tupleDatum,  
                                  TupleAttributeDescriptor& desc, /* Unused */
                                  double value)
{
    switch (type) 
    {
    case STANDARD_TYPE_REAL:

        *(reinterpret_cast<float *>(const_cast<PBuffer>(tupleDatum.pData))) = 
            numeric_cast<float>(value);

        // Check for underflow where the value becomes 0
        // NOTE: Underflows that causes precision loss but does not become 0
        //       are ignored for now.
        if ((value != 0) && 
            (*(reinterpret_cast<float *>(const_cast<PBuffer>(tupleDatum.pData))) == 0))
            throw InvalidValueException<double>("bad numeric cast: underflow", type, value);
        break;
    case STANDARD_TYPE_DOUBLE:
        *(reinterpret_cast<double *>(const_cast<PBuffer>(tupleDatum.pData))) = 
            numeric_cast<double>(value);
        break;
    default:
        // Invalid real type - horrible, horrible
        throw InvalidValueException<double>("Cannot assign double", type, value);
    }
}

void CalcAssembler::setTupleDatum(StandardTypeDescriptorOrdinal type,
                                  TupleDatum& tupleDatum, 
                                  TupleAttributeDescriptor& desc, /* Unused */
                                  uint64_t value)
{
    switch (type) 
    {
    case STANDARD_TYPE_INT_8:
        *(reinterpret_cast<int8_t *>(const_cast<PBuffer>(tupleDatum.pData))) =
            numeric_cast<int8_t>(value);
        break;
    case STANDARD_TYPE_UINT_8:
        *(reinterpret_cast<uint8_t *>(const_cast<PBuffer>(tupleDatum.pData))) = 
            numeric_cast<uint8_t>(value);
        break;
    case STANDARD_TYPE_INT_16:
        *(reinterpret_cast<int16_t *>(const_cast<PBuffer>(tupleDatum.pData))) = 
                numeric_cast<int16_t>(value);
        break;
    case STANDARD_TYPE_UINT_16:
        *(reinterpret_cast<uint16_t *>(const_cast<PBuffer>(tupleDatum.pData))) = 
            numeric_cast<uint16_t>(value);
        break;
    case STANDARD_TYPE_INT_32:
        *(reinterpret_cast<int32_t *>(const_cast<PBuffer>(tupleDatum.pData))) =
            numeric_cast<int32_t>(value);
        break;
    case STANDARD_TYPE_UINT_32:
        *(reinterpret_cast<uint32_t *>(const_cast<PBuffer>(tupleDatum.pData))) = 
            numeric_cast<uint32_t>(value);
        break;
    case STANDARD_TYPE_INT_64:
        // Explicitly check for overflow of int64_t because the boost numeric_cast
        // does not throw an exception in this case
        if (value > std::numeric_limits<int64_t>::max())
            throw InvalidValueException<uint64_t>("bad numeric cast: overflow", type, value);
        *(reinterpret_cast<int64_t *>(const_cast<PBuffer>(tupleDatum.pData))) =
            numeric_cast<int64_t>(value);
        break;
    case STANDARD_TYPE_UINT_64:
        *(reinterpret_cast<uint64_t *>(const_cast<PBuffer>(tupleDatum.pData))) = 
            numeric_cast<uint64_t>(value);
            break;
    case STANDARD_TYPE_BOOL:
        // Booleans are specifed as 0 or 1
        if (value == 1) {
            *(reinterpret_cast<bool *>(const_cast<PBuffer>(tupleDatum.pData))) = true;
        }
        else if (value == 0) {
            *(reinterpret_cast<bool *>(const_cast<PBuffer>(tupleDatum.pData))) = false;
        }
        else {
            // Invalid boolean value
            throw InvalidValueException<uint64_t>("Boolean value should be 0 or 1", type, value);
        }
        break;
    default:
        // Invalid unsigned integer type - horrible, horrible
        throw InvalidValueException<uint64_t>("Cannot assign unsigned integer", type, value);
    }
}

void CalcAssembler::setTupleDatum(StandardTypeDescriptorOrdinal type,
                                  TupleDatum& tupleDatum, 
                                  TupleAttributeDescriptor& desc, /* Unused */
                                  int64_t value)
{
    switch (type) 
    {
    case STANDARD_TYPE_INT_8:
        *(reinterpret_cast<int8_t *>(const_cast<PBuffer>(tupleDatum.pData))) = 
            numeric_cast<int8_t>(value);
        break;
    case STANDARD_TYPE_INT_16:
        *(reinterpret_cast<int16_t *>(const_cast<PBuffer>(tupleDatum.pData))) = 
            numeric_cast<int16_t>(value);
        break;
    case STANDARD_TYPE_INT_32:
        *(reinterpret_cast<int32_t *>(const_cast<PBuffer>(tupleDatum.pData))) = 
            numeric_cast<int32_t>(value);
        break;
    case STANDARD_TYPE_INT_64:
        *(reinterpret_cast<int64_t *>(const_cast<PBuffer>(tupleDatum.pData))) =
            numeric_cast<int64_t>(value);
        break;
    default:
        throw InvalidValueException<int64_t>("Cannot assign signed integer", type, value);
    }
}

void CalcAssembler::setTupleDatum(StandardTypeDescriptorOrdinal type,
                                  TupleDatum& tupleDatum, 
                                  TupleAttributeDescriptor& desc,
                                  string str)
{
    ostringstream errorStr;
    char* ptr = reinterpret_cast<char*>(const_cast<PBuffer>(tupleDatum.pData));
    switch (type) 
    {
    case STANDARD_TYPE_CHAR:
    case STANDARD_TYPE_BINARY:
        // Fixed length storage
        // For fixed length arrays, cbData should be the same as cbStorage 
        assert(tupleDatum.cbData == desc.cbStorage);

        // Fixed width arrays should be padded to be the specifed width
        // Verify that the number of bytes matches the specified width
        if (str.length() != tupleDatum.cbData)
        {
            ostringstream ostr("");
            ostr << "String length " << str.length()
                 << " not equal to fixed size array of length "
                 << tupleDatum.cbData;
            throw FennelExcn(ostr.str());
        }

        // Copy the string
        memcpy(ptr, str.data(), str.length());
        break;
        
    case STANDARD_TYPE_VARCHAR:
    case STANDARD_TYPE_VARBINARY:
        // Variable length storage

        // Verify that there the length of the string is not larger than the
        // maximum length
        if (str.length() > desc.cbStorage)
        {
            ostringstream ostr("");
            ostr << "String length " << str.length()
                 << " too long for variabled sized array of maximum length "
                 << desc.cbStorage;
            throw FennelExcn(ostr.str());
        }

        // Copy the string
        memcpy(ptr, str.data(), str.length());
        tupleDatum.cbData = str.length();
        break;
        
    default:
        throw InvalidValueException<string>("Cannot assign string", type, str);
    }
}

void CalcAssembler::bindLiteralDone()
{
    // We are done with binding literals
    // Check that all literals have been a value
    TRegisterIndex regSize = getRegisterSize(RegisterReference::ELiteral);
    if (mLiteralIndex != regSize)
    {
        ostringstream errorStr("");
        errorStr << "Error binding literals: " << regSize << " literal registers, only "
                 << mLiteralIndex << " registers bound";
        throw CalcAssemblerException(errorStr.str(), getLexer().getLocation());
    }
}

void CalcAssembler::selectRegisterSet(RegisterReference::ERegisterSet setIndex)
{
    mCurrentRegSet = setIndex;
}

StandardTypeDescriptorOrdinal CalcAssembler::getRegisterType(RegisterReference::ERegisterSet setIndex, 
                                                             TRegisterIndex regIndex)
{
    RegisterReference* regRef = getRegister(setIndex, regIndex);
    assert(regRef != NULL);
    return regRef->type();
}

RegisterReference* CalcAssembler::getRegister(RegisterReference::ERegisterSet setIndex, 
                                              TRegisterIndex regIndex)
{
    assert(setIndex < RegisterReference::ELastSet);
    TRegisterIndex size = getRegisterSize(setIndex);
    if (regIndex >= size)
    {
        ostringstream errorStr("");
        errorStr << "Register index " << regIndex << " out of bounds.";
        errorStr << " Register set " << RegisterReference::getSetName(setIndex)
                 << " has " << size << " registers.";
        throw CalcAssemblerException(errorStr.str(), getLexer().getLocation());
    }
    return mCalc->mRegisterRef[setIndex][regIndex];
}

TRegisterIndex CalcAssembler::getRegisterSize(RegisterReference::ERegisterSet setIndex)
{
    assert(setIndex < RegisterReference::ELastSet);
    return mCalc->mRegisterRef[setIndex].size();
}

TupleData* CalcAssembler::getTupleData(RegisterReference::ERegisterSet setIndex)
{
    assert(setIndex < RegisterReference::ELastSet);
    return mRegisterTupleData[setIndex];
}

TupleDescriptor& CalcAssembler::getTupleDescriptor(RegisterReference::ERegisterSet setIndex)
{
    assert(setIndex < RegisterReference::ELastSet);
    return mRegisterSetDescriptor[setIndex];
}

/* Need factory? */
RegisterReference* CalcAssembler::createRegisterReference(RegisterReference::ERegisterSet setIndex,
                                                          TRegisterIndex                  regIndex,
                                                          StandardTypeDescriptorOrdinal   regType)
{
    // TODO: check setIndex and regIndex
    RegisterReference* regRef = NULL;
    switch (regType)
    {
    case STANDARD_TYPE_INT_8:
        regRef = new RegisterRef<int8_t>(setIndex, regIndex, regType);
        break;
    case STANDARD_TYPE_UINT_8:
        regRef = new RegisterRef<uint8_t>(setIndex, regIndex, regType);
        break;
    case STANDARD_TYPE_INT_16:
        regRef = new RegisterRef<int16_t>(setIndex, regIndex, regType);
        break;
    case STANDARD_TYPE_UINT_16:
        regRef = new RegisterRef<uint16_t>(setIndex, regIndex, regType);
        break;
    case STANDARD_TYPE_INT_32:
        regRef = new RegisterRef<int32_t>(setIndex, regIndex, regType);
        break;
    case STANDARD_TYPE_UINT_32:
        regRef = new RegisterRef<uint32_t>(setIndex, regIndex, regType);
        break;
    case STANDARD_TYPE_INT_64:
        regRef = new RegisterRef<int64_t>(setIndex, regIndex, regType);
        break;
    case STANDARD_TYPE_UINT_64:
        regRef = new RegisterRef<uint64_t>(setIndex, regIndex, regType);
        break;
    case STANDARD_TYPE_REAL:
        regRef = new RegisterRef<float>(setIndex, regIndex, regType);
        break;
    case STANDARD_TYPE_DOUBLE:
        regRef = new RegisterRef<double>(setIndex, regIndex, regType);
        break;
    case STANDARD_TYPE_BOOL:
        regRef = new RegisterRef<bool>(setIndex, regIndex, regType);
        break;
    case STANDARD_TYPE_CHAR:
    case STANDARD_TYPE_VARCHAR:
        regRef = new RegisterRef<char*>(setIndex, regIndex, regType);
        break;
    case STANDARD_TYPE_BINARY:
    case STANDARD_TYPE_VARBINARY:
        regRef = new RegisterRef<int8_t*>(setIndex, regIndex, regType);
        break;
    default: 
        ostringstream errorStr("");
        errorStr << "Error creating register reference for " 
                 << RegisterReference::toString(setIndex, regIndex) << ": ";
        errorStr << "Unsupported register type " << regType;
        throw CalcAssemblerException(errorStr.str());
        break;
    }
    return regRef;
}

void CalcAssembler::addRegister(RegisterReference::ERegisterSet setIndex,
                                StandardTypeDescriptorOrdinal   regType,
                                TupleStorageByteLength          cbStorage)
{
    assert(mCurrentRegSet < RegisterReference::ELastSet);
    bool isNullable = true;

    /* Add to tuple descriptor */
    StoredTypeDescriptor const &typeDesc = mTypeFactory.newDataType(regType);
    getTupleDescriptor(mCurrentRegSet).push_back(TupleAttributeDescriptor(typeDesc, isNullable, cbStorage));

    /* Add register to calculator */
    TRegisterIndex regIndex = mCalc->mRegisterRef[setIndex].size();
    RegisterReference* regRef = createRegisterReference(setIndex, regIndex, regType);
    mCalc->appendRegRef(regRef);
}

void CalcAssembler::addRegister(StandardTypeDescriptorOrdinal const regType, TupleStorageByteLength cbStorage)
{
    addRegister(mCurrentRegSet, regType, cbStorage);
}

TupleData* CalcAssembler::createTupleData(TupleDescriptor const& tupleDesc, FixedBuffer** buf)
{
    assert(buf != NULL);

    /* Prepare tuples - should only do this for literal/local */
    /* Compute memory layout and access */
    TupleAccessor tupleAccessor;
    tupleAccessor.compute(tupleDesc, TUPLE_FORMAT_ALL_NOT_NULL_AND_FIXED);

    /* Allocate memory */
    *buf = new FixedBuffer[tupleAccessor.getMaxByteCount()];

    /* Link memory - Who will delete this????? */
    tupleAccessor.setCurrentTupleBuf(*buf);
  
    /* Create Tuple Data - to be deleted by the calculator */
    TupleData* pTupleData = new TupleData(tupleDesc);
  
    /* Link Tuple Data with Tuple Accessor memory */
    tupleAccessor.unmarshal(*pTupleData);
    return pTupleData;
}


void CalcAssembler::allocateTuples()
{
    /* Allocate memory for the tuples */
    for (uint reg = RegisterReference::EFirstSet; reg < RegisterReference::ELastSet; reg++)
    {
        /* Verify that tuples have not already been allocated */
        assert(mRegisterTupleData[reg] == NULL);
        assert(mBuffers[reg] == NULL);

        if (reg == RegisterReference::ELiteral || 
            reg == RegisterReference::EStatus ||
            reg == RegisterReference::ELocal)
        {
            /* Allocate tuple for literal/status/local registers */
            mRegisterTupleData[reg] = createTupleData(mRegisterSetDescriptor[reg],
                                                      &mBuffers[reg]);
        }
     
        /* Do not need to create input/output tuple data */
    }
}

void CalcAssembler::bindRegisters()
{
    /* Bind registers to calculator */
    RegisterReference::ERegisterSet reg;

    /* Bind literal */
    reg = RegisterReference::ELiteral;
    mCalc->mBuffers.push_back(mBuffers[reg]);
    mCalc->bind(reg, getTupleData(reg), getTupleDescriptor(reg));

    /* Bind status */
    reg = RegisterReference::EStatus;
    mCalc->mBuffers.push_back(mBuffers[reg]);
    mCalc->bind(reg, getTupleData(reg), getTupleDescriptor(reg));

    /* Bind local */
    reg = RegisterReference::ELocal;
    mCalc->mBuffers.push_back(mBuffers[reg]);
    mCalc->bind(reg, getTupleData(reg), getTupleDescriptor(reg));

    /* Do not create input/output tuple data - we still need to bind the tuple descriptor */
    reg = RegisterReference::EInput;
    mCalc->mRegisterSetDescriptor[reg] = new TupleDescriptor(getTupleDescriptor(reg));

    reg = RegisterReference::EOutput;
    mCalc->mRegisterSetDescriptor[reg] = new TupleDescriptor(getTupleDescriptor(reg));
}

void CalcAssembler::addInstruction(Instruction* inst)
{
    assert(inst != NULL);
    mCalc->appendInstruction(inst);
}

FENNEL_END_CPPFILE("$Id$");

// End CalcAssembler.cpp
