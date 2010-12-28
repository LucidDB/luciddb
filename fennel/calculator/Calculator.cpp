/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005 The Eigenbase Project
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
*/
#include "fennel/common/CommonPreamble.h"
#include "fennel/calculator/Calculator.h"
#include "fennel/calculator/Instruction.h"
#include "fennel/calculator/CalcAssembler.h"

#include "boost/format.hpp"
using boost::format;

FENNEL_BEGIN_CPPFILE("$Id$");

Calculator::Calculator(DynamicParamManager* dynamicParamManager)
    : mIsUsingAssembler(true),
      mIsAssembling(false),
      mPDynamicParamManager(dynamicParamManager)
{
    init(0, 0, 0, 0, 0, 0);
}


Calculator::Calculator(
    DynamicParamManager* dynamicParamManager,
    int codeSize, int literalSize, int inputSize,
    int outputSize, int localSize, int statusSize)
    : mIsUsingAssembler(false),
      mIsAssembling(false),
      mPDynamicParamManager(dynamicParamManager)
{
    init(
        codeSize, literalSize, inputSize, outputSize,
        localSize, statusSize);
}

void
Calculator::init(
    int codeSize, int literalSize, int inputSize,
    int outputSize, int localSize, int statusSize)
{
    mCode.reserve(codeSize);
    mRegisterRef[RegisterReference::ELiteral].reserve(literalSize);
    mRegisterRef[RegisterReference::EInput].reserve(inputSize);
    mRegisterRef[RegisterReference::EOutput].reserve(outputSize);
    mRegisterRef[RegisterReference::ELocal].reserve(localSize);
    mRegisterRef[RegisterReference::EStatus].reserve(statusSize);

    int i;
    for (i = RegisterReference::EFirstSet;
         i < RegisterReference::ELastSet;
         i++)
    {
        // explicitly clear registers. allows detection of rebinding
        // local & literal
        mRegisterSetBinding[i] = NULL;
        // explicitly clear descriptors. allows cleaner destructor
        mRegisterSetDescriptor[i] = NULL;
    }
    // Default is to use output register set by reference.
    mOutputRegisterByReference = true;

    // Default is to continue execution after exceptions
    mContinueOnException = true;
}

Calculator::~Calculator()
{
    uint i;
    for (i = RegisterReference::EFirstSet;
         i < RegisterReference::ELastSet;
         i++)
    {
        unbind((RegisterReference::ERegisterSet)i);
    }

    if (mIsUsingAssembler) {
        // Assembler created all these register references, let's delete them
        for (i = RegisterReference::EFirstSet;
             i < RegisterReference::ELastSet;
             i++)
        {
            for (uint reg = 0; reg < mRegisterRef[i].size(); reg++) {
                if (mRegisterRef[i][reg]) {
                    delete mRegisterRef[i][reg];
                }
            }
            mRegisterRef[i].clear();
            mRegisterReset.clear();
        }

        // Assembler created all these instructions so it's up to us
        // to delete them
        for (i = 0; i < mCode.size(); i++) {
            delete mCode[i];
        }
        mCode.clear();

        for (i = 0; i < mBuffers.size(); i++) {
            delete[] mBuffers[i];
        }
        mBuffers.clear();
    }
}

void
Calculator::outputRegisterByReference(bool flag)
{
    mOutputRegisterByReference = flag;
}

void
Calculator::assemble(const char *program)
{
    assert(mIsUsingAssembler);

    FENNEL_TRACE(
        TRACE_FINEST,
        "Calculator instructions:" << endl
        << InstructionFactory::signatures());
    FENNEL_TRACE(
        TRACE_FINEST,
        "Calculator extended instructions:" << endl
        << InstructionFactory::extendedSignatures());
    FENNEL_TRACE(
        TRACE_FINE,
        "Calculator assembly = |" << endl
        << program << "|" << endl);

    mIsAssembling = true;
    CalcAssembler assembler(this);
    assembler.assemble(program);
    mIsAssembling = false;
}

void  Calculator::unbind(
    RegisterReference::ERegisterSet regset,
    bool unbindDescriptor)
{
    if (unbindDescriptor && mRegisterSetDescriptor[regset]) {
        delete mRegisterSetDescriptor[regset];
        mRegisterSetDescriptor[regset] = NULL;
    }
    if (mRegisterSetBinding[regset]) {
        delete mRegisterSetBinding[regset];
        mRegisterSetBinding[regset] = NULL;
    }
}

void Calculator::bind(
    RegisterReference::ERegisterSet regset,
    TupleData* data,
    const TupleDescriptor& desc)
{
    assert(mIsUsingAssembler ? mIsAssembling : true);
    assert(regset < RegisterReference::ELastSet);
    assert(data); // Not strictly needed

    // At the moment, do not allow literal and local register sets
    // to ever be rebound as they (may) have cached pointers.
    // If rebinding these register sets is a required feature, you
    // must reset each RegisterReference that points to these
    // tuples
    assert(
        (regset == RegisterReference::ELiteral)
        ? !mRegisterSetBinding[RegisterReference::ELiteral]
        : true);
    assert(
        (regset == RegisterReference::ELocal)
        ? !mRegisterSetBinding[RegisterReference::ELocal]
        : true);

    unbind(regset);
    mRegisterSetBinding[regset] = new RegisterSetBinding(data);
    mRegisterSetDescriptor[regset] = new TupleDescriptor(desc);

    // cache pointers for local and literal sets only
    if (regset == RegisterReference::ELiteral
        || regset == RegisterReference::ELocal)
    {
        for_each(
            mRegisterRef[regset].begin(),
            mRegisterRef[regset].end(),
            mem_fun(&RegisterReference::cachePointer));
    }

    // pre-allocate mReset vector to the largest possible value
    // trade memory for speed - vector should never have to reallocate
    // TODO: This calls reserve twice, which is wasteful, even at startup.
    size_t totalResetableRegisters =
        mRegisterRef[RegisterReference::ELiteral].size()
        + mRegisterRef[RegisterReference::ELocal].size();
    mRegisterReset.reserve(totalResetableRegisters);
}

// PERFORMANCE: If RegisterSetBinding is NULL, create a new instance.
// Otherwise, rebind new TupleData values to it.
// Earlier, we called unbind() and created a new instance of RegisterSetBinding.
// bind() could potentially be called for every row in an XO.
void Calculator::bind(
    TupleData* input, TupleData* output,
    bool takeOwnwership, const TupleData* outputWrite)
{
    if (mRegisterSetBinding[RegisterReference::EInput] == NULL) {
        mRegisterSetBinding[RegisterReference::EInput] =
            new RegisterSetBinding(input, takeOwnwership);
    } else {
        mRegisterSetBinding[RegisterReference::EInput]->rebind(
            input, takeOwnwership);
    }

    if (mRegisterSetBinding[RegisterReference::EOutput] == NULL) {
        if (outputWrite) {
            mRegisterSetBinding[RegisterReference::EOutput] =
                new RegisterSetBinding(output, outputWrite, takeOwnwership);
        } else {
            mRegisterSetBinding[RegisterReference::EOutput] =
                new RegisterSetBinding(output, takeOwnwership);
        }
    } else {
        if (outputWrite) {
            mRegisterSetBinding[RegisterReference::EOutput]->rebind(
                output, outputWrite, takeOwnwership);
        } else {
            mRegisterSetBinding[RegisterReference::EOutput]->rebind(
                output, takeOwnwership);
        }
    }
}

TupleDescriptor
Calculator::getOutputRegisterDescriptor() const
{
    return *(mRegisterSetDescriptor[RegisterReference::EOutput]);
}

TupleDescriptor
Calculator::getInputRegisterDescriptor() const
{
    return *(mRegisterSetDescriptor[RegisterReference::EInput]);
}

TupleDescriptor
Calculator::getStatusRegisterDescriptor() const
{
    return *(mRegisterSetDescriptor[RegisterReference::EStatus]);
}

TupleData const * const
Calculator::getStatusRegister() const
{
    return &(mRegisterSetBinding[RegisterReference::EStatus]->asTupleData());
}

void Calculator::zeroStatusRegister()
{
    if (mRegisterSetDescriptor[RegisterReference::EStatus] != NULL) {
        RegisterSetBinding *statusBinding =
            mRegisterSetBinding[RegisterReference::EStatus];

        int ncols = statusBinding->asTupleData().size();

        for (int i = 0; i < ncols; i++) {
            memset(
                const_cast<PBuffer>((*statusBinding)[i].pData),
                0,
                (*statusBinding)[i].cbData);
        }
    }
}

void
Calculator::continueOnException(bool c)
{
    mContinueOnException = c;
}


void
Calculator::exec()
{
    // Clear state from previous execution
    mWarnings.clear();

    // reset altered registers
    for_each(
        mRegisterReset.begin(),
        mRegisterReset.end(),
        mem_fun(&RegisterReference::cachePointer));
    mRegisterReset.clear();    // does not change capacity

#ifdef DEBUG
    ostringstream oss;
    TuplePrinter p;
    if (isTracingLevel(TRACE_FINEST)) {
        oss << "Pre-Exec" << endl << "Output Register: " << endl;
        p.print(
            oss, getOutputRegisterDescriptor(),
            mRegisterSetBinding[RegisterReference::EOutput]->asTupleData());
        oss << endl << "Input Register: " << endl;
        p.print(
            oss, getInputRegisterDescriptor(),
            mRegisterSetBinding[RegisterReference::EInput]->asTupleData());
        oss << endl << "Status Register: " << endl;
        p.print(
            oss, getStatusRegisterDescriptor(),
            mRegisterSetBinding[RegisterReference::EStatus]->asTupleData());
        oss << endl;
        trace(TRACE_FINEST, oss.str());
    }
#endif


    TProgramCounter pc = 0, endOfProgram;
    endOfProgram = mCode.size();

    while (pc >= 0 && pc < endOfProgram) {
        try {
#ifdef DEBUG
            int oldpc = pc;
            string out;
            if (isTracingLevel(TRACE_FINEST)) {
                mCode[oldpc]->describe(out, true);
                FENNEL_TRACE(
                    TRACE_FINEST, "BF [" << oldpc << "] " <<  out.c_str());
            }
#endif

            mCode[pc]->exec(pc);

#ifdef DEBUG
            if (isTracingLevel(TRACE_FINEST)) {
                mCode[oldpc]->describe(out, true);
                FENNEL_TRACE(
                    TRACE_FINEST, "AF [" << oldpc << "] " <<  out.c_str());
            }
#endif
        } catch (CalcMessage m) {
            // each instruction sets pc assuming continued execution
            mWarnings.push_back(m);
            if (!mContinueOnException) {
                break;
            }
        }
    }
#ifdef DEBUG
    if (isTracingLevel(TRACE_FINEST)) {
        oss.clear();
        oss << "Post-Exec" << endl << "Output Register: " << endl;
        p.print(
            oss, getOutputRegisterDescriptor(),
            mRegisterSetBinding[RegisterReference::EOutput]->asTupleData());
        oss << endl << "Input Register: " << endl;
        p.print(
            oss, getInputRegisterDescriptor(),
            mRegisterSetBinding[RegisterReference::EInput]->asTupleData());
        oss << endl << "Status Register: " << endl;
        p.print(
            oss, getStatusRegisterDescriptor(),
            mRegisterSetBinding[RegisterReference::EStatus]->asTupleData());
        oss << endl << "Warnings: |" << warnings() << "|"<< endl;
        trace(TRACE_FINEST, oss.str());
    }
#endif
}

string
Calculator::warnings()
{
    string ret;
    deque<CalcMessage>::iterator iter = mWarnings.begin(),
        end = mWarnings.end();
    int i = 0;

    while (iter != end) {
        ret += boost::io::str(format("[%d]:PC=%ld Code=") % i % iter->pc);
        ret += iter->str;
        ret += " ";
        iter++;
        i++;
    }
    return ret;
}

FENNEL_END_CPPFILE("$Id$");

// End Calculator.cpp
