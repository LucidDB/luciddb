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
*/
#include "fennel/common/CommonPreamble.h"
#include "fennel/calc/Calculator.h"
#include "fennel/calc/Instruction.h"
#include "fennel/calc/ReturnException.h"
#include "fennel/calc/CalcAssembler.h"

#include "boost/format.hpp"
using boost::format;

FENNEL_BEGIN_CPPFILE("$Id$");

Calculator::Calculator() :
    mIsUsingAssembler(true),
    mIsAssembling(false)
{
    init(0,0,0,0,0,0);
}


Calculator::Calculator(int codeSize, int literalSize, int inputSize,
                       int outputSize, int localSize, int statusSize) :
    mIsUsingAssembler(false),
    mIsAssembling(false)
{
    init(codeSize, literalSize, inputSize, outputSize,
         localSize, statusSize);
}

void
Calculator::init(int codeSize, int literalSize, int inputSize,
                 int outputSize, int localSize, int statusSize)
{
    mCode.reserve(codeSize);
    mRegisterRef[RegisterReference::ELiteral].reserve(literalSize);
    mRegisterRef[RegisterReference::EInput].reserve(inputSize);
    mRegisterRef[RegisterReference::EOutput].reserve(outputSize);
    mRegisterRef[RegisterReference::ELocal].reserve(localSize);
    mRegisterRef[RegisterReference::EStatus].reserve(statusSize);

    int i;
    for (i = RegisterReference::EFirstSet; i < RegisterReference::ELastSet; i++) {
        // explicitly clear registers. allows detection of rebinding local & literal
        mRegisterTuple[i] = NULL;
        // explicitly clear descriptors. allows cleaner destructor
        mRegisterSetDescriptor[i] = NULL;
    }
    // Default is to use output register set by reference.
    mOutputRegisterByReference = true;
}

Calculator::~Calculator()
{
    uint i;
    for (i = RegisterReference::EFirstSet; i < RegisterReference::ELastSet; i++) {
        if (mRegisterSetDescriptor[i]) {
            delete mRegisterSetDescriptor[i];
            mRegisterSetDescriptor[i] = NULL;
        }
    }

    if (mIsUsingAssembler) {
        // Assembler created all these register references, let's delete them
        for (i = RegisterReference::EFirstSet; i < RegisterReference::ELastSet; i++) {
            for (uint reg=0; reg < mRegisterRef[i].size(); reg++) {
                if (mRegisterRef[i][reg])
                    delete mRegisterRef[i][reg];
            }
            mRegisterRef[i].clear();
            mRegisterReset.clear();
        }

        // Assembler created all these instructions so it's up to us to delete them
        for (i = 0; i < mCode.size(); i++) {
            delete mCode[i];
        }
        mCode.clear();

        // Assembler also created TupleData for status, literal, and local registers

        if (mRegisterTuple[RegisterReference::ELiteral]) {
            delete mRegisterTuple[RegisterReference::ELiteral];
        }
        
        if (mRegisterTuple[RegisterReference::ELocal]) {
            delete mRegisterTuple[RegisterReference::ELocal];
        }

        if (mRegisterTuple[RegisterReference::EStatus]) {
            delete mRegisterTuple[RegisterReference::EStatus];
        }

        // Assembler also allocated space for tuple data
        for (i=0; i < mBuffers.size(); i++) {
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
        TRACE_FINE,
        "Calculator assembly = |" << endl
        << program << "|" << endl);
    mIsAssembling = true;
    CalcAssembler assembler(this);
    assembler.assemble(program);
    mIsAssembling = false;
}

void
Calculator::bind(RegisterReference::ERegisterSet regset,
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
    assert((regset == RegisterReference::ELiteral) ? 
           !mRegisterTuple[RegisterReference::ELiteral] : true);
    assert((regset == RegisterReference::ELocal) ? 
           !mRegisterTuple[RegisterReference::ELocal] : true);
            
    mRegisterTuple[regset] = data;
    mRegisterSetDescriptor[regset] = new TupleDescriptor(desc);

    // cache pointers for local and literal sets only
    if (regset == RegisterReference::ELiteral ||
        regset == RegisterReference::ELocal) {
        for_each(mRegisterRef[regset].begin(),
                 mRegisterRef[regset].end(),
                 mem_fun(&RegisterReference::cachePointer));
    }
    
    // pre-allocate mReset vector to the largest possible value
    // trade memory for speed - vector should never have to reallocate
    // TODO: This calls reserve twice, which is wasteful, even at startup.
    size_t totalResetableRegisters = 
        mRegisterRef[RegisterReference::ELiteral].size() +
        mRegisterRef[RegisterReference::ELocal].size();
    mRegisterReset.reserve(totalResetableRegisters);
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
    return mRegisterTuple[RegisterReference::EStatus];
}

void
Calculator::exec()
{
    // Clear state from previous execution
    mWarnings.clear();

    // reset altered registers
    for_each(mRegisterReset.begin(),
             mRegisterReset.end(),
             mem_fun(&RegisterReference::cachePointer));
    mRegisterReset.clear();    // does not change capacity

#ifdef DEBUG
    ostringstream oss;
    TuplePrinter p;
    oss << "Pre-Exec" << endl << "Output Register: " << endl;
    p.print(oss, getOutputRegisterDescriptor(),
            *(mRegisterTuple[RegisterReference::EOutput]));
    oss << endl << "Input Register: " << endl;
    p.print(oss, getInputRegisterDescriptor(), 
            *(mRegisterTuple[RegisterReference::EInput]));
    oss << endl;
    string forsomereasonthisisneeded = oss.str();
    FENNEL_TRACE(TRACE_FINER, forsomereasonthisisneeded);
#endif
                 

    TProgramCounter pc = 0, endOfProgram;
    endOfProgram = mCode.size();

    while (pc >= 0 && pc < endOfProgram) {
        try {
#ifdef DEBUG
            int oldpc = pc;
            string out;
            mCode[oldpc]->describe(out, true);
            FENNEL_TRACE(TRACE_FINER,
                         "BF [" << oldpc << "] " <<  out.c_str());
#endif 
            
            mCode[pc]->exec(pc);

#ifdef DEBUG
            mCode[oldpc]->describe(out, true);
            FENNEL_TRACE(TRACE_FINER,
                         "AF [" << oldpc << "] " <<  out.c_str());
#endif
        }

        catch(CalcMessage m) {
            // each instruction sets pc assuming continued execution
            mWarnings.push_back(m);
        }
        catch(ReturnException m) {
            break;
        }
    }
#ifdef DEBUG
    oss.clear();
    oss << "Post-Exec" << endl << "Output Register: " << endl;
    p.print(oss, getOutputRegisterDescriptor(),
            *(mRegisterTuple[RegisterReference::EOutput]));
    oss << endl << "Input Register: " << endl;
    p.print(oss, getInputRegisterDescriptor(), 
            *(mRegisterTuple[RegisterReference::EInput]));
    oss << endl << "Status Register: " << endl;
    p.print(oss, getStatusRegisterDescriptor(), 
            *(mRegisterTuple[RegisterReference::EStatus]));
    oss << endl;
    forsomereasonthisisneeded = oss.str();
    FENNEL_TRACE(TRACE_FINER, forsomereasonthisisneeded);
#endif
}

string
Calculator::warnings()
{
    string ret;
    deque<CalcMessage>::iterator iter = mWarnings.begin(),
        end = mWarnings.end();
    int i = 0;

    while(iter != end) {
        ret += boost::io::str( format("[%d]:PC=%ld Code=") % i % iter->mPc);
        ret += iter->mStr;
        ret += " ";
        iter++;
        i++;
    }
    return ret;
}

FENNEL_END_CPPFILE("$Id$");

// End Calculator.cpp
