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
#ifndef Fennel_Calculator_Included
#define Fennel_Calculator_Included

#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/tuple/TupleData.h"
#include "fennel/tuple/TupleAccessor.h"
#include "fennel/tuple/TuplePrinter.h"

#include <stdio.h>
#include <assert.h>

#include <algorithm>
#include <vector>
#include <string>
#include <deque>

FENNEL_BEGIN_NAMESPACE

class Instruction;
class TupleData;

FENNEL_END_NAMESPACE

#include "fennel/calc/CalcTypedefs.h"
#include "fennel/calc/CalcMessage.h"
#include "fennel/calc/RegisterReference.h"
#include "fennel/common/TraceSource.h"

FENNEL_BEGIN_NAMESPACE

using namespace std;


class Calculator : virtual public TraceSource
{
public:
    //! Constructor for XOs that will use assemble().
    explicit
    Calculator();

    //! Constructor for XOs that will populate Calculator piecemeal.
    //!
    //! Cannot use the assembler with this format.
    //! @param codeSize size of Instruction vector
    //! @param literalSize size of literal RegisterReference vector
    //! @param inputSize size of literal RegisterReference vector
    //! @param outputSize size of literal RegisterReference vector
    //! @param localSize size of literal RegisterReference vector
    //! @param statusSize size of literal RegisterReference vector
    explicit
    Calculator(int codeSize, int literalSize, int inputSize,
               int outputSize, int localSize, int statusSize);

    ~Calculator();


    //
    // Pre-Execution Configuration
    //

    //! Pre-execution: Output register by reference only?
    //! Must be set before appending instructions.
    void outputRegisterByReference(bool flag);

    //! Pre-execution: Append an Instruction to the Calculator
    void appendInstruction(Instruction* newInst) 
    {
        assert(mIsUsingAssembler ? mIsAssembling : true);
        mCode.push_back(newInst);
    }

    //! Pre-execution: Append a RegisterReference to the Calculator
    //!
    //! Must occur only before a call to exec() or bind()
    void appendRegRef(RegisterReference* newRef)
    {
        assert(mIsUsingAssembler ? mIsAssembling : true);
        assert(newRef->setIndex() < RegisterReference::ELastSet);

        // do not allow more RegisterReferences after bind()
        assert(!mRegisterTuple[RegisterReference::ELiteral]);
        assert(!mRegisterTuple[RegisterReference::EInput]);
        assert(!mRegisterTuple[RegisterReference::EOutput]);
        assert(!mRegisterTuple[RegisterReference::ELocal]);
        assert(!mRegisterTuple[RegisterReference::EStatus]);

        mRegisterRef[newRef->setIndex()].push_back(newRef);
        newRef->setCalc(this);
    }
    
    //! Pre-execution: Given a serialized program, populate Calculator
    //!
    //! Given a serialized program, create register sets, set up literals
    //! and prepare instructions
    void assemble(const char *program);

    //! Pre-execution: Bind Tuples to Register Sets when XO is
    //! populating Calculator
    //!
    //! Allows for the initial bind of externally allocated register
    //! memory tuples. Used only when tuples are allocated by XO, not
    //! by Assembler
    void bind(RegisterReference::ERegisterSet regset,
              TupleData* data, 
              const TupleDescriptor& desc);
  
    //! Determine Output Tuple format
    //!
    //! When assemble() is used, an XO learns the format of its
    //! output from Calculator. Provides a copy of the internally
    //! held output TupleDescriptor. Should only be called after
    //! assemble(). Typically called before exec().
    TupleDescriptor getOutputRegisterDescriptor() const;

    //! Determine Input Tuple format
    //!
    //! When assemble() is used, an XO <b>may</b> learn the format of its
    //! input from Calculator. The XO could use this information to
    //! double-check the integrity of the TupleDescriptor via 
    //! asserts. Typically called before exec().
    TupleDescriptor getInputRegisterDescriptor() const;

    //! Determine Status Tuple format
    //!
    //! When assemble() is used, an XO <b>may</b> learn the format of its
    //! status from Calculator. The XO could use this information to
    //! double-check the integrity of other plan information.
    //! Typically called before exec().
    TupleDescriptor getStatusRegisterDescriptor() const;


    //! Get a pointer to Status Register Tuple
    //!
    //! Typically called once after Calculator configuration, as
    //! this tuple never changes.
    TupleData const * const getStatusRegister() const;
    

    //! Bind commonly changing Register Sets Input and Output.
    //!
    //! Binding or rebinding of varying externally allocated 
    //! register memory tuples. This is the common case call to
    //! bind, where input and output tuples are rebound between
    //! exec() calls. Typically called to advance to the next row.
    void bind(TupleData* input,
              TupleData* output)
    {
        mRegisterTuple[RegisterReference::EInput] = input;
        mRegisterTuple[RegisterReference::EOutput] = output;
    }

    //! Configure Calculator to either exit immediately upon
    //! exceptions or to continue execution.
    void continueOnException(bool c);

    //
    // Execution
    //

    //! Execute program
    void exec();

    //
    // Post Execution Information
    //

    //! Return a formatting string containing all warnings generated
    //! during exec()
    //!
    //! String contains one warning per line, and includes PC and warning code.
    //! Cleared by each call to exec().
    string warnings();

    //! A deque of warnings encapsulated by CalcMessage object
    //!
    //! Deque is cleared by each call to exec().
    deque<CalcMessage> mWarnings;

protected:
    // Note the exact syntax to declare all versions of templated class as friends
    template <typename TMPLT> friend class RegisterRef;
    friend class RegisterReference;
    friend class CalcAssembler;

    //! Program instructions
    vector<Instruction *> mCode;

    //! Tuples that underlie registers, indexed by register set
    //!
    //! Note: Referenced in class RegisterReference and CalcAssembler
    TupleData* mRegisterTuple[RegisterReference::ELastSet];

    //! All active registers, indexed by register set
    //!
    //! Note: Referenced in class CalcAssembler
    vector<RegisterReference *> mRegisterRef[RegisterReference::ELastSet]; 

    //! A list of registers to be reset by next call to exec()
    //!
    //! Note: mRegisterReset is appended to in class RegisterReference
    vector<RegisterReference *> mRegisterReset;

    //! A TupleDescriptor for each Register Set (i.e. each register tuple)
    //!
    //! Used by XO to determine tuple layouts after a serialized
    //! program is assembled. Also needed for Pointer types to
    //! determine column width.
    //! Populated by the CalcAssembler.
    TupleDescriptor* mRegisterSetDescriptor[RegisterReference::ELastSet];

    //! Assembler was/will-be used
    const bool mIsUsingAssembler;
    //! Assembler is actively assembling
    bool mIsAssembling;
    //! Output register does not have memory associated with it, is
    //! passed in a don't-care state, and output should refer to
    //! other register sets by reference. Typical XO execution mode.
    //! Must be set before appending instructions.
    bool mOutputRegisterByReference;
    //! Exceptions cause calculator to return immediately, or do they
    //! allow execution to conitnue?
    bool mContinueOnException;
    
    //! Actual storage used by the CalcAssembler for the literal, local
    //! and status registers
    vector<FixedBuffer*> mBuffers;
private:
    //! Helper function for constructors.
    void init(int codeSize, int literalSize, int inputSize,
              int outputSize, int localSize, int statusSize);
};

FENNEL_END_NAMESPACE

#endif

// End Calculator.h

