/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2004-2009 SQLstream, Inc.
// Copyright (C) 2009-2009 LucidEra, Inc.
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

#include "fennel/calculator/CalcTypedefs.h"
#include "fennel/calculator/CalcMessage.h"
#include "fennel/calculator/RegisterReference.h"
#include "fennel/exec/DynamicParam.h"
#include "fennel/common/TraceSource.h"

FENNEL_BEGIN_NAMESPACE

using namespace std;


class Calculator : virtual public TraceSource
{
public:
    //! Constructor for XOs that will use assemble().
    //! @param dynamicParamManager the DynamicParamManager to use
    //!        from calculator program. May be NULL if program
    //!        doesn't make use of it.
    explicit
    Calculator(DynamicParamManager* dynamicParamManager);

    //! Constructor for XOs that will populate Calculator piecemeal.
    //!
    //! Cannot use the assembler with this format.
    //! @param dynamicParamManager the dynamic parameter manager
    //! @param codeSize size of Instruction vector
    //! @param literalSize size of literal RegisterReference vector
    //! @param inputSize size of literal RegisterReference vector
    //! @param outputSize size of literal RegisterReference vector
    //! @param localSize size of literal RegisterReference vector
    //! @param statusSize size of literal RegisterReference vector
    explicit
    Calculator(
        DynamicParamManager* dynamicParamManager,
        int codeSize, int literalSize, int inputSize,
        int outputSize, int localSize, int statusSize);

    ~Calculator();

    //! Gets this calculator instance's DynamicParamManager
    inline DynamicParamManager* getDynamicParamManager() const {
        return mPDynamicParamManager;
    }

    //
    // Pre-Execution Configuration
    //

    //! Pre-execution: Configure output register to be set by reference
    //! (default), or by value.
    //!
    //! <p>
    //! Default: True.
    //!
    //! <p>
    //! If flag is true, output register can only be set by reference
    //! using the reference "REF" instructions, PointerRef and
    //! NativeRef. Copy by value into the output register is disallowed,
    //! as the registers may point to another read-only register set.
    //! Reading from the output register in this mode is possible,
    //! but should only occur after the appropriate REF instruction.
    //! Output register may be passed to the Calculator in a don't
    //! care state.
    //!
    //! <p>
    //! If flag is false, output register is assumed to point to
    //! appropriately allocated memory and is set using copy by value
    //! instructions.
    //!
    //! <p>
    //! Must be set before appending instructions.
    void outputRegisterByReference(bool flag);

    //! Pre-execution: Appends an Instruction to the Calculator
    void appendInstruction(Instruction* newInst)
    {
        assert(mIsUsingAssembler ? mIsAssembling : true);
        mCode.push_back(newInst);
    }

    //! Pre-execution: Appends a RegisterReference to the Calculator
    //!
    //! Must occur only before a call to exec() or bind()
    void appendRegRef(RegisterReference* newRef)
    {
        assert(mIsUsingAssembler ? mIsAssembling : true);
        assert(newRef->setIndex() < RegisterReference::ELastSet);

        // do not allow more RegisterReferences after bind()
        assert(!mRegisterSetBinding[RegisterReference::ELiteral]);
        assert(!mRegisterSetBinding[RegisterReference::EInput]);
        assert(!mRegisterSetBinding[RegisterReference::EOutput]);
        assert(!mRegisterSetBinding[RegisterReference::ELocal]);
        assert(!mRegisterSetBinding[RegisterReference::EStatus]);

        mRegisterRef[newRef->setIndex()].push_back(newRef);
        newRef->setCalc(this);
    }

    //! Pre-execution: Given a serialized program, populates Calculator
    //!
    //! Given a serialized program, creates register sets, sets up literals
    //! and prepares instructions
    void assemble(const char *program);

    //! Pre-execution: Binds Tuples to Register Sets when XO is
    //! populating Calculator
    //!
    //! Allows for the initial bind of externally allocated register
    //! memory tuples. Used only when tuples are allocated by XO, not
    //! by Assembler
    void bind(
        RegisterReference::ERegisterSet regset,
        TupleData* data,
        const TupleDescriptor& desc);

    //! Determines Output Tuple format
    //!
    //! When assemble() is used, an XO learns the format of its
    //! output from Calculator. Provides a copy of the internally
    //! held output TupleDescriptor. Should only be called after
    //! assemble(). Typically called before exec().
    TupleDescriptor getOutputRegisterDescriptor() const;

    //! Determines Input Tuple format
    //!
    //! When assemble() is used, an XO <b>may</b> learn the format of its
    //! input from Calculator. The XO could use this information to
    //! double-check the integrity of the TupleDescriptor via
    //! asserts. Typically called before exec().
    TupleDescriptor getInputRegisterDescriptor() const;

    //! Determines Status Tuple format
    //!
    //! When assemble() is used, an XO <b>may</b> learn the format of its
    //! status from Calculator. The XO could use this information to
    //! double-check the integrity of other plan information.
    //! Typically called before exec().
    TupleDescriptor getStatusRegisterDescriptor() const;


    //! Gets a pointer to Status Register Tuple
    //!
    //! Typically called once after Calculator configuration, as
    //! this tuple never changes.
    TupleData const * const getStatusRegister() const;

    //! Zeroes out the values of all TupleDatum within the Staus Register
    //! Tuple.
    //!
    //! Typically this is called before the first call to exec() and should
    //! never be called between calls to exec() unless execution is being
    //! restarted at the beginning of a series of tuples.
    void zeroStatusRegister();


    //! Binds the commonly changing Register Sets Input and Output.
    //!
    //! Binding or rebinding of varying externally allocated
    //! register memory tuples. This is the common case call to
    //! bind, where input and output tuples are rebound between
    //! exec() calls. Typically called to advance to the next row.
    //!
    //! @param input  bind the input registers to this tuple
    //! @param output bind the output registers to this tuple

    //! @param outputWrite (optional, use when \c output contains null values).
    //!  Equivalent to \c output, except it has the allocated target
    //!  address of each datum which is null in \c output.
    //! @param takeOwnership When true, the Calculator owns these TupleData, and
    //!   will delete them in its destructor.
    void bind(
        TupleData* input,
        TupleData* output,
        bool takeOwnership = false,
        const TupleData* outputWrite = 0);

    //! Configures Calculator to either exit immediately upon
    //! exceptions or to continue execution.
    void continueOnException(bool c);

    //
    // Execution
    //

    //! Executes program
    void exec();

    //
    // Post Execution Information
    //

    //! Returns a formatting string containing all warnings generated
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
    // Note the exact syntax to declare all versions of templated
    // class as friends
    template <typename TMPLT> friend class RegisterRef;
    friend class RegisterReference;
    friend class CalcAssembler;

    //! Program instructions
    vector<Instruction *> mCode;

    //! How registers are bound to underlying tuples, indexed by register set
    //! (null element means the register set is not bound)
    //! Note: Referenced in class RegisterReference and CalcAssembler
    RegisterSetBinding* mRegisterSetBinding[RegisterReference::ELastSet];

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
    //! If set, output register can only be set by reference.
    bool mOutputRegisterByReference;
    //! Exceptions cause calculator to return immediately, or do they
    //! allow execution to conitnue?
    bool mContinueOnException;

    //! Actual storage used by the CalcAssembler for the literal, local
    //! and status registers
    vector<FixedBuffer*> mBuffers;

    //! Reference to the Dynamic Parameter Manager
    DynamicParamManager* mPDynamicParamManager;

private:
    //! Helper function for constructors.
    void init(
        int codeSize,
        int literalSize,
        int inputSize,
        int outputSize,
        int localSize,
        int statusSize);

    //! Free up memory from bind.
    void unbind(
        RegisterReference::ERegisterSet regset,
        bool unbindDescriptor = true);
};

FENNEL_END_NAMESPACE

#endif

// End Calculator.h

