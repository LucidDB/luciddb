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

#ifndef Fennel_CalcAssembler_Included
#define Fennel_CalcAssembler_Included

#include "fennel/calc/CalcLexer.h"
#include "fennel/calc/CalcAssemblerException.h"
#include "fennel/calc/InstructionFactory.h"

#include <strstream>

FENNEL_BEGIN_NAMESPACE

using namespace std;

typedef size_t TRegisterIndex;

class Calculator;
class RegisterReference;
class TupleDescriptor;
class StoredTypeDescriptorFactory;

/**
 * The CalcAssembler is responsible for taking a textual representation
 * of a calculator program and forming a calculator. The CalcAssembler
 * will do the following:
 * <ol>
 *    <li>Create TupleDescriptors to describe all registers</li>
 *    <li>Create Literal, Local, and Status Tuples and bind
 *        them to the Calculator register sets</li> 
 *    <li>Initializes literal values in the Literal Register Set</li>
 *    <li>Create and insert instructions into the Calculator</li>
 *    <li>Verifies the format of serialized programs</li>
 *    <li>Performs type checking</li>
 * </ol>
 * The CalcAssembler is a temporal object that is used by the Calculator,
 * with the Calculator responsible for the deallocation of any objects
 * that are allocated by the CalcAssembler.
 */
class CalcAssembler
{
public:
    explicit
    CalcAssembler(Calculator* calc) { assert(calc != NULL); mCalc = calc; }
    ~CalcAssembler();

    CalcLexer&  getLexer() { return mLexer; }

    int   assemble(const char* program);
    int   assemble();

    // Functions for creating objects for the calculator
    static TupleData*         createTupleData(TupleDescriptor const& tupleDes, FixedBuffer** buf);
    static RegisterReference* createRegisterReference(RegisterReference::ERegisterSet setIndex,
                                                      TRegisterIndex                  regIndex,
                                                      StandardTypeDescriptorOrdinal   regType);

    static Instruction* createInstruction(string& name,
                                          RegisterReference* result,
                                          RegisterReference* operand1,
                                          RegisterReference* operand2,
                                          CalcYYLocType& location )
    {
        Instruction* inst = NULL;
        try {
            inst = InstructionFactory::createInstruction(name, result, operand1, operand2);
        }
        catch (FennelExcn& ex) {
            throw CalcAssemblerException(ex.getMessage(), location);
        }   
        catch (std::exception& ex) {
            throw CalcAssemblerException(ex.what(), location);
        }   
        if (inst == NULL) 
            throw CalcAssemblerException("Error instantiating instruction: " + name + " "
                                         + result->toString() + ", " + operand1->toString() + ", "
                                         + operand2->toString(), location);

        return inst;
    }


    static Instruction* createInstruction(string& name,
                                          RegisterReference* result,
                                          RegisterReference* operand1,
                                          CalcYYLocType& location )
    {
        Instruction* inst = NULL;
        try {
            inst = InstructionFactory::createInstruction(name, result, operand1);
        }
        catch (FennelExcn& ex) {
            throw CalcAssemblerException(ex.getMessage(), location);
        }   
        catch (std::exception& ex) {
            throw CalcAssemblerException(ex.what(), location);
        }   
        if (inst == NULL) 
            throw CalcAssemblerException("Error instantiating instruction: " + name + " "
                                         + result->toString() + ", " + operand1->toString(),
                                         location);

        return inst;
    }

    static Instruction* createInstruction(string& name,
                                          RegisterReference* result,
                                          CalcYYLocType& location)
    {
        Instruction* inst = NULL;
        try {
            inst = InstructionFactory::createInstruction(name, result);
        }
        catch (FennelExcn& ex) {
            throw CalcAssemblerException(ex.getMessage(), location);
        }   
        catch (std::exception& ex) {
            throw CalcAssemblerException(ex.what(), location);
        }   
        if (inst == NULL) 
            throw CalcAssemblerException("Error instantiating instruction: " + name + " "
                                         + result->toString(), location);

        return inst;
    }

    static Instruction* createInstruction(string& name,
                                          CalcYYLocType& location)
    {
        Instruction* inst = NULL;
        try {
            inst = InstructionFactory::createInstruction(name);
        }
        catch (FennelExcn& ex) {
            throw CalcAssemblerException(ex.getMessage(), location);
        }   
        catch (std::exception& ex) {
            throw CalcAssemblerException(ex.what(), location);
        }   
        if (inst == NULL) 
            throw CalcAssemblerException("Error instantiating instruction: " + name,
                                          location);

        return inst;
    }

    static Instruction* createInstruction(string& name,
                                          TProgramCounter pc,
                                          CalcYYLocType& location)
    {
        Instruction* inst = NULL;
        try {
            inst = InstructionFactory::createInstruction(name, pc);
        }
        catch (FennelExcn& ex) {
            throw CalcAssemblerException(ex.getMessage(), location);
        }   
        catch (std::exception& ex) {
            throw CalcAssemblerException(ex.what(), location);
        }   

        if (inst == NULL) {
            stringstream errorStr("Error instantiating instruction: ");
            errorStr << name << " " << pc;
            throw CalcAssemblerException(errorStr.str(), location);
        }

        return inst;
    }


    static Instruction* createInstruction(string& name,
                                          TProgramCounter pc,
                                          RegisterReference* operand,
                                          CalcYYLocType& location)
    {
        Instruction* inst = NULL;

        try {
            inst = InstructionFactory::createInstruction(name, pc, operand);
        }
        catch (FennelExcn& ex) {
            throw CalcAssemblerException(ex.getMessage(), location);
        }   
        catch (std::exception& ex) {
            throw CalcAssemblerException(ex.what(), location);
        }   

        if (inst == NULL) {
            stringstream errorStr("Error instantiating instruction: ");
            errorStr << name << " " << pc << ", " << operand->toString();
            throw CalcAssemblerException(errorStr.str(), location);
        }

        return inst;
    }

    Instruction* createInstruction(string& name,
                                   string& function,
                                   vector<RegisterReference*>& operands,
                                   CalcYYLocType& location)
    {
        Instruction* inst = NULL;

        try {
            inst = InstructionFactory::createInstruction(mCalc, name, function, operands);
        }
        catch (FennelExcn& ex) {
            throw CalcAssemblerException(ex.getMessage(), location);
        }   
        catch (std::exception& ex) {
            throw CalcAssemblerException(ex.what(), location);
        }   

        if (inst == NULL) {
            stringstream errorStr("Error instantiating instruction: ");
            errorStr << name << " " << InstructionFactory::computeSignature(function, operands)
                     << " not registered ";
            throw CalcAssemblerException(errorStr.str(), location);
        }

        return inst;
    }

    static void CalcAssembler::setTupleDatum(StandardTypeDescriptorOrdinal type,
                                             TupleDatum& tupleDatum,  
                                             TupleAttributeDescriptor& desc,
                                             double value);
    static void CalcAssembler::setTupleDatum(StandardTypeDescriptorOrdinal type,
                                             TupleDatum& tupleDatum,  
                                             TupleAttributeDescriptor& desc,
                                             uint64_t value);
    static void CalcAssembler::setTupleDatum(StandardTypeDescriptorOrdinal type,
                                             TupleDatum& tupleDatum,  
                                             TupleAttributeDescriptor& desc,
                                             int64_t value);
    static void CalcAssembler::setTupleDatum(StandardTypeDescriptorOrdinal type,
                                             TupleDatum& tupleDatum,  
                                             TupleAttributeDescriptor& desc,
                                             string value);

protected:
    friend int CalcYYparse (void *);

    // Protected functions that are used by CalcYYparse

    //! Initializes the assembler
    void  init();

    /**
     * Allocate memory (create TupleData structure) for the literal, local, and
     * status registers.
     * CalcYYparse should call this function after parsing the register
     * definitions and before parsing the literal values.
     * @note This function must be called before attempting to initialize the
     *       literal values.
     */
    void allocateTuples();

    /**
     * Bind registers to the calculator
     * CalcYYparse should call this function after parsing the register 
     * definitions and literal values to bind the TupleData to the calculator
     * registers.
     * @note This function must be called after all tuples have been allocated
     *       and literal values initialized
     */
    void bindRegisters();

    /**
     * Binds the templated value to the specified register.
     * A CalcAssemblerException with the location of the current token is
     * thrown if there is an error binding the value to the register.
     * @param setIndex Register set index
     * @param regIndex Register index
     * @param value Value to bind to the register
     * @throw CalcAssemblerException
     * @note The assembler is only responsible for binding literal values.
     *       This function is used by bindNextLiteral for binding literal values.
     *       There should be no reason to use this function directly.
     * @see  bindNextLiteral
     */
    template <typename T>
    void bindRegisterValue(RegisterReference::ERegisterSet setIndex, 
                           TRegisterIndex regIndex, T value)
    {
        try {
            StandardTypeDescriptorOrdinal regType = getRegisterType(setIndex, regIndex);
            TupleData* tupleData = getTupleData(setIndex);
            assert(tupleData != NULL);
            TupleDatum& tupleDatum = (*tupleData)[regIndex];
            TupleDescriptor& tupleDesc = getTupleDescriptor(setIndex);
            setTupleDatum(regType, tupleDatum, tupleDesc[regIndex], value);
        }
        catch (CalcAssemblerException& ex) {
            // Just pass this exception up
            throw ex;
        }
        catch (FennelExcn& ex) {
            // Other exception - let's make the error clearer
            ostringstream errorStr("");
            errorStr << "Error binding register "
                     << RegisterReference::toString(setIndex, regIndex) 
                     << ": " << ex.getMessage();
            throw CalcAssemblerException(errorStr.str(), getLexer().getLocation());
        }
        catch (exception& ex) {
            // Other exception - let's make the error clearer
            ostringstream errorStr("");
            errorStr << "Error binding register: " 
                     << RegisterReference::toString(setIndex, regIndex) 
                     << ": " << ex.what();
            throw CalcAssemblerException(errorStr.str(), getLexer().getLocation());
        }
    }

    /**
     * Binds the templated value to the next literal register. 
     * A CalcAssemblerException with the location of the current token is 
     * thrown if there is an error binding the value to the register.
     * @param value Value to bind to the register
     * @throw CalcAssemblerException
     * @note  CalcYYparse calls this function to bind individual literal value 
     *        as they are parsed one by one.  Before calling this function, 
     *        bindRegisters() should be called to allocate memory for the
     *        TupleData and bind them to the registers. After all literal 
     *        values are bound, bindLiteralsDone() should be called to check 
     *        that all literal registers have been initialized.
     * @see   bindLiteralsDone()
     * @see   bindRegisters()
     */
    template <typename T>
    void bindNextLiteral(T value)
    {
        bindRegisterValue<T>(RegisterReference::ELiteral, mLiteralIndex, value);
        mLiteralIndex++;
    }

    /**
     * Finishes binding literal values to registers.  This function should be 
     * called by CalcYYparse after all parsing literal values to check that 
     * all literal registers have been initialized. 
     */
    void bindLiteralDone();
    
    // CalcYYparse uses the two functions below to add a register 
    
    /**
     * Sets the current register set.  
     * @param setIndex Register set index
     */
    void selectRegisterSet(RegisterReference::ERegisterSet setIndex);

    /**
     * Add a register to the current register set
     * @param regType type of the register to add
     * @param cbStorage space to allocate for the register (used for arrays)
     * @note Use selectRegisterSet to set the current register set
     */
    void addRegister(StandardTypeDescriptorOrdinal regType, uint cbStorage = 0);
    /**
     * Add a register to the specified register set
     * @param setIndex Register set index
     * @param regType type of the register to add
     * @param cbStorage space to allocate for the register (used for arrays)
     */
    void addRegister(RegisterReference::ERegisterSet setIndex,
                     StandardTypeDescriptorOrdinal regType, uint cbStorage = 0);


    // CalcYYparse uses this function to add a instruction

    /**
     * Add a instruction to the calulator
     * @param inst Instruction to add
     */
    void addInstruction(Instruction* inst);

    // Functions used by CalcYYparse to access registers and tuples

    /**
     * Returns the number of registers in a register set
     * @param setIndex Register set index
     * @return Number of registers in the register set
     */
    TRegisterIndex getRegisterSize(RegisterReference::ERegisterSet setIndex);

    /**
     * Returns a specified RegisterReference
     * @param  setIndex Register set index
     * @param  regIndex Register index
     * @return RegisterReference pointer
     * @throw  FennelExcn Exception indicating the register index is out of bounds
     */
    RegisterReference* getRegister(RegisterReference::ERegisterSet setIndex, 
                                   TRegisterIndex regIndex);

    /**
     * Returns the type of register given the set index and the register index
     * @param  setIndex Register set index
     * @param  regIndex Register index
     * @return Type of the register
     * @throw  FennelExcn Exception indicating the register index is out of bounds
     */
    StandardTypeDescriptorOrdinal  getRegisterType(RegisterReference::ERegisterSet setIndex, 
                                                   TRegisterIndex regIndex);

    /**
     * Returns a pointer to the TupleData for a register set.
     * @param  setIndex Register set index
     * @return Pointer to the TupleData
     */
    TupleData* getTupleData(RegisterReference::ERegisterSet setIndex);

    /**
     * Returns the TupleDescriptoro for a register set.
     * @param  setIndex Register set index
     * @return TupleDescriptor
     */
    TupleDescriptor& getTupleDescriptor(RegisterReference::ERegisterSet setIndex);

    /**
     * Verifies that the program counter is valid
     * @param pc the program counter
     * @throw CalcAssemblerException Exception indicating that the PC is invalid 
     */ 
    void checkPC(TProgramCounter pc, CalcYYLocType& loc) {
        assert(mCalc->mCode.size() > 0);
        if (pc >= mCalc->mCode.size()) {
            ostringstream errorStr("");
            errorStr << "Invalid PC " << pc << ": PC should be between 0 and "
                     << (mCalc->mCode.size() - 1);
            throw CalcAssemblerException(errorStr.str(), loc);
        }
    }

    /**
     * Saves the maximum PC.  It is impossible to check the PC until we have 
     * assembled the entire program and it is too much trouble to keep track of
     * every PC to see if they are valid or not.  Instead, we will just keep
     * track of the maximum PC, and if it is valid, then all PCs should be 
     * valid.  If it is not, then we will just report on that PC.
     */
    void saveMaxPC(TProgramCounter pc) 
    {
        assert(pc > 0);
        if (pc > mMaxPC) 
        {
            mMaxPC = pc;
            mMaxPCLoc = getLexer().getLocation();
        }
    }

protected:
    CalcLexer   mLexer; //!< Lexer for the assembler
    Calculator* mCalc;  //!< Calculator to be assembled

    //! TupleDescriptors for the register sets
    TupleDescriptor mRegisterSetDescriptor[RegisterReference::ELastSet];

    //! Pointers to the tuple data
    //! Once they have been bound to the calculator, it is the calculator's
    //! responsibility to destroty the tuple data and the buffers.
    TupleData* mRegisterTupleData[RegisterReference::ELastSet];
    //! Actual storage used by the CalcAssembler for the literal, local
    //! and status registers
    FixedBuffer* mBuffers[RegisterReference::ELastSet];

    //! Factory used to create TupleAttributeDescriptor
    StandardTypeDescriptorFactory   mTypeFactory;

    //! Register set that the assembler is currently parsing
    RegisterReference::ERegisterSet mCurrentRegSet;

    //! Index of the next literal register to bind
    TRegisterIndex                  mLiteralIndex;

    //! Saved information about the maximum PC
    TProgramCounter mMaxPC;
    CalcYYLocType   mMaxPCLoc;
};

FENNEL_END_NAMESPACE

#endif

// End CalcAssembler.h
