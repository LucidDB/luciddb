/*
// $Id$
// Fennel is a relational database kernel.
// (C) Copyright 2004-2004 Disruptive Tech
//
// This library is free software; you can redistribute it and/or
// modify it under the terms of the GNU Library General Public
// License as published by the Free Software Foundation; either
// version 2 of the License, or (at your option) any later version.
//
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// Library General Public License for more details.
//
// You should have received a copy of the GNU Library General Public
// License along with this library; if not, write to the
// Free Software Foundation, Inc., 59 Temple Place - Suite 330,
// Boston, MA  02111-1307, USA.
//
// See the LICENSE.html file located in the top-level-directory of
// the archive of this library for complete text of license.
//
// jhyde 17 January, 2004
*/
#ifndef Fennel_ExtendedInstruction_Included
#define Fennel_ExtendedInstruction_Included

#include "fennel/calc/Instruction.h"
#include "map"

FENNEL_BEGIN_NAMESPACE

using std::string;
class Register;
class ExtendedInstruction;


/**
 * Describes an extended instruction.
 *
 * Created by ExtendedInstructionTable.
 */
class ExtendedInstructionDef
{
protected:
    string _name;
    vector<StandardTypeDescriptorOrdinal> _parameterTypes;
    string _signature;

    friend class ExtendedInstructionTable;

    /**
     * Only an ExtendedInstructionTable can create an ExtendedInstructionDef.
     */
    explicit
    ExtendedInstructionDef(
            string const &name,
            const vector<StandardTypeDescriptorOrdinal> &parameterTypes) :
        _name(name),
        _parameterTypes(parameterTypes),
        _signature(computeSignature()) {
    }
public:
    virtual
    ~ExtendedInstructionDef() {}

    /**
     * Returns the name of this instruction.
     */
    string getName() { return _name; }
    /**
     * Returns the parameter types of this instruction.
     */
    const vector<StandardTypeDescriptorOrdinal> &getParameterTypes() { return _parameterTypes; }
    /**
     * Returns the signature of this instruction. The signature is always of
     * the form "<name>(<type0>,<type1>,...)", for example
     * "substr(ptr,ptr,int,intr)". The signature is used to identify
     * instructions in an ExtendedInstructionTable, and in assembly language
     * instructions such as "CALL 'substr(ptr,ptr,int,ptr) &0, &1, &2, &3".
     */
    string getSignature() const { return _signature; }
    /**
     * Creates an instruction of this type which references a particular set of
     * registers. The registers supplied must be the same number and type as
     * the registers supported by the function.
     */
    virtual ExtendedInstruction *createInstruction(
            Calculator *pCalc,
            vector<RegisterReference *> regs) = 0;

private:
    string computeSignature();
};

/**
 * Definition of an extended instruction based upon a functor.
 *
 * Template parameter 'T' must be a type such as
 * ExtendedInstruction2<int,double>. It must have a type T:Functor.
 */
template <typename T>
class FunctorExtendedInstructionDef : public ExtendedInstructionDef
{
    friend class ExtendedInstructionTable;

protected:
    /**
     * Creates a FunctorExtendedInstructionDef. Only ExtendedInstructionTable
     * calls this.
     */
    explicit
    FunctorExtendedInstructionDef(
            const string &name,
            const vector<StandardTypeDescriptorOrdinal> &parameterTypes,
            typename T::Functor functor) :
        ExtendedInstructionDef(name, parameterTypes),
        _functor(functor) {
            assert(functor != NULL);
        }
public:
    typename T::Functor _functor;

    // implement ExtendedInstructionDef
    ExtendedInstruction *createInstruction(
            Calculator *pCalc,
            vector<RegisterReference *> regs) {
        return T::create(*this,pCalc,regs);
    }
};


/**
 * List of extended instructions.
 */
class ExtendedInstructionTable
{
private:
    map<string,ExtendedInstructionDef *> _defsByName;

public:
    /**
     * Registers an extended instruction and the functor which implements it.
     */
    template <typename T>
    void add(
            const string &name,
            const vector<StandardTypeDescriptorOrdinal> &parameterTypes,
            T *dummy,
            typename T::Functor functor)
    {
         FunctorExtendedInstructionDef<T> *pDef = 
             new FunctorExtendedInstructionDef<T>(name, parameterTypes,
                                                  functor);
         _defsByName[pDef->getSignature()] = pDef;
    }

    /**
     * Looks for an extended instruction by signature (name + argument
     * types). Returns null if there is no such instruction.
     */
    ExtendedInstructionDef *lookupBySignature(string const &signature);
};

/**
 * Base class for all extended instructions. Derived classes are typically
 * (though not necessarily) templates such as ExtendedInstruction1.
 */
class ExtendedInstruction : public Instruction
{
protected:
    Calculator *_pCalc;
    virtual ExtendedInstructionDef const &getDef() const = 0;

    template <typename T>
    static void describeArg(string &out, bool values, 
                            RegisterRef<T> * const reg) {
        out += reg->toString();
        if (values) {
            out += " ( ";
            if (reg->isNull()) {
                out += "NULL";
            } else {
                out += reg->valueToString();
            }
            out += " ) ";
        }
    }

public:
    explicit
    ExtendedInstruction(Calculator *pCalc) : _pCalc(pCalc) {
    }
    // implement Instruction
    const char *longName() const { 
        return const_cast<ExtendedInstructionDef &>(getDef()).getName().c_str(); 
    }
    const char *shortName() const { 
        return const_cast<ExtendedInstructionDef &>(getDef()).getName().c_str();
    }
};

/**
 * Extended instruction which takes 0 parameters and is implemented using a
 * functor.
 */
class ExtendedInstruction0 : public ExtendedInstruction
{
public:
    /**
     * The type of functor used to implement this extended instruction. For
     * example, a function such as<blockquote>
     *
     * <pre>void execute(
     *     Calc *pCalc)</pre>
     *
     * </blockquote>
     */
    typedef void (*Functor)(
            Calculator *pCalc);
    /**
     * The specific type of the definition of this instruction.
     */
    typedef FunctorExtendedInstructionDef<ExtendedInstruction0> DefT;

private:
    DefT _def;

protected:
    // implement ExtendedInstruction
    ExtendedInstructionDef const &getDef() const {
        return static_cast<ExtendedInstructionDef const &>(_def);
    }
    void setCalc(Calculator* calcP) {
        // cout << "null setCalc registers call" << endl;
    }

public:
    explicit
    ExtendedInstruction0(DefT &def, Calculator *pCalc)
        : ExtendedInstruction(pCalc),
          _def(def) {
    }

    static ExtendedInstruction0 *create(
            DefT &def,
            Calculator *pCalc,
            vector<RegisterReference *> regRefs) {
        assert(regRefs.size() == 0);
        return new ExtendedInstruction0(
                def, pCalc);
    }

    // implement Instruction
    void describe(string &out, bool values) const {
        out = "CALL '";
        out += _def.getSignature();
        out += "'";
    }
    void exec(TProgramCounter &pc) const {
        (*_def._functor)(_pCalc);
        ++pc;
    }
};

/**
 * Extended instruction which takes 1 parameter and is implemented using a
 * functor.
 */
template <typename T0>
class ExtendedInstruction1 : public ExtendedInstruction
{
public:
    /**
     * The type of functor used to implement this extended instruction.
     * For example, if T0 is 'int', then we will require a function such
     * as<blockquote>
     *
     * <pre>void execute(
     *     Calc *pCalc,
     *     RegisterRef<int>* reg0</pre>
     *
     * </blockquote>
     */
    typedef void (*Functor)(
            Calculator *pCalc,
            RegisterRef<T0>* reg0);
    /**
     * The specific type of the definition of this instruction.
     */
    typedef FunctorExtendedInstructionDef<ExtendedInstruction1<T0> > DefT;

private:
    DefT _def;
    RegisterRef<T0>* _reg0;

protected:
    // implement ExtendedInstruction
    ExtendedInstructionDef const &getDef() {
        return _def;
    }
    void setCalc(Calculator* calcP) {
        // cout << "1 setCalc registers call" << endl;
      _reg0->setCalc(calcP);
    }

public:
    explicit
    ExtendedInstruction1(
            DefT &def,
            Calculator *pCalc,
            RegisterRef<T0>* const reg0)
        : ExtendedInstruction(pCalc),
          _def(def),
          _reg0(reg0) {
    }

    static ExtendedInstruction1<T0> create(
            DefT &def,
            Calculator *pCalc,
            vector<RegisterReference *> regRefs) {
        assert(regRefs.size() == 1);
// todo:        assert(regRefs[0].getType() == T0::typeCode);
        return new ExtendedInstruction1<T0>(
                def,
                pCalc,
                dynamic_cast<RegisterRef<T0> *>(regRefs[0]));
    }

    // implement Instruction
    void describe(string &out, bool values) const {
        out = "CALL '";
        out += _def.getSignature();
        out += "' ";
        describeArg(out, values, _reg0);
    }
    void exec(TProgramCounter &pc) const {
        (*_def._functor)(_reg0);
        ++pc;
    }
};

/**
 * Extended instruction which takes 2 parameters and is implemented using a
 * functor.
 */
template <typename T0, typename T1>
class ExtendedInstruction2 : public ExtendedInstruction
{
public:
    /**
     * The type of functor used to implement this extended instruction. For
     * example, if T0 is 'int' and T1 is 'double',
     * then we will require a function such as<blockquote>
     *
     * <pre>void execute(
     *     Calc *pCalc,
     *     RegisterRef<int>* reg0,
     *     RegisterRef<double>* reg1)</pre>
     *
     * </blockquote>
     */
    typedef void (*Functor)(
            Calculator *pCalc,
            RegisterRef<T0>* const reg0,
            RegisterRef<T1>* const reg1);
    /**
     * The specific type of the definition of this instruction.
     */
    typedef FunctorExtendedInstructionDef<ExtendedInstruction2<T0,T1> > DefT;

private:
    DefT _def;
    RegisterRef<T0>* _reg0;
    RegisterRef<T1>* _reg1;

protected:
    // implement ExtendedInstruction
    ExtendedInstructionDef const &getDef() const {
        return _def;
    }
    void setCalc(Calculator* calcP) {
      _reg0->setCalc(calcP);
      _reg1->setCalc(calcP);
    }

public:
    explicit
    ExtendedInstruction2(
            DefT &def,
            Calculator *pCalc,
            RegisterRef<T0>* const reg0,
            RegisterRef<T1>* const reg1)
        : ExtendedInstruction(pCalc),
          _def(def),
          _reg0(reg0),
          _reg1(reg1) {
    }

    static ExtendedInstruction2<T0,T1> *create(
            DefT &def,
            Calculator *pCalc,
            vector<RegisterReference *> regRefs) {
        assert(regRefs.size() == 2);
        return new ExtendedInstruction2<T0,T1>(
                def,
                pCalc,
                static_cast<RegisterRef<T0> *>(regRefs[0]),
                static_cast<RegisterRef<T1> *>(regRefs[1]));
    }

    // implement Instruction
    void describe(string &out, bool values) const {
        out = "CALL '";
        out += _def.getSignature();
        out += "' ";
        describeArg(out, values, _reg0);
        out += ", ";
        describeArg(out, values, _reg1);
    }
    void exec(TProgramCounter &pc) const {
        (*_def._functor)(_pCalc,_reg0,_reg1);
        ++pc;
    }
};

/**
 * Extended instruction which takes 3 parameters and is implemented using a
 * functor.
 */
template <typename T0, typename T1, typename T2>
class ExtendedInstruction3 : public ExtendedInstruction
{
public:
    /**
     * The type of functor used to implement this extended instruction. For
     * example, if T0 is 'int' and T1 is 'double' and T2 is 'int'
     * then we will require a function such as<blockquote>
     *
     * <pre>void execute(
     *     Calc *pCalc,
     *     RegisterRef<int> &reg0,
     *     RegisterRef<double> &reg1,
     *     RegisterRef<int> &reg2)</pre>
     *
     * </blockquote>
     */
    typedef void (*Functor)(
            Calculator *pCalc,
            RegisterRef<T0>* const reg0,
            RegisterRef<T1>* const reg1,
            RegisterRef<T2>* const reg2);
    /**
     * The specific type of the definition of this instruction.
     */
    typedef FunctorExtendedInstructionDef<ExtendedInstruction3<T0,T1,T2> > DefT;

private:
    DefT _def;
    RegisterRef<T0>* _reg0;
    RegisterRef<T1>* _reg1;
    RegisterRef<T2>* _reg2;

protected:
    // implement ExtendedInstruction
    ExtendedInstructionDef const &getDef() const {
        return _def;
    }
    void setCalc(Calculator* calcP) {
      _reg0->setCalc(calcP);
      _reg1->setCalc(calcP);
      _reg2->setCalc(calcP);
    }

public:
    explicit
    ExtendedInstruction3(
            DefT &def,
            Calculator *pCalc,
            RegisterRef<T0>* const reg0,
            RegisterRef<T1>* const reg1,
            RegisterRef<T2>* const reg2)
        : ExtendedInstruction(pCalc),
          _def(def),
          _reg0(reg0),
          _reg1(reg1),
          _reg2(reg2){
    }

    static ExtendedInstruction3<T0,T1,T2> *create(
            DefT &def,
            Calculator *pCalc,
            vector<RegisterReference *> regRefs) {
        assert(regRefs.size() == 3);
        return new ExtendedInstruction3<T0,T1,T2>(
                def,
                pCalc,
                static_cast<RegisterRef<T0> *>(regRefs[0]),
                static_cast<RegisterRef<T1> *>(regRefs[1]),
                static_cast<RegisterRef<T2> *>(regRefs[2]));
    }

    // implement Instruction
    void describe(string &out, bool values) const {
        out = "CALL '";
        out += _def.getSignature();
        out += "' ";
        describeArg(out, values, _reg0);
        out += ", ";
        describeArg(out, values, _reg1);
        out += ", ";
        describeArg(out, values, _reg2);
    }
    void exec(TProgramCounter &pc) const {
        (*_def._functor)(_pCalc,_reg0,_reg1,_reg2);
        ++pc;
    }
};

FENNEL_END_NAMESPACE

#endif

// End ExtendedInstruction.h
