/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2004-2005 Disruptive Tech
// Copyright (C) 2005-2005 The Eigenbase Project
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later Eigenbase-approved version.
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

#ifndef Fennel_ExtendedInstruction_Included
#define Fennel_ExtendedInstruction_Included

#include "fennel/disruptivetech/calc/Instruction.h"
#include "fennel/disruptivetech/calc/Calculator.h"
#include "fennel/disruptivetech/calc/ExtendedInstructionContext.h"

#include <boost/scoped_ptr.hpp>

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
    const vector<StandardTypeDescriptorOrdinal> &getParameterTypes() { 
        return _parameterTypes;
    }
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
    virtual ExtendedInstruction *createInstruction(vector<RegisterReference *> regs) = 0;

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
    FunctorExtendedInstructionDef(const string &name,
                                  const vector<StandardTypeDescriptorOrdinal> &parameterTypes,
                                  typename T::Functor functor) :
        ExtendedInstructionDef(name, parameterTypes),
        _functor(functor) {
        assert(functor != NULL);
    }
public:
    typename T::Functor _functor;

    // implement ExtendedInstructionDef
    ExtendedInstruction *createInstruction(vector<RegisterReference *> regs) {
        return T::create(*this,regs);
    }
};



/**
 * Base class for all extended instructions. ExtendedInstructions
 * allow a programmer to extend Calculator with greater ease than
 * adding regular instructions. See ExtendedInstructionHowTo for
 * details.
 * 
 * Derived classes are typically (though not necessarily) templates
 * such as ExtendedInstruction1.
 */
class ExtendedInstruction : public Instruction
{
protected:
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
    ExtendedInstruction() { }
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
 * TODO: ExtendedInstruction0 is untested and may be broken.
 */
class ExtendedInstruction0 : public ExtendedInstruction
{
public:
    /**
     * The type of functor used to implement this extended instruction. For
     * example, a function such as<blockquote>
     *
     * <pre>void execute()</pre>
     *
     * </blockquote>
     */
    typedef void (*Functor)();
    /**
     * The specific type of the definition of this instruction.
     */
    typedef FunctorExtendedInstructionDef<ExtendedInstruction0> DefT;

protected:
    DefT _def;

    // implement ExtendedInstruction
    ExtendedInstructionDef const &getDef() const {
        return static_cast<ExtendedInstructionDef const &>(_def);
    }

public:
    explicit
    ExtendedInstruction0(DefT &def)
        : ExtendedInstruction(),
          _def(def) {
    }

    static ExtendedInstruction0 *create(DefT &def,
                                        vector<RegisterReference *> regRefs) {
        assert(regRefs.size() == 0);
        return new ExtendedInstruction0(def);
    }

    // implement Instruction
    void describe(string& out, bool values) const {
        out = "CALL '";
        out += _def.getSignature();
        out += "'";
    }
    void exec(TProgramCounter &pc) const {
        pc++;
        try {
            (*_def._functor)();
        } catch (char const * str) {
            throw CalcMessage(str, pc - 1);
        }
    }
};

/**
 * Extended instruction which takes 0 parameters and is implemented using a
 * functor.
 * TODO: ExtendedInstruction0Context is untested and may be broken.
 */
class ExtendedInstruction0Context : public ExtendedInstruction
{
public:
    /**
     * The type of functor used to implement this extended instruction. For
     * example, a function such as<blockquote>
     *
     * <pre>void execute()</pre>
     *
     * </blockquote>
     */
    typedef void (*Functor)(boost::scoped_ptr<ExtendedInstructionContext>& ctx);
    /**
     * The specific type of the definition of this instruction.
     */
    typedef FunctorExtendedInstructionDef<ExtendedInstruction0Context> DefT;

protected:
    DefT _def;
    boost::scoped_ptr<ExtendedInstructionContext> _ctx;

    // implement ExtendedInstruction
    ExtendedInstructionDef const &getDef() const {
        return static_cast<ExtendedInstructionDef const &>(_def);
    }

public:
    explicit
    ExtendedInstruction0Context(DefT &def)
        : ExtendedInstruction(),
          _def(def),
          _ctx(0) {
    }

    static ExtendedInstruction0Context *create(DefT &def,
                                               vector<RegisterReference *> regRefs) {
        assert(regRefs.size() == 0);
        return new ExtendedInstruction0Context(def);
    }

    // implement Instruction
    void describe(string& out, bool values) const {
        out = "CALL '";
        out += _def.getSignature();
        out += "'";
    }
    void exec(TProgramCounter &pc) const{
        pc++;
        try {
            // TODO: Remove this const cast. Ugly, but exec is defined as const
            // TODO: in virtual above.
            (*_def._functor)(const_cast<boost::scoped_ptr<ExtendedInstructionContext>& >(_ctx));
            
        } catch (char const * str) {
            throw CalcMessage(str, pc - 1);
        }
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
     * <pre>void execute(RegisterRef<int>* reg0</pre>
     *
     * </blockquote>
     */
    typedef void (*Functor)(RegisterRef<T0>* reg0);
    /**
     * The specific type of the definition of this instruction.
     */
    typedef FunctorExtendedInstructionDef<ExtendedInstruction1<T0> > DefT;

private:
    DefT _def;
    RegisterRef<T0>* _reg0;

protected:
    // implement ExtendedInstruction
    ExtendedInstructionDef const &getDef() const {
        return _def;
    }

public:
    explicit
    ExtendedInstruction1(DefT &def,
                         RegisterRef<T0>* const reg0)
        : _def(def),
          _reg0(reg0) {
    }

    static ExtendedInstruction1<T0> *create(DefT &def,
                                            vector<RegisterReference *> regRefs) {
        assert(regRefs.size() == 1);
        // todo:        assert(regRefs[0].getType() == T0::typeCode);
        return new ExtendedInstruction1<T0>(def,
                                            static_cast<RegisterRef<T0> *>(regRefs[0]));
    }

    // implement Instruction
    void describe(string& out, bool values) const {
        out = "CALL '";
        out += _def.getSignature();
        out += "' ";
        describeArg(out, values, _reg0);
    }
    void exec(TProgramCounter &pc) const {
        pc++;
        try {
            (*_def._functor)(_reg0);
        } catch (char const * str) {
            throw CalcMessage(str, pc - 1);
        }
    }
};

/**
 * Extended instruction which takes 1 parameter and is implemented using a
 * functor. Also provides a pointer for caching context.
 */
template <typename T0>
class ExtendedInstruction1Context : public ExtendedInstruction
{
public:
    /**
     * The type of functor used to implement this extended instruction.
     * For example, if T0 is 'int', then we will require a function such
     * as<blockquote>
     *
     * <pre>void execute(RegisterRef<int>* reg0</pre>
     *
     * </blockquote>
     */
    typedef void (*Functor)(boost::scoped_ptr<ExtendedInstructionContext>& ctx,
                            RegisterRef<T0>* reg0);
    /**
     * The specific type of the definition of this instruction.
     */
    typedef FunctorExtendedInstructionDef<ExtendedInstruction1Context<T0> > DefT;

private:
    DefT _def;
    boost::scoped_ptr<ExtendedInstructionContext> _ctx;
    RegisterRef<T0>* _reg0;

protected:
    // implement ExtendedInstruction
    ExtendedInstructionDef const &getDef() const {
        return _def;
    }

public:
    explicit
    ExtendedInstruction1Context(DefT &def,
                                RegisterRef<T0>* const reg0)
        : _def(def),
          _ctx(0),
          _reg0(reg0)
    { }
    
    static ExtendedInstruction1Context<T0> *create(DefT &def,
                                                   vector<RegisterReference *> regRefs) {
        assert(regRefs.size() == 1);
        // todo:        assert(regRefs[0].getType() == T0::typeCode);
        return new ExtendedInstruction1Context<T0>(def,
                                                   static_cast<RegisterRef<T0> *>(regRefs[0]));
    }

    // implement Instruction
    void describe(string& out, bool values) const {
        out = "CALL '";
        out += _def.getSignature();
        out += "' ";
        describeArg(out, values, _reg0);
    }
    void exec(TProgramCounter &pc) const {
        pc++;
        try {
            // TODO: Remove this const cast. Ugly, but exec is defined as const
            // TODO: in virtual above.
            (*_def._functor)(const_cast<boost::scoped_ptr<ExtendedInstructionContext>& >(_ctx),
                             _reg0);
        } catch (char const * str) {
            throw CalcMessage(str, pc - 1);
        }
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
     * <pre>void execute(RegisterRef<int>* reg0,
     *                   RegisterRef<double>* reg1)
     * </pre>
     *
     * </blockquote>
     */
    typedef void (*Functor)(RegisterRef<T0>* const reg0,
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

public:
    explicit
    ExtendedInstruction2(
                         DefT &def,
                         RegisterRef<T0>* const reg0,
                         RegisterRef<T1>* const reg1)
        : _def(def),
          _reg0(reg0),
          _reg1(reg1) 
    { }

    static ExtendedInstruction2<T0,T1> *create(DefT &def,
                                               vector<RegisterReference *> regRefs)
    {
        assert(regRefs.size() == 2);
        return new ExtendedInstruction2<T0,T1>(def,
                                               static_cast<RegisterRef<T0> *>(regRefs[0]),
                                               static_cast<RegisterRef<T1> *>(regRefs[1]));
    }

    // implement Instruction
    void describe(string& out, bool values) const {
        out = "CALL '";
        out += _def.getSignature();
        out += "' ";
        describeArg(out, values, _reg0);
        out += ", ";
        describeArg(out, values, _reg1);
    }
    void exec(TProgramCounter &pc) const {
        pc++;
        try {
            (*_def._functor)(_reg0,_reg1);
        } catch (char const * str) {
            throw CalcMessage(str, pc - 1);
        }
    }
};

/**
 * Extended instruction which takes 2 parameters and is implemented using a
 * functor. Also provides a pointer for caching context.
 */
template <typename T0, typename T1>
class ExtendedInstruction2Context : public ExtendedInstruction
{
public:
    /**
     * The type of functor used to implement this extended instruction. For
     * example, if T0 is 'int' and T1 is 'double',
     * then we will require a function such as<blockquote>
     *
     * <pre>void execute(RegisterRef<int>* reg0,
     *                   RegisterRef<double>* reg1)
     * </pre>
     *
     * </blockquote>
     */
    typedef void (*Functor)(boost::scoped_ptr<ExtendedInstructionContext>& ctx,
                            RegisterRef<T0>* const reg0,
                            RegisterRef<T1>* const reg1);
    /**
     * The specific type of the definition of this instruction.
     */
    typedef FunctorExtendedInstructionDef<ExtendedInstruction2Context<T0,T1> > DefT;

private:
    DefT _def;
    boost::scoped_ptr<ExtendedInstructionContext> _ctx;
    RegisterRef<T0>* _reg0;
    RegisterRef<T1>* _reg1;

protected:
    // implement ExtendedInstruction
    ExtendedInstructionDef const &getDef() const {
        return _def;
    }

public:
    explicit
    ExtendedInstruction2Context(DefT &def,
                                RegisterRef<T0>* const reg0,
                                RegisterRef<T1>* const reg1)
        : _def(def),
          _ctx(0),
          _reg0(reg0),
          _reg1(reg1) 
    { }

    static ExtendedInstruction2Context<T0,T1> *create(DefT &def,
                                                      vector<RegisterReference *> regRefs)
    {
        assert(regRefs.size() == 2);
        return new ExtendedInstruction2Context<T0,T1>(def,
                                                      static_cast<RegisterRef<T0> *>(regRefs[0]),
                                                      static_cast<RegisterRef<T1> *>(regRefs[1]));
    }

    // implement Instruction
    void describe(string& out, bool values) const {
        out = "CALL '";
        out += _def.getSignature();
        out += "' ";
        describeArg(out, values, _reg0);
        out += ", ";
        describeArg(out, values, _reg1);
    }
    void exec(TProgramCounter &pc) const {
        pc++;
        try {
            // TODO: Remove this const cast. Ugly, but exec is defined as const
            // TODO: in virtual above.
            (*_def._functor)(const_cast<boost::scoped_ptr<ExtendedInstructionContext>& >(_ctx),
                             _reg0,_reg1);
        } catch (char const * str) {
            throw CalcMessage(str, pc - 1);
        }
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
     *     RegisterRef<int> &reg0,
     *     RegisterRef<double> &reg1,
     *     RegisterRef<int> &reg2)</pre>
     *
     * </blockquote>
     */
    typedef void (*Functor)(RegisterRef<T0>* const reg0,
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

public:
    explicit
    ExtendedInstruction3(DefT &def,
                         RegisterRef<T0>* const reg0,
                         RegisterRef<T1>* const reg1,
                         RegisterRef<T2>* const reg2)
        : _def(def),
          _reg0(reg0),
          _reg1(reg1),
          _reg2(reg2)
    { }

    static ExtendedInstruction3<T0,T1,T2> *create(DefT &def,
                                                  vector<RegisterReference *> regRefs)
    {
        assert(regRefs.size() == 3);
        return new ExtendedInstruction3<T0,T1,T2>(
                                                  def,
                                                  static_cast<RegisterRef<T0> *>(regRefs[0]),
                                                  static_cast<RegisterRef<T1> *>(regRefs[1]),
                                                  static_cast<RegisterRef<T2> *>(regRefs[2]));
    }

    // implement Instruction
    void describe(string& out, bool values) const {
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
        pc++;
        try {
            (*_def._functor)(_reg0,_reg1,_reg2);
        } catch (char const * str) {
            throw CalcMessage(str, pc - 1);
        }
    }
};

/**
 * Extended instruction which takes 3 parameters and is implemented using a
 * functor. Also provides a pointer for caching context.
 */
template <typename T0, typename T1, typename T2>
class ExtendedInstruction3Context : public ExtendedInstruction
{
public:
    /**
     * The type of functor used to implement this extended instruction. For
     * example, if T0 is 'int' and T1 is 'double' and T2 is 'int'
     * then we will require a function such as<blockquote>
     *
     * <pre>void execute(
     *     RegisterRef<int> &reg0,
     *     RegisterRef<double> &reg1,
     *     RegisterRef<int> &reg2)</pre>
     *
     * </blockquote>
     */
    typedef void (*Functor)(boost::scoped_ptr<ExtendedInstructionContext>& ctx,
                            RegisterRef<T0>* const reg0,
                            RegisterRef<T1>* const reg1,
                            RegisterRef<T2>* const reg2);
    /**
     * The specific type of the definition of this instruction.
     */
    typedef FunctorExtendedInstructionDef<ExtendedInstruction3Context<T0,T1,T2> > DefT;

private:
    DefT _def;
    boost::scoped_ptr<ExtendedInstructionContext> _ctx;
    RegisterRef<T0>* _reg0;
    RegisterRef<T1>* _reg1;
    RegisterRef<T2>* _reg2;

protected:
    // implement ExtendedInstruction
    ExtendedInstructionDef const &getDef() const {
        return _def;
    }

public:
    explicit
    ExtendedInstruction3Context(DefT &def,
                                RegisterRef<T0>* const reg0,
                                RegisterRef<T1>* const reg1,
                                RegisterRef<T2>* const reg2)
        : _def(def),
          _ctx(0),
          _reg0(reg0),
          _reg1(reg1),
          _reg2(reg2)
    { }

    static ExtendedInstruction3Context<T0,T1,T2> *create(DefT &def,
                                                         vector<RegisterReference *> regRefs)
    {
        assert(regRefs.size() == 3);
        return new ExtendedInstruction3Context<T0,T1,T2>(
                                                         def,
                                                         static_cast<RegisterRef<T0> *>(regRefs[0]),
                                                         static_cast<RegisterRef<T1> *>(regRefs[1]),
                                                         static_cast<RegisterRef<T2> *>(regRefs[2]));
    }

    // implement Instruction
    void describe(string& out, bool values) const {
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
        pc++;
        try {
            // TODO: Remove this const cast. Ugly, but exec is defined as const
            // TODO: in virtual above.
            (*_def._functor)(const_cast<boost::scoped_ptr<ExtendedInstructionContext>& >(_ctx),
                             _reg0, _reg1, _reg2);
        } catch (char const * str) {
            throw CalcMessage(str, pc - 1);
        }
    }
};

/**
 * Extended instruction which takes 4 parameters and is implemented using a
 * functor.
 */
template <typename T0, typename T1, typename T2, typename T3>
class ExtendedInstruction4 : public ExtendedInstruction
{
public:
    /**
     * The type of functor used to implement this extended instruction. For
     * example, if T0 is 'int' and T1 is 'double' and T2 is 'int'
     * then we will require a function such as<blockquote>
     *
     * <pre>void execute(
     *     RegisterRef<int> &reg0,
     *     RegisterRef<double> &reg1,
     *     RegisterRef<int> &reg2)</pre>
     *
     * </blockquote>
     */
    typedef void (*Functor)(RegisterRef<T0>* const reg0,
                            RegisterRef<T1>* const reg1,
                            RegisterRef<T2>* const reg2,
                            RegisterRef<T3>* const reg3);
    /**
     * The specific type of the definition of this instruction.
     */
    typedef FunctorExtendedInstructionDef<ExtendedInstruction4<T0,T1,T2,T3> > DefT;

private:
    DefT _def;
    RegisterRef<T0>* _reg0;
    RegisterRef<T1>* _reg1;
    RegisterRef<T2>* _reg2;
    RegisterRef<T3>* _reg3;

protected:
    // implement ExtendedInstruction
    ExtendedInstructionDef const &getDef() const {
        return _def;
    }

public:
    explicit
    ExtendedInstruction4(DefT &def,
                         RegisterRef<T0>* const reg0,
                         RegisterRef<T1>* const reg1,
                         RegisterRef<T2>* const reg2,
                         RegisterRef<T3>* const reg3)
        : _def(def),
          _reg0(reg0),
          _reg1(reg1),
          _reg2(reg2),
          _reg3(reg3)
    { }

    static ExtendedInstruction4<T0,T1,T2,T3> *create(DefT &def,
                                                     vector<RegisterReference *> regRefs)
    {
        assert(regRefs.size() == 4);
        return new
            ExtendedInstruction4<T0,T1,T2,T3>(def,
                                              static_cast<RegisterRef<T0> *>(regRefs[0]),
                                              static_cast<RegisterRef<T1> *>(regRefs[1]),
                                              static_cast<RegisterRef<T2> *>(regRefs[2]),
                                              static_cast<RegisterRef<T3> *>(regRefs[3]));
    }

    // implement Instruction
    void describe(string& out, bool values) const {
        out = "CALL '";
        out += _def.getSignature();
        out += "' ";
        describeArg(out, values, _reg0);
        out += ", ";
        describeArg(out, values, _reg1);
        out += ", ";
        describeArg(out, values, _reg2);
        out += ", ";
        describeArg(out, values, _reg3);
    }
    void exec(TProgramCounter &pc) const {
        pc++;
        try {
            (*_def._functor)(_reg0,_reg1,_reg2,_reg3);
        } catch (char const * str) {
            throw CalcMessage(str, pc - 1);
        }
    }
};
/**
 * Extended instruction which takes 4 parameters and is implemented using a
 * functor. Also provides a pointer for caching context.
 */
template <typename T0, typename T1, typename T2, typename T3>
class ExtendedInstruction4Context : public ExtendedInstruction
{
public:
    /**
     * The type of functor used to implement this extended instruction. For
     * example, if T0 is 'int' and T1 is 'double' and T2 is 'int'
     * then we will require a function such as<blockquote>
     *
     * <pre>void execute(
     *     RegisterRef<int> &reg0,
     *     RegisterRef<double> &reg1,
     *     RegisterRef<int> &reg2)</pre>
     *
     * </blockquote>
     */
    typedef void (*Functor)(boost::scoped_ptr<ExtendedInstructionContext>& ctx,
                            RegisterRef<T0>* const reg0,
                            RegisterRef<T1>* const reg1,
                            RegisterRef<T2>* const reg2,
                            RegisterRef<T3>* const reg3);
    /**
     * The specific type of the definition of this instruction.
     */
    typedef FunctorExtendedInstructionDef<ExtendedInstruction4Context<T0,T1,T2,T3> > DefT;

private:
    DefT _def;
    boost::scoped_ptr<ExtendedInstructionContext> _ctx;
    RegisterRef<T0>* _reg0;
    RegisterRef<T1>* _reg1;
    RegisterRef<T2>* _reg2;
    RegisterRef<T3>* _reg3;

protected:
    // implement ExtendedInstruction
    ExtendedInstructionDef const &getDef() const {
        return _def;
    }

public:
    explicit
    ExtendedInstruction4Context(DefT &def,
                                RegisterRef<T0>* const reg0,
                                RegisterRef<T1>* const reg1,
                                RegisterRef<T2>* const reg2,
                                RegisterRef<T3>* const reg3)
        : _def(def),
          _ctx(0),
          _reg0(reg0),
          _reg1(reg1),
          _reg2(reg2),
          _reg3(reg3)
    { }

    static ExtendedInstruction4Context<T0,T1,T2,T3> *create(DefT &def,
                                                            vector<RegisterReference *> regRefs)
    {
        assert(regRefs.size() == 4);
        return new
            ExtendedInstruction4Context<T0,T1,T2,T3>(def,
                                                     static_cast<RegisterRef<T0> *>(regRefs[0]),
                                                     static_cast<RegisterRef<T1> *>(regRefs[1]),
                                                     static_cast<RegisterRef<T2> *>(regRefs[2]),
                                                     static_cast<RegisterRef<T3> *>(regRefs[3]));
    }

    // implement Instruction
    void describe(string& out, bool values) const {
        out = "CALL '";
        out += _def.getSignature();
        out += "' ";
        describeArg(out, values, _reg0);
        out += ", ";
        describeArg(out, values, _reg1);
        out += ", ";
        describeArg(out, values, _reg2);
        out += ", ";
        describeArg(out, values, _reg3);
    }
    void exec(TProgramCounter &pc) const {
        pc++;
        try {
            // TODO: Remove this const cast. Ugly, but exec is defined as const
            // TODO: in virtual above.
            (*_def._functor)(const_cast<boost::scoped_ptr<ExtendedInstructionContext>& >(_ctx),
                             _reg0, _reg1, _reg2, _reg3);
        } catch (char const * str) {
            throw CalcMessage(str, pc - 1);
        }
    }
};

/**
 * Extended instruction which takes 5 parameters and is implemented using a
 * functor.
 */
template <typename T0, typename T1, typename T2, typename T3, typename T4>
class ExtendedInstruction5 : public ExtendedInstruction
{
public:
    /**
     * The type of functor used to implement this extended instruction. For
     * example, if T0 is 'int' and T1 is 'double' and T2 is 'int'
     * then we will require a function such as<blockquote>
     *
     * <pre>void execute(
     *     RegisterRef<int> &reg0,
     *     RegisterRef<double> &reg1,
     *     RegisterRef<int> &reg2)</pre>
     *
     * </blockquote>
     */
    typedef void (*Functor)(RegisterRef<T0>* const reg0,
                            RegisterRef<T1>* const reg1,
                            RegisterRef<T2>* const reg2,
                            RegisterRef<T3>* const reg3,
                            RegisterRef<T4>* const reg4);
    /**
     * The specific type of the definition of this instruction.
     */
    typedef FunctorExtendedInstructionDef<ExtendedInstruction5<T0,T1,T2,T3,T4> > DefT;

private:
    DefT _def;
    RegisterRef<T0>* _reg0;
    RegisterRef<T1>* _reg1;
    RegisterRef<T2>* _reg2;
    RegisterRef<T3>* _reg3;
    RegisterRef<T4>* _reg4;

protected:
    // implement ExtendedInstruction
    ExtendedInstructionDef const &getDef() const {
        return _def;
    }

public:
    explicit
    ExtendedInstruction5(DefT &def,
                         RegisterRef<T0>* const reg0,
                         RegisterRef<T1>* const reg1,
                         RegisterRef<T2>* const reg2,
                         RegisterRef<T3>* const reg3,
                         RegisterRef<T4>* const reg4)
        : _def(def),
          _reg0(reg0),
          _reg1(reg1),
          _reg2(reg2),
          _reg3(reg3),
          _reg4(reg4)
    { }

    static ExtendedInstruction5<T0,T1,T2,T3,T4> *create(DefT &def,
                                                        vector<RegisterReference *> regRefs) {
        assert(regRefs.size() == 5);
        return new ExtendedInstruction5<T0,T1,T2,T3,T4>(
                                                        def,
                                                        static_cast<RegisterRef<T0> *>(regRefs[0]),
                                                        static_cast<RegisterRef<T1> *>(regRefs[1]),
                                                        static_cast<RegisterRef<T2> *>(regRefs[2]),
                                                        static_cast<RegisterRef<T3> *>(regRefs[3]),
                                                        static_cast<RegisterRef<T4> *>(regRefs[4]));
    }

    // implement Instruction
    void describe(string& out, bool values) const {
        out = "CALL '";
        out += _def.getSignature();
        out += "' ";
        describeArg(out, values, _reg0);
        out += ", ";
        describeArg(out, values, _reg1);
        out += ", ";
        describeArg(out, values, _reg2);
        out += ", ";
        describeArg(out, values, _reg3);
        out += ", ";
        describeArg(out, values, _reg4);
    }
    void exec(TProgramCounter &pc) const {
        pc++;
        try {
            (*_def._functor)(_reg0,_reg1,_reg2,_reg3,_reg4);
        } catch (char const * str) {
            throw CalcMessage(str, pc - 1);
        }
    }
};

/**
 * Extended instruction which takes 5 parameters and is implemented using a
 * functor. Also provides a pointer for caching context.
 */
template <typename T0, typename T1, typename T2, typename T3, typename T4>
class ExtendedInstruction5Context : public ExtendedInstruction
{
public:
    /**
     * The type of functor used to implement this extended instruction. For
     * example, if T0 is 'int' and T1 is 'double' and T2 is 'int'
     * then we will require a function such as<blockquote>
     *
     * <pre>void execute(
     *     RegisterRef<int> &reg0,
     *     RegisterRef<double> &reg1,
     *     RegisterRef<int> &reg2)</pre>
     *
     * </blockquote>
     */
    typedef void (*Functor)(boost::scoped_ptr<ExtendedInstructionContext>& ctx,
                            RegisterRef<T0>* const reg0,
                            RegisterRef<T1>* const reg1,
                            RegisterRef<T2>* const reg2,
                            RegisterRef<T3>* const reg3,
                            RegisterRef<T4>* const reg4);
    /**
     * The specific type of the definition of this instruction.
     */
    typedef FunctorExtendedInstructionDef<ExtendedInstruction5Context<T0,T1,T2,T3,T4> > DefT;

private:
    DefT _def;
    boost::scoped_ptr<ExtendedInstructionContext> _ctx;
    RegisterRef<T0>* _reg0;
    RegisterRef<T1>* _reg1;
    RegisterRef<T2>* _reg2;
    RegisterRef<T3>* _reg3;
    RegisterRef<T4>* _reg4;

protected:
    // implement ExtendedInstruction
    ExtendedInstructionDef const &getDef() const {
        return _def;
    }

public:
    explicit
    ExtendedInstruction5Context(DefT &def,
                                RegisterRef<T0>* const reg0,
                                RegisterRef<T1>* const reg1,
                                RegisterRef<T2>* const reg2,
                                RegisterRef<T3>* const reg3,
                                RegisterRef<T4>* const reg4)
        : _def(def),
          _ctx(0),
          _reg0(reg0),
          _reg1(reg1),
          _reg2(reg2),
          _reg3(reg3),
          _reg4(reg4)
    { }

    static ExtendedInstruction5Context<T0,T1,T2,T3,T4> *create(DefT &def,
                                                               vector<RegisterReference *> regRefs) {
        assert(regRefs.size() == 5);
        return new ExtendedInstruction5Context<T0,T1,T2,T3,T4>(
                                                               def,
                                                               static_cast<RegisterRef<T0> *>(regRefs[0]),
                                                               static_cast<RegisterRef<T1> *>(regRefs[1]),
                                                               static_cast<RegisterRef<T2> *>(regRefs[2]),
                                                               static_cast<RegisterRef<T3> *>(regRefs[3]),
                                                               static_cast<RegisterRef<T4> *>(regRefs[4]));
    }

    // implement Instruction
    void describe(string& out, bool values) const {
        out = "CALL '";
        out += _def.getSignature();
        out += "' ";
        describeArg(out, values, _reg0);
        out += ", ";
        describeArg(out, values, _reg1);
        out += ", ";
        describeArg(out, values, _reg2);
        out += ", ";
        describeArg(out, values, _reg3);
        out += ", ";
        describeArg(out, values, _reg4);
    }
    void exec(TProgramCounter &pc) const {
        pc++;
        try {
            // TODO: Remove this const cast. Ugly, but exec is defined as const
            // TODO: in virtual above.
            (*_def._functor)(const_cast<boost::scoped_ptr<ExtendedInstructionContext>& >(_ctx),
                             _reg0, _reg1, _reg2, _reg3, _reg4);
        } catch (char const * str) {
            throw CalcMessage(str, pc - 1);
        }
    }
};


FENNEL_END_NAMESPACE

#endif

// End ExtendedInstruction.h
