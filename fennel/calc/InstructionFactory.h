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
//
// InstructionFactory
//
*/

#ifndef Fennel_InstructionFactory_Included
#define Fennel_InstructionFactory_Included

#include "fennel/calc/CalcAssemblerException.h"
#include "fennel/calc/RegisterReference.h"
#include "fennel/tuple/StandardTypeDescriptor.h"
#include <boost/shared_ptr.hpp>

FENNEL_BEGIN_NAMESPACE

// Factory for creating instructions with the same type
template < typename TMPLT, StandardTypeDescriptorOrdinal typeOrdinal>
class TypedInstructionFactory
{
public:
    template < template <typename T> class TInstruction >
    static TInstruction<TMPLT>* createInstruction(RegisterReference* result,
                                                  RegisterReference* op1,
                                                  RegisterReference* op2)
    {
        assert(result);
        assert(op1);
        assert(op2);
        if ((result->type() != typeOrdinal)) {
            throw InvalidTypeException("Invalid result", result->type(), typeOrdinal );
        }
        if ((op1->type() != typeOrdinal)) {
            throw InvalidTypeException("Invalid operand1", op1->type(), typeOrdinal );
        }
        if ((op2->type() != typeOrdinal)) {
            throw InvalidTypeException("Invalid operand2", op2->type(), typeOrdinal );
        }
        return new TInstruction<TMPLT>(static_cast<RegisterRef<TMPLT> *> (result),
                                       static_cast<RegisterRef<TMPLT> *> (op1),
                                       static_cast<RegisterRef<TMPLT> *> (op2),
                                       typeOrdinal);
    }

    template < template <typename T> class TInstruction >
    static TInstruction<TMPLT>* createInstruction(RegisterReference* result,
                                                  RegisterReference* op1)
    {
        assert(result);
        assert(op1);
        if ((result->type() != typeOrdinal)) {
            throw InvalidTypeException("Invalid result", result->type(), typeOrdinal );
        }
        if ((op1->type() != typeOrdinal)) {
            throw InvalidTypeException("Invalid operand", op1->type(), typeOrdinal );
        }
        return new TInstruction<TMPLT>(static_cast<RegisterRef<TMPLT> *> (result),
                                       static_cast<RegisterRef<TMPLT> *> (op1),
                                       typeOrdinal);
    }

    template < template <typename T> class TInstruction >
    static TInstruction<TMPLT>* createInstruction(RegisterReference* result)
    {
        assert(result);
        if ((result->type() != typeOrdinal)) {
            throw InvalidTypeException("Invalid result", result->type(), typeOrdinal );
        }
        return new TInstruction<TMPLT>(static_cast<RegisterRef<TMPLT> *> (result),
                                       typeOrdinal);
    }
};

// Factory for creating instructions that results in a bool
// Operands should be of the same type (TMPLT type)
template < typename TMPLT, StandardTypeDescriptorOrdinal typeOrdinal >
class BoolInstructionFactory: public TypedInstructionFactory<TMPLT, typeOrdinal>
{
public:
    template < template <typename T> class TInstruction >
    static TInstruction<TMPLT>* createInstruction(RegisterReference* result,
                                                  RegisterReference* op1,
                                                  RegisterReference* op2)
    {
        assert(result);
        assert(op1);
        assert(op2);
        if ((result->type() != STANDARD_TYPE_BOOL)) {
            throw InvalidTypeException("Invalid result", result->type(), STANDARD_TYPE_BOOL );
        }
        if ((op1->type() != typeOrdinal)) {
            throw InvalidTypeException("Invalid operand1", op1->type(), typeOrdinal );
        }
        if ((op2->type() != typeOrdinal)) {
            throw InvalidTypeException("Invalid operand2", op2->type(), typeOrdinal );
        }
        return new TInstruction<TMPLT>(static_cast<RegisterRef<bool> *> (result),
                                       static_cast<RegisterRef<TMPLT> *> (op1),
                                       static_cast<RegisterRef<TMPLT> *> (op2),
                                       typeOrdinal);
    }

    template < template <typename T> class TInstruction >
    static TInstruction<TMPLT>* createInstruction(RegisterReference* result,
                                                  RegisterReference* op1)
    {
        assert(result);
        assert(op1);
        if ((result->type() != STANDARD_TYPE_BOOL)) {
            throw InvalidTypeException("Invalid result", result->type(), STANDARD_TYPE_BOOL );
        }
        if ((op1->type() != typeOrdinal)) {
            throw InvalidTypeException("Invalid operand", op1->type(), typeOrdinal );
        }
        return new TInstruction<TMPLT>(static_cast<RegisterRef<bool> *> (result),
                                       static_cast<RegisterRef<TMPLT> *> (op1),
                                       typeOrdinal);
    }

    template < template <typename T> class TInstruction >
    static TInstruction<TMPLT>* createInstruction(RegisterReference* result)
    {
        assert(result);
        if ((result->type() != STANDARD_TYPE_BOOL)) {
            throw InvalidTypeException("Invalid result", result->type(), STANDARD_TYPE_BOOL );
        }
        return new TInstruction<TMPLT>(static_cast<RegisterRef<bool> *> (result));
    }
};


// Factory for creating instructions that results in a bool
// For when operands are also bool
template <>
class BoolInstructionFactory<bool, STANDARD_TYPE_BOOL>: 
      public TypedInstructionFactory<bool, STANDARD_TYPE_BOOL>
{
public:
    template < typename TInstruction >
    static TInstruction* createInstruction(RegisterReference* result,
                                           RegisterReference* op1,
                                           RegisterReference* op2)
    {
        assert(result);
        assert(op1);
        assert(op2);
        if ((result->type() != STANDARD_TYPE_BOOL)) {
            throw InvalidTypeException("Invalid result", result->type(), STANDARD_TYPE_BOOL );
        }
        if ((op1->type() != STANDARD_TYPE_BOOL)) {
            throw InvalidTypeException("Invalid operand1", op1->type(), STANDARD_TYPE_BOOL );
        }
        if ((op2->type() != STANDARD_TYPE_BOOL)) {
            throw InvalidTypeException("Invalid operand2", op2->type(), STANDARD_TYPE_BOOL );
        }
        return new TInstruction(static_cast<RegisterRef<bool> *> (result),
                                static_cast<RegisterRef<bool> *> (op1),
                                static_cast<RegisterRef<bool> *> (op2));
    }

    template < typename TInstruction >
    static TInstruction* createInstruction(RegisterReference* result,
                                           RegisterReference* op1)
    {
        assert(result);
        assert(op1);
        if ((result->type() != STANDARD_TYPE_BOOL)) {
            throw InvalidTypeException("Invalid result", result->type(), STANDARD_TYPE_BOOL );
        }
        if ((op1->type() != STANDARD_TYPE_BOOL)) {
            throw InvalidTypeException("Invalid operand", op1->type(), STANDARD_TYPE_BOOL );
        }
        return new TInstruction(static_cast<RegisterRef<bool> *> (result),
                                static_cast<RegisterRef<bool> *> (op1));
    }

    template < typename TInstruction >
    static TInstruction* createInstruction(RegisterReference* result)
    {
        assert(result);
        if ((result->type() != STANDARD_TYPE_BOOL)) {
            throw InvalidTypeException("Invalid result", result->type(), STANDARD_TYPE_BOOL );
        }
        return new TInstruction(static_cast<RegisterRef<bool> *> (result));
    }

};

// Factory for creating instructions that results in a pointer type
// Only used for instructions in which the operand is a different type from the
//  pointer type (TMPLT is the type of that operand)
template < typename PTRTYPE,
           StandardTypeDescriptorOrdinal pointerType,
           typename TMPLT,
           StandardTypeDescriptorOrdinal typeOrdinal >
class TypedPointerInstructionFactory: 
      public TypedInstructionFactory<PTRTYPE, pointerType>
{
public:
    template < template <typename T> class TInstruction >
    static TInstruction<PTRTYPE>* createInstruction(RegisterReference* result,
                                                    RegisterReference* op1,
                                                    RegisterReference* op2)
    {
        assert(result);
        assert(op1);
        assert(op2);
        if ((result->type() != pointerType)) {
            throw InvalidTypeException("Invalid result", result->type(), pointerType );
        }
        if ((op1->type() != pointerType)) {
            throw InvalidTypeException("Invalid operand1", op1->type(), pointerType );
        }
        if ((op2->type() != typeOrdinal)) {
            throw InvalidTypeException("Invalid operand2", op2->type(), typeOrdinal );
        }
        return new TInstruction<PTRTYPE>(static_cast<RegisterRef<PTRTYPE> *> (result),
                                         static_cast<RegisterRef<PTRTYPE> *> (op1),
                                         static_cast<RegisterRef<TMPLT> *>   (op2),
                                         pointerType);
    }

    template < template <typename T> class TInstruction >
    static TInstruction<PTRTYPE>* createInstruction(RegisterReference* result,
                                                    RegisterReference* op1)
    {
        assert(result);
        assert(op1);
        if ((result->type() != pointerType)) {
            throw InvalidTypeException("Invalid result", result->type(), pointerType );
        }
        if ((op1->type() != typeOrdinal)) {
            throw InvalidTypeException("Invalid operand", op1->type(), typeOrdinal );
        }
        return new TInstruction<PTRTYPE>(static_cast<RegisterRef<PTRTYPE> *> (result),
                                         static_cast<RegisterRef<TMPLT> *>   (op1),
                                         typeOrdinal);
    }
};

template < typename PTRTYPE, StandardTypeDescriptorOrdinal pointerType >
class PointerPointerOperandTInstructionFactory:
      public TypedPointerInstructionFactory<PTRTYPE, pointerType, 
                                            PointerOperandT, STANDARD_TYPE_UINT_32>
{
};

template < typename PTRTYPE, StandardTypeDescriptorOrdinal pointerType >
class PointerPointerSizeTInstructionFactory:
      public TypedPointerInstructionFactory<PTRTYPE, pointerType, 
                                            PointerSizeT, STANDARD_TYPE_UINT_32>
{
};

// IntegralPointerInstructionFactory
// Creates Pointer instructions that returns PointerSizeT
// TODO: Move into IntegralPointerInstruction.h
template < typename PTRTYPE,
           StandardTypeDescriptorOrdinal pointerType >
class IntegralPointerInstructionFactory: 
      public TypedInstructionFactory<PTRTYPE, pointerType>
{
public:
    template < template <typename T> class TInstruction >
    static TInstruction<PTRTYPE>* createInstruction(RegisterReference* result,
                                                    RegisterReference* op1)
    {
        assert(result);
        assert(op1);
        if ((result->type() != STANDARD_TYPE_UINT_32)) {
            throw InvalidTypeException("Invalid result", result->type(), STANDARD_TYPE_UINT_32 );
        }
        if ((op1->type() != pointerType)) {
            throw InvalidTypeException("Invalid operand1", op1->type(), pointerType );
        }
        return new TInstruction<PTRTYPE>(static_cast<RegisterRef<PointerSizeT> *> (result),
                                         static_cast<RegisterRef<PTRTYPE> *> (op1),
                                         pointerType);
    }
};

// Base class for instruction factory - always returns NULL
class BaseInstructionFactory
{
public:
    explicit 
    BaseInstructionFactory() {}
    virtual ~BaseInstructionFactory() {}

    virtual Instruction* createInstruction(RegisterReference* result) 
    { return NULL; }

    virtual Instruction* createInstruction(RegisterReference* result,
                                           RegisterReference* op1) 
    { return NULL; }

    virtual Instruction* createInstruction(RegisterReference* result,
                                           RegisterReference* op1,
                                           RegisterReference* op2) 
    { return NULL; } 

    virtual Instruction* createInstruction(TProgramCounter pc)
    { return NULL; }

    virtual Instruction* createInstruction(TProgramCounter pc,
                                           RegisterReference* cond)
    { return NULL; }

};

// Base class for instruction factory map that takes type and gives
// back the appropriate factory for creating instructions of that type
// Base class always returns NULL
class BaseInstructionFactoryMap
{
public:
    explicit 
    BaseInstructionFactoryMap() {}
    virtual ~BaseInstructionFactoryMap() {}

    virtual BaseInstructionFactory* getInstructionFactory(
        StandardTypeDescriptorOrdinal typeOrdinal)
    { return NULL; }

protected:
    typedef boost::shared_ptr<BaseInstructionFactory> FactoryPtr;
    typedef map < StandardTypeDescriptorOrdinal, FactoryPtr > TypeFactoryMap;
    typedef map < string, FactoryPtr > NameFactoryMap;
};

// Factory that knows how to create one instruction of one type
// TInstruction: instruction to create
// TIFactory:    factory for creating instructions of particular type
// args:         number of arguments the instruction takes (1,2, or 3)
template < template <typename T> class TInstruction, typename TIFactory, int args>
class SingleTypedInstructionFactory: public BaseInstructionFactory
{
public:
    SingleTypedInstructionFactory() {}
    virtual ~SingleTypedInstructionFactory() {}

    // TODO: Add something so this can not be instantianted if args not 1,2, or 3
};

template < template <typename T> class TInstruction, typename TIFactory>
class SingleTypedInstructionFactory<TInstruction, TIFactory, 1>: 
      public BaseInstructionFactory
{
public:
    SingleTypedInstructionFactory() {}
    virtual ~SingleTypedInstructionFactory() {}

    virtual Instruction* createInstruction(RegisterReference* result)
    {  
        return TIFactory::template createInstruction<TInstruction>(result);
    }
};

template < template <typename T> class TInstruction, typename TIFactory>
class SingleTypedInstructionFactory<TInstruction, TIFactory, 2>: 
      public BaseInstructionFactory
{
public:
    SingleTypedInstructionFactory() {}
    virtual ~SingleTypedInstructionFactory() {}

    virtual Instruction* createInstruction(RegisterReference* result,
                                           RegisterReference* op1)
    {  
        return TIFactory::template createInstruction<TInstruction>(result, op1);
    }
};

template < template <typename T> class TInstruction, typename TIFactory>
class SingleTypedInstructionFactory<TInstruction, TIFactory, 3>: 
      public BaseInstructionFactory
{
 public:
    SingleTypedInstructionFactory() {}
    virtual ~SingleTypedInstructionFactory() {}

    virtual Instruction* createInstruction(RegisterReference* result,
                                           RegisterReference* op1,
                                           RegisterReference* op2)
    {  
        return TIFactory::template createInstruction<TInstruction>(result, op1, op2);
    }
};

// Factory that knows how to create one instruction of one type
// TInstruction: instruction to create
// TIFactory:    factory for creating instructions of particular type
// args:         number of arguments the instruction takes (1,2, or 3)
template < typename TInstruction, typename TIFactory, int args>
class SingleInstructionFactory: public BaseInstructionFactory
{
public:
    SingleInstructionFactory() {}
    virtual ~SingleInstructionFactory() {}

    // TODO: Add something so this can not be instantianted if args not 1,2, or 3
};

template < typename TInstruction, typename TIFactory>
class SingleInstructionFactory<TInstruction, TIFactory, 1>: 
      public BaseInstructionFactory
{
public:
    SingleInstructionFactory() {}
    virtual ~SingleInstructionFactory() {}

    virtual Instruction* createInstruction(RegisterReference* result)
    {  
        return TIFactory::template createInstruction<TInstruction>(result);
    }
};

template < typename TInstruction, typename TIFactory>
class SingleInstructionFactory<TInstruction, TIFactory, 2>: 
      public BaseInstructionFactory
{
public:
    SingleInstructionFactory() {}
    virtual ~SingleInstructionFactory() {}

    virtual Instruction* createInstruction(RegisterReference* result,
                                           RegisterReference* op1)
    {  
        return TIFactory::template createInstruction<TInstruction>(result, op1);
    }
};

template < typename TInstruction, typename TIFactory>
class SingleInstructionFactory<TInstruction, TIFactory, 3>: 
      public BaseInstructionFactory
{
 public:
    SingleInstructionFactory() {}
    virtual ~SingleInstructionFactory() {}

    virtual Instruction* createInstruction(RegisterReference* result,
                                           RegisterReference* op1,
                                           RegisterReference* op2)
    {  
        return TIFactory::template createInstruction<TInstruction>(result, op1, op2);
    }
};

class JumpInstructionFactory
{
public:
    template < typename TInstruction >
    static Instruction* createInstruction(TProgramCounter pc)
    {
        return new TInstruction(pc);
    }

    template < typename TInstruction >
    static Instruction* createInstruction(TProgramCounter pc,
                                          RegisterReference* cond)
    {
        assert(cond != NULL);
        if (cond->type() != STANDARD_TYPE_BOOL) 
        {
            throw InvalidTypeException("Invalid operand", cond->type(), STANDARD_TYPE_BOOL );
        }
        return new TInstruction(pc, static_cast<RegisterRef<bool> *> (cond));
    }
};

template < typename TInstruction >
class SingleInstructionFactory<TInstruction, JumpInstructionFactory, 2>: 
      public BaseInstructionFactory
{
public:
    SingleInstructionFactory() {}
    virtual ~SingleInstructionFactory() {}

    virtual Instruction* createInstruction(TProgramCounter pc,
                                           RegisterReference* cond)
    {  
        return JumpInstructionFactory::template createInstruction<TInstruction>(pc, cond);
    }
};


template < typename TInstruction >
class SingleInstructionFactory<TInstruction, JumpInstructionFactory, 1>: 
      public BaseInstructionFactory
{
public:
    SingleInstructionFactory() {}
    virtual ~SingleInstructionFactory() {}

    virtual Instruction* createInstruction(TProgramCounter pc)
    {  
        return JumpInstructionFactory::template createInstruction<TInstruction>(pc);
    }
};

class JumpInstructionFactoryMap: public BaseInstructionFactoryMap
{
public:
    JumpInstructionFactoryMap() 
    {
    }
    virtual ~JumpInstructionFactoryMap() {}

    virtual BaseInstructionFactory* getInstructionFactory(string& name)
    {
        return factoryMap[name].get();
    }

    template < typename TInstruction, int args >
    void registerInstruction(string name)
    {
        factoryMap[name] = 
            FactoryPtr(static_cast<BaseInstructionFactory*>
                       (new SingleInstructionFactory<TInstruction, JumpInstructionFactory, args>()));
    }

protected:
    NameFactoryMap factoryMap;
};

// Factory Map of types to SingleInstructionFactory
// Provides a function for looking up the SingleInstructionFactory to use given the type

// TInstFactory: factory for creating instructions 
//               (TypedInstructionFactory, BoolInstructionFactory, or PointerInstructionFactory)

// TODO: Provide different specifications for SingleInstructinoFactoryMap depending
// on the TInstFactory type (e.g Factories that create PointerInstructions need to have
// instantiations of different types than Factories that create NativeInstructions
template < template <typename T, StandardTypeDescriptorOrdinal typeOrdinal> class TInstFactory >
class SingleInstructionFactoryMap: public BaseInstructionFactoryMap
{
public:
    SingleInstructionFactoryMap() 
    {
    }
    virtual ~SingleInstructionFactoryMap() {}

    virtual BaseInstructionFactory* getInstructionFactory(StandardTypeDescriptorOrdinal typeOrdinal)
    {
        return factoryMap[typeOrdinal].get();
    }

    virtual void putInstructionFactory(StandardTypeDescriptorOrdinal typeOrdinal, 
                                       BaseInstructionFactory* factory)
    {
        factoryMap[typeOrdinal] = FactoryPtr(factory);
    }

    // TInstruction: instruction to create
    // args:         number of arguments the instruction takes (1,2, or 3)
    template < template <typename T> class TInstruction, int args >
    void registerNativeOperands()
    {
        registerIntegralNativeOperands<TInstruction, args>();
        registerApproxNativeOperands<TInstruction, args>();
    }

    // TInstruction: instruction to create
    // args:         number of arguments the instruction takes (1,2, or 3)
    template < template <typename T> class TInstruction, int args >
    void registerApproxNativeOperands()
    {
        factoryMap[STANDARD_TYPE_REAL] = 
            FactoryPtr(static_cast<BaseInstructionFactory*>
                       (new SingleTypedInstructionFactory<TInstruction, TIFactoryREAL, args>()));
        factoryMap[STANDARD_TYPE_DOUBLE] = 
            FactoryPtr(static_cast<BaseInstructionFactory*>
                       (new SingleTypedInstructionFactory<TInstruction, TIFactoryDOUBLE, args>()));
    }

    // TInstruction: instruction to create
    // args:         number of arguments the instruction takes (1,2, or 3)
    template < template <typename T> class TInstruction, int args >
    void registerIntegralNativeOperands()
    {
        // TODO: Use putInstructionFunction() function */
        factoryMap[STANDARD_TYPE_INT_8] = 
            FactoryPtr(static_cast<BaseInstructionFactory*>
                       (new SingleTypedInstructionFactory<TInstruction, TIFactoryINT_8, args>()));
        factoryMap[STANDARD_TYPE_UINT_8] = 
            FactoryPtr(static_cast<BaseInstructionFactory*>
                       (new SingleTypedInstructionFactory<TInstruction, TIFactoryUINT_8, args>()));
        factoryMap[STANDARD_TYPE_INT_16] = 
            FactoryPtr(static_cast<BaseInstructionFactory*>
                       (new SingleTypedInstructionFactory<TInstruction, TIFactoryINT_16, args>()));
        factoryMap[STANDARD_TYPE_UINT_16] = 
            FactoryPtr(static_cast<BaseInstructionFactory*>
                       (new SingleTypedInstructionFactory<TInstruction, TIFactoryUINT_16, args>()));
        factoryMap[STANDARD_TYPE_INT_32] = 
            FactoryPtr(static_cast<BaseInstructionFactory*>
                       (new SingleTypedInstructionFactory<TInstruction, TIFactoryINT_32, args>()));
        factoryMap[STANDARD_TYPE_UINT_32] = 
            FactoryPtr(static_cast<BaseInstructionFactory*>
                       (new SingleTypedInstructionFactory<TInstruction, TIFactoryUINT_32, args>()));
        factoryMap[STANDARD_TYPE_INT_64] = 
            FactoryPtr(static_cast<BaseInstructionFactory*>
                       (new SingleTypedInstructionFactory<TInstruction, TIFactoryINT_64, args>()));
        factoryMap[STANDARD_TYPE_UINT_64] = 
            FactoryPtr(static_cast<BaseInstructionFactory*>
                       (new SingleTypedInstructionFactory<TInstruction, TIFactoryUINT_64, args>()));
    }

    // TInstruction: instruction to create
    // args:         number of arguments the instruction takes (1,2, or 3)
    template < typename TInstruction, int args >
    void registerBoolOperands()
    {
        factoryMap[STANDARD_TYPE_BOOL] = 
            FactoryPtr(static_cast<BaseInstructionFactory*>
                       (new SingleInstructionFactory<TInstruction, TIFactoryBOOL, args>()));
    }

    // TInstruction: instruction to create
    // args:         number of arguments the instruction takes (1, 2, or 3)
    template < template <typename T> class TInstruction, int args >
    void registerPointerOperands()
    {
        factoryMap[STANDARD_TYPE_CHAR] = 
            FactoryPtr(static_cast<BaseInstructionFactory*>
                       (new SingleTypedInstructionFactory<TInstruction, TIFactoryCHAR, args>()));

        factoryMap[STANDARD_TYPE_VARCHAR] = 
            FactoryPtr(static_cast<BaseInstructionFactory*>
                       (new SingleTypedInstructionFactory<TInstruction, TIFactoryVARCHAR, args>()));

        factoryMap[STANDARD_TYPE_BINARY] = 
            FactoryPtr(static_cast<BaseInstructionFactory*>
                       (new SingleTypedInstructionFactory<TInstruction, TIFactoryBINARY, args>()));

        factoryMap[STANDARD_TYPE_VARBINARY] = 
            FactoryPtr(static_cast<BaseInstructionFactory*>
                       (new SingleTypedInstructionFactory<TInstruction, TIFactoryVARBINARY, args>()));
    } 

protected:
    TypeFactoryMap factoryMap;

    typedef TInstFactory<int8_t,   STANDARD_TYPE_INT_8>     TIFactoryINT_8;
    typedef TInstFactory<uint8_t,  STANDARD_TYPE_UINT_8>    TIFactoryUINT_8;
    typedef TInstFactory<int16_t,  STANDARD_TYPE_INT_16>    TIFactoryINT_16;
    typedef TInstFactory<uint16_t, STANDARD_TYPE_UINT_16>   TIFactoryUINT_16;
    typedef TInstFactory<int32_t,  STANDARD_TYPE_INT_32>    TIFactoryINT_32;
    typedef TInstFactory<uint32_t, STANDARD_TYPE_UINT_32>   TIFactoryUINT_32;
    typedef TInstFactory<int64_t,  STANDARD_TYPE_INT_64>    TIFactoryINT_64;
    typedef TInstFactory<uint64_t, STANDARD_TYPE_UINT_64>   TIFactoryUINT_64;
    typedef TInstFactory<float,    STANDARD_TYPE_REAL>      TIFactoryREAL;
    typedef TInstFactory<double,   STANDARD_TYPE_DOUBLE>    TIFactoryDOUBLE;
    typedef TInstFactory<bool,     STANDARD_TYPE_BOOL>      TIFactoryBOOL;
    typedef TInstFactory<char*,    STANDARD_TYPE_CHAR>      TIFactoryCHAR;
    typedef TInstFactory<char*,    STANDARD_TYPE_VARCHAR>   TIFactoryVARCHAR;
    typedef TInstFactory<int8_t*,  STANDARD_TYPE_BINARY>    TIFactoryBINARY;
    typedef TInstFactory<int8_t*,  STANDARD_TYPE_VARBINARY> TIFactoryVARBINARY;
};


class ExtendedInstructionFactoryMap
{
public:
    typedef boost::shared_ptr<ExtendedInstructionTable> TablePtr;
    explicit
    ExtendedInstructionFactoryMap() {};
    ~ExtendedInstructionFactoryMap() {};

    TablePtr getInstructionTable(const string& name)
    {
        return factoryMap[name];
    }

    TablePtr registerInstructionTable(const string name)
    {
        if (factoryMap[name] == NULL)
            factoryMap[name] = TablePtr(new ExtendedInstructionTable());
        return factoryMap[name];
    }

    ExtendedInstructionDef* lookupBySignature(const string& name, const string& signature)
    {
        TablePtr table = getInstructionTable(name);
        if (table == NULL) return NULL;
        return table->lookupBySignature(signature);
    }
 
protected:
    map<string, TablePtr> factoryMap;    
};

// Class that people actually use
// Super-smart class that knows how to create instructions given the
// instruction name and reference registers.
class InstructionFactory
{
public:
    // Use boost::shared_ptr so that all the allocated factory maps will just magically disappear
    typedef boost::shared_ptr<BaseInstructionFactoryMap> FactoryMapPtr;

    // Map of Instruction Name to SingleInstructionFactoryMap
    // TODO: Use hash_map instead of map?
    typedef map < string, FactoryMapPtr > StringFactoryMap;

    static BaseInstructionFactory* getNativeInstructionFactory(string& name, StandardTypeDescriptorOrdinal typeOrdinal)
    {
        FactoryMapPtr pFactoryMap = nativeInstructionMap[name];
        if (pFactoryMap == NULL)
            throw FennelExcn(name + " is not a registered native instruction");
        return pFactoryMap->getInstructionFactory(typeOrdinal);
    }

    static BaseInstructionFactory* getBoolInstructionFactory(string& name, StandardTypeDescriptorOrdinal typeOrdinal)
    {
        FactoryMapPtr pFactoryMap = boolInstructionMap[name];
        if (pFactoryMap == NULL)
            throw FennelExcn(name + " is not a registered boolean instruction");
        return pFactoryMap->getInstructionFactory(typeOrdinal);
    }

    static BaseInstructionFactory* getPointerInstructionFactory(string& name, StandardTypeDescriptorOrdinal typeOrdinal)
    {
        FactoryMapPtr pFactoryMap = pointerInstructionMap[name];
        if (pFactoryMap == NULL)
            throw FennelExcn(name + " is not a registered pointer instruction");
        return pFactoryMap->getInstructionFactory(typeOrdinal);
    }

    static BaseInstructionFactory* getInstructionFactory(string& name, 
                                                         RegisterReference* result,
                                                         RegisterReference* operand1 = NULL,
                                                         RegisterReference* operand2 = NULL)
    {
        BaseInstructionFactory* factory = NULL;

        if (result != NULL) {
            if (result->type() == STANDARD_TYPE_BOOL)
            {
                if (operand1 != NULL)
                    factory = getBoolInstructionFactory(name, operand1->type());
                else 
                    factory = getBoolInstructionFactory(name, result->type());
            }
            else if (StandardTypeDescriptor::isArray(result->type())) 
            {
                factory = getPointerInstructionFactory(name, result->type());
            }
            else if ((operand1 != NULL) && (StandardTypeDescriptor::isArray(operand1->type())))
            {
                factory = getPointerInstructionFactory(name, operand1->type());
            }
            else if (StandardTypeDescriptor::isNative(result->type()))
            {
                factory = getNativeInstructionFactory(name, result->type());
            }
        }

        return factory;
    }
    
    static Instruction* createInstruction(string& name,
                                          RegisterReference* result, 
                                          RegisterReference* operand1,
                                          RegisterReference* operand2)
        
    {
        assert(result   != NULL);
        assert(operand1 != NULL);
        assert(operand2 != NULL);

        BaseInstructionFactory* factory = getInstructionFactory(name, result, operand1, operand2);

        if (factory == NULL)
            throw FennelExcn(name + " is not a registered instruction with types "
                             + StandardTypeDescriptor::toString(result->type())   + ", "
                             + StandardTypeDescriptor::toString(operand1->type()) + ", "
                             + StandardTypeDescriptor::toString(operand2->type()) + ", ");

        return factory->createInstruction(result, operand1, operand2);
    }

    static Instruction* createInstruction(string& name,
                                          RegisterReference* result, 
                                          RegisterReference* operand1)
        
    {
        assert(result   != NULL);
        assert(operand1 != NULL);

        BaseInstructionFactory* factory = getInstructionFactory(name, result, operand1);

        if (factory == NULL)
            throw FennelExcn(name + " is not a registered instruction with types "
                             + StandardTypeDescriptor::toString(result->type())   + ", "
                             + StandardTypeDescriptor::toString(operand1->type()));

        return factory->createInstruction(result, operand1);
    }

    static Instruction* createInstruction(string& name,
                                          RegisterReference* result)
    {
        assert(result   != NULL);

        BaseInstructionFactory* factory = getInstructionFactory(name, result);

        if (factory == NULL)
            throw FennelExcn(name + " is not a registered instruction with types "
                             + StandardTypeDescriptor::toString(result->type()));

        return factory->createInstruction(result);
    }

    static Instruction* createInstruction(string& name, TProgramCounter pc, 
                                          RegisterReference* operand = NULL)
    {
        BaseInstructionFactory* factory = jumpInstructionMap.getInstructionFactory(name);

        if (factory == NULL) {
            if (operand == NULL)
                throw FennelExcn(name + " is not a registered instruction with signature pc");
            else throw FennelExcn(name + " is not a registered instruction with signature pc, "
                                  + StandardTypeDescriptor::toString(operand->type()));
        }

        if (operand == NULL)
            return factory->createInstruction(pc);
        else return factory->createInstruction(pc, operand);
    }

    static string computeSignature(string& function,
                                   vector<RegisterReference*>& operands)
    {
        ostringstream ostr;
        ostr << function << "(";
        for (uint i = 0; i < operands.size(); i++)
        {
            assert(operands[i] != NULL);
            if (i > 0) { ostr << ","; }
            ostr << StandardTypeDescriptor::toString(operands[i]->type());
        }
        ostr << ")";
        return ostr.str();
    }

    static Instruction* createInstruction(Calculator* pCalc,
                                          string& name,
                                          string& function,
                                          vector<RegisterReference*>& operands)
    {
        string signature = computeSignature(function, operands);
        ExtendedInstructionDef* instDef = extendedInstructionMap.lookupBySignature(name, signature);
        if (instDef == NULL)
            return NULL;
        return instDef->createInstruction(pCalc, operands);
    }

    template < template <typename T, StandardTypeDescriptorOrdinal typeOrdinal> class TInstFactory >
    static FactoryMapPtr registerInstruction(StringFactoryMap& factoryMap, string& name)
    {
        FactoryMapPtr factoryMapPtr = factoryMap[name];
        if (factoryMapPtr == NULL)
        {
            /* Hmm, we haven't filled in this yet */
            /* We are registering a completely new instruction */
            BaseInstructionFactoryMap* pFactoryMap = static_cast<BaseInstructionFactoryMap*>
                (new SingleInstructionFactoryMap<TInstFactory>());

            factoryMapPtr = FactoryMapPtr(pFactoryMap);
            factoryMap[name] = factoryMapPtr;
        }

        // By now we should have filled in the factory map pointer
        assert(factoryMapPtr != NULL);

        return factoryMapPtr;
    }

    // TODO: Move into NativeInstruction.h
    static FactoryMapPtr registerNativeInstruction(string& name)
    {
        return registerInstruction<TypedInstructionFactory>(nativeInstructionMap, name);
    }

    // TODO: Move into PointerInstruction.h
    template < template <typename T, StandardTypeDescriptorOrdinal typeOrdinal> class TInstFactory >
    static FactoryMapPtr registerPointerInstruction(string& name)
    {
        return registerInstruction<TInstFactory>(pointerInstructionMap, name);
    }

    // TODO: Move into BoolInstruction.h
    static FactoryMapPtr registerBoolInstruction(string& name)
    {
        return registerInstruction<BoolInstructionFactory>(boolInstructionMap, name);
    }

    // TODO: Move into appropriate Instruction files
    template < template <typename T> class TInstruction, int args >
    static void registerNativeNativeInstruction(string name)
    {
        FactoryMapPtr factoryMapPtr = registerNativeInstruction(name);
        
        SingleInstructionFactoryMap<TypedInstructionFactory>* pSIFMap = 
            static_cast<SingleInstructionFactoryMap<TypedInstructionFactory>*>(factoryMapPtr.get());
        pSIFMap->template registerNativeOperands<TInstruction, args>();
    }

    template < template <typename T> class TInstruction, int args >
    static void registerIntegralNativeInstruction(string name)
    {
        FactoryMapPtr factoryMapPtr = registerNativeInstruction(name);
        
        SingleInstructionFactoryMap<TypedInstructionFactory>* pSIFMap = 
            static_cast<SingleInstructionFactoryMap<TypedInstructionFactory>*>(factoryMapPtr.get());
        pSIFMap->template registerIntegralNativeOperands<TInstruction, args>();
    }

    template < template <typename T> class TInstruction, int args >
    static void registerBoolNativeInstruction(string name)
    {
        FactoryMapPtr factoryMapPtr = registerBoolInstruction(name);

        SingleInstructionFactoryMap<BoolInstructionFactory>* pSIFMap = 
            static_cast<SingleInstructionFactoryMap<BoolInstructionFactory>*>(factoryMapPtr.get());
        pSIFMap->template registerNativeOperands<TInstruction, args>();
    }


    template < template <typename T> class TInstruction, int args >
    static void registerBoolPointerInstruction(string name)
    {
        FactoryMapPtr factoryMapPtr = registerBoolInstruction(name);

        SingleInstructionFactoryMap<BoolInstructionFactory>* pSIFMap = 
            static_cast<SingleInstructionFactoryMap<BoolInstructionFactory>*>(factoryMapPtr.get());
        pSIFMap->template registerPointerOperands<TInstruction, args>();
    }

    template < typename TInstruction, int args >
    static void registerBoolBoolInstruction(string name)
    {
        FactoryMapPtr factoryMapPtr = registerBoolInstruction(name);

        SingleInstructionFactoryMap<BoolInstructionFactory>* pSIFMap = 
            static_cast<SingleInstructionFactoryMap<BoolInstructionFactory>*>(factoryMapPtr.get());
        pSIFMap->template registerBoolOperands<TInstruction, args>();
    }

    template < template <typename T> class TInstruction, int args >
    static void registerPointerPointerInstruction(string name)
    {
        FactoryMapPtr factoryMapPtr = registerPointerInstruction<TypedInstructionFactory>(name);

        SingleInstructionFactoryMap<TypedInstructionFactory>* pSIFMap = 
            static_cast<SingleInstructionFactoryMap<TypedInstructionFactory>*>(factoryMapPtr.get());
        pSIFMap->template registerPointerOperands<TInstruction, args>();
    }

    template < template <typename T> class TInstruction, int args >
    static void registerPointerPointerOperandTInstruction(string name)
    {
        FactoryMapPtr factoryMapPtr = registerPointerInstruction<PointerPointerOperandTInstructionFactory>(name);

        SingleInstructionFactoryMap<PointerPointerOperandTInstructionFactory>* pSIFMap = 
            static_cast<SingleInstructionFactoryMap<PointerPointerOperandTInstructionFactory>*>(factoryMapPtr.get());
        pSIFMap->template registerPointerOperands<TInstruction, args>();
    }


    template < template <typename T> class TInstruction, int args >
    static void registerPointerPointerSizeTInstruction(string name)
    {
        FactoryMapPtr factoryMapPtr = registerPointerInstruction<PointerPointerSizeTInstructionFactory>(name);

        SingleInstructionFactoryMap<PointerPointerSizeTInstructionFactory>* pSIFMap = 
            static_cast<SingleInstructionFactoryMap<PointerPointerSizeTInstructionFactory>*>(factoryMapPtr.get());
        pSIFMap->template registerPointerOperands<TInstruction, args>();
    }

    template < template <typename T> class TInstruction, int args >
    static void registerIntegralPointerInstruction(string name)
    {
        FactoryMapPtr factoryMapPtr = registerPointerInstruction<IntegralPointerInstructionFactory>(name);
        
        SingleInstructionFactoryMap<IntegralPointerInstructionFactory>* pSIFMap = 
            static_cast<SingleInstructionFactoryMap<IntegralPointerInstructionFactory>*>(factoryMapPtr.get());
        pSIFMap->template registerPointerOperands<TInstruction, args>();
    }

    static ExtendedInstructionTable* registerExtendedInstructionTable()
    {
        return extendedInstructionMap.registerInstructionTable("CALL").get();
    }

    static ExtendedInstructionTable* getExtendedInstructionTable()
    {
        return extendedInstructionMap.getInstructionTable("CALL").get();
    }

    // TODO: Move registration of functions into individual Instruction
    static void registerInstructions()
    {
        // Register boolean instructions
        registerBoolBoolInstruction<BoolAnd,3>("AND");
        registerBoolBoolInstruction<BoolNot,2>("NOT");
        registerBoolBoolInstruction<BoolEqual,3>("EQ");
        registerBoolBoolInstruction<BoolNotEqual,3>("NE");
        registerBoolBoolInstruction<BoolGreater,3>("GT");
        registerBoolBoolInstruction<BoolLess,3>("LT");
        registerBoolBoolInstruction<BoolIsNull,2>("ISNULL");
        registerBoolBoolInstruction<BoolIsNotNull,2>("ISNOTNULL");
        registerBoolBoolInstruction<BoolToNull,1>("TONULL");

        registerBoolNativeInstruction<BoolNativeEqual,3>("EQ");
        registerBoolNativeInstruction<BoolNativeNotEqual,3>("NE");
        registerBoolNativeInstruction<BoolNativeGreater,3>("GT");
        registerBoolNativeInstruction<BoolNativeGreaterEqual,3>("GE");
        registerBoolNativeInstruction<BoolNativeLess,3>("LT");
        registerBoolNativeInstruction<BoolNativeLessEqual,3>("LE");
        registerBoolNativeInstruction<BoolNativeIsNull,2>("ISNULL");
        registerBoolNativeInstruction<BoolNativeIsNotNull,2>("ISNOTNULL");

        registerBoolPointerInstruction<BoolPointerEqual,3>("EQ");
        registerBoolPointerInstruction<BoolPointerNotEqual,3>("NE");
        registerBoolPointerInstruction<BoolPointerGreater,3>("GT");
        registerBoolPointerInstruction<BoolPointerGreaterEqual,3>("GE");
        registerBoolPointerInstruction<BoolPointerLess,3>("LT");
        registerBoolPointerInstruction<BoolPointerLessEqual,3>("LE");
        registerBoolPointerInstruction<BoolPointerIsNull,2>("ISNULL");
        registerBoolPointerInstruction<BoolPointerIsNotNull,2>("ISNOTNULL");

        // Register native  instructions
        registerNativeNativeInstruction<NativeAdd,3>("ADD");
        registerNativeNativeInstruction<NativeSub,3>("SUB");
        registerNativeNativeInstruction<NativeDiv,3>("DIV");
        registerNativeNativeInstruction<NativeMul,3>("MUL");
        registerNativeNativeInstruction<NativeNeg,2>("NEG");
        registerNativeNativeInstruction<NativeMove,2>("MOVE");
        registerNativeNativeInstruction<NativeToNull,1>("TONULL");

        registerIntegralNativeInstruction<IntegralNativeMod,3>("MOD");
        registerIntegralNativeInstruction<IntegralNativeAnd,3>("AND");
        registerIntegralNativeInstruction<IntegralNativeOr,3>("OR");
        registerIntegralNativeInstruction<IntegralNativeShiftLeft,3>("SHFL");
        registerIntegralNativeInstruction<IntegralNativeShiftRight,3>("SHFR");

        // Register pointer instructions
        registerIntegralPointerInstruction<PointerGetSize, 2>("GETS");
        registerIntegralPointerInstruction<PointerGetMaxSize, 2>("GETMS");

        registerPointerPointerSizeTInstruction<PointerPutSize, 2>("PUTS");

        registerPointerPointerOperandTInstruction<PointerAdd, 3>("ADD");
        registerPointerPointerOperandTInstruction<PointerSub, 3>("SUB");

        registerPointerPointerInstruction<PointerMove,2>("MOVE");
        registerPointerPointerInstruction<PointerToNull,1>("TONULL");

        // Register jump instructions
        jumpInstructionMap.registerInstruction<Jump,1>("JMP");
        jumpInstructionMap.registerInstruction<JumpTrue,2>("JMPT");
        jumpInstructionMap.registerInstruction<JumpFalse,2>("JMPF");
        jumpInstructionMap.registerInstruction<JumpNull,2>("JMPN");
        jumpInstructionMap.registerInstruction<JumpNotNull,2>("JMPNN");

        // TODO: Register return instruction

        // Register Extended instruction table
        registerExtendedInstructionTable();
    }

protected:
    static StringFactoryMap nativeInstructionMap;
    static StringFactoryMap boolInstructionMap;
    static StringFactoryMap pointerInstructionMap;
    static JumpInstructionFactoryMap jumpInstructionMap;
    static ExtendedInstructionFactoryMap extendedInstructionMap;
};

FENNEL_END_NAMESPACE

#endif

// End InstructionFactory.h
