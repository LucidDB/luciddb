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
#ifndef Fennel_IntegralPointerInstruction_Included
#define Fennel_IntegralPointerInstruction_Included

#include "fennel/calc/PointerInstruction.h"

FENNEL_BEGIN_NAMESPACE

//! PointerSizeT is the only valid result type defined for IntegralPointerInstruction.


template<typename PTR_TYPE>
class IntegralPointerInstruction : public PointerInstruction
{
public:
    explicit
    IntegralPointerInstruction(RegisterRef<PointerSizeT>* result,
                               RegisterRef<PTR_TYPE>* op1,
                               StandardTypeDescriptorOrdinal pointerType)
        : mResult(result),
          mOp1(op1),
          mPointerType(pointerType)
    { }
    ~IntegralPointerInstruction() { 
        // If (0) to reduce performance impact of template type checking
        if (0) PointerInstruction_NotAPointerType<PTR_TYPE>();
    }

protected:
    RegisterRef<PointerSizeT>* mResult;
    RegisterRef<PTR_TYPE>* mOp1;
    StandardTypeDescriptorOrdinal mPointerType;
};

template <typename PTR_TYPE>
class PointerGetSize : public IntegralPointerInstruction<PTR_TYPE>
{
public: 
    explicit
    PointerGetSize(RegisterRef<PointerSizeT>* result,
                   RegisterRef<PTR_TYPE>* op1, 
                   StandardTypeDescriptorOrdinal pointerType)
        : IntegralPointerInstruction<PTR_TYPE>(result, op1, pointerType)
    { }
    virtual
    ~PointerGetSize() { }

    virtual void exec(TProgramCounter& pc) const { 
        pc++;

        if (mOp1->isNull()) {
            mResult->toNull();
        } else {
            mResult->value(mOp1->length());  // get size, put value
        }
    }

    const char * longName() const { return "PointerGetSize"; }
    const char * shortName() const { return "GetS"; }
    void describe(string &out, bool values) const {
        RegisterRef<PTR_TYPE> mOp2; // create invalid regref
        describeHelper(out, values, longName(), shortName(), mResult, mOp1, &mOp2);
    }
};

template <typename PTR_TYPE>
class PointerGetMaxSize : public IntegralPointerInstruction<PTR_TYPE>
{
public: 
    explicit
    PointerGetMaxSize(RegisterRef<PointerSizeT>* result,
                      RegisterRef<PTR_TYPE>* op1, 
                      StandardTypeDescriptorOrdinal pointerType)
        : IntegralPointerInstruction<PTR_TYPE>(result, op1, pointerType)
    { }
    virtual
    ~PointerGetMaxSize() { }

    virtual void exec(TProgramCounter& pc) const { 
        pc++;

        if (mOp1->isNull()) {
            mResult->toNull();
        } else {
            mResult->value(mOp1->storage());  // get size, put value
        }
    }

    const char * longName() const { return "PointerGetMaxSize"; }
    const char * shortName() const { return "GetMS"; }
    void describe(string &out, bool values) const {
        RegisterRef<PTR_TYPE> mOp2; // create invalid regref
        describeHelper(out, values, longName(), shortName(), mResult, mOp1, &mOp2);
    }
};


FENNEL_END_NAMESPACE

#endif

// End IntegralPointerInstruction.h

