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
#ifndef Fennel_PointerIntegralInstruction_Included
#define Fennel_PointerIntegralInstruction_Included

#include "fennel/calc/PointerInstruction.h"

FENNEL_BEGIN_NAMESPACE


template<typename PTR_TYPE>
class PointerIntegralInstruction : public PointerInstruction
{
public:
    explicit
    PointerIntegralInstruction(RegisterRef<PTR_TYPE>* result,
                               RegisterRef<PointerSizeT>* op1,
                               StandardTypeDescriptorOrdinal pointerType)
        : mResult(result),
          mOp1(op1),
          mPointerType(pointerType)
    { }
    ~PointerIntegralInstruction() { 
        // If (0) to reduce performance impact of template type checking
        if (0) PointerInstruction_NotAPointerType<PTR_TYPE>();
    }

protected:
    RegisterRef<PTR_TYPE>* mResult;
    RegisterRef<PointerSizeT>* mOp1;
    StandardTypeDescriptorOrdinal mPointerType;
};

// TODO: Rename to PointerPutLength to be consistant with RegisterReference
// TODO: accessors.
template <typename PTR_TYPE>
class PointerPutSize : public PointerIntegralInstruction<PTR_TYPE>
{
public: 
    explicit
    PointerPutSize(RegisterRef<PTR_TYPE>* result,
                   RegisterRef<PointerSizeT>* op1, 
                   StandardTypeDescriptorOrdinal pointerType)
        : PointerIntegralInstruction<PTR_TYPE>(result, op1, pointerType)
    { }
    virtual
    ~PointerPutSize() { }

    virtual void exec(TProgramCounter& pc) const { 
        pc++;

        if (mOp1->isNull()) {
            mResult->toNull();
            mResult->length(0);
        } else {
            mResult->length(mOp1->value());   // get value, put size
        }
    }

    const char * longName() const { return "PointerPutSize"; }
    const char * shortName() const { return "PutS"; }
    void describe(string &out, bool values) const {
        RegisterRef<PTR_TYPE> mOp2; // create invalid regref
        describeHelper(out, values, longName(), shortName(), mResult, mOp1, &mOp2);
    }
};

//! Note: There cannot be a PointerIntegralPutStorage() as cbStorage,
//! the maximum size, is always read-only.

FENNEL_END_NAMESPACE

#endif

// End PointerIntegralInstruction.h

