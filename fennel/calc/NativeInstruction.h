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
// NativeInstruction
//
// Instruction->Native
//
// Template for all native types
*/
#ifndef Fennel_NativeInstruction_Included
#define Fennel_NativeInstruction_Included

#include "boost/lexical_cast.hpp"
#include "fennel/calc/Instruction.h"

FENNEL_BEGIN_NAMESPACE

using boost::lexical_cast;

template<typename T> class RegisterRef;

//
// NativeInstruction_NotANativeType
//
// Force the use of a (non-pointer) native type.
// Note: You cannot use typedefs like int32_t here or the
// built-in names therefrom won't work. By using the built-in
// type name, you can support the built-in and typedefs
// built on top. Also, signed char is somehow different
// than char. This is not true for short, int, long or
// long long.
//
template <class T> class NativeInstruction_NotANativeType;
class NativeInstruction_NotANativeType<char> {} ;
class NativeInstruction_NotANativeType<short> {} ;
class NativeInstruction_NotANativeType<int> {} ;
class NativeInstruction_NotANativeType<long> {} ;
class NativeInstruction_NotANativeType<long long> {} ;
class NativeInstruction_NotANativeType<unsigned char> {} ;
class NativeInstruction_NotANativeType<unsigned short> {} ;
class NativeInstruction_NotANativeType<unsigned int> {} ;
class NativeInstruction_NotANativeType<unsigned long> {} ;
class NativeInstruction_NotANativeType<unsigned long long> {} ;
class NativeInstruction_NotANativeType<signed char> {} ;
class NativeInstruction_NotANativeType<float> {} ;
class NativeInstruction_NotANativeType<double> {} ;


template<typename TMPLT>
class NativeInstruction : public Instruction
{
public:
    explicit
    NativeInstruction(StandardTypeDescriptorOrdinal nativeType)
        : mOp1(),
          mOp2(), 
          mNativeType(nativeType)
    {
        assert(StandardTypeDescriptor::isNative(nativeType));
    }
    explicit
    NativeInstruction(RegisterRef<TMPLT>* op1,
                      StandardTypeDescriptorOrdinal nativeType)
        : mOp1(op1),
          mOp2(), 
          mNativeType(nativeType)
    {
        assert(StandardTypeDescriptor::isNative(nativeType));
    }
    explicit
    NativeInstruction(RegisterRef<TMPLT>* op1,
                      RegisterRef<TMPLT>* op2, 
                      StandardTypeDescriptorOrdinal nativeType)
        : mOp1(op1),
          mOp2(op2),
          mNativeType(nativeType)
    {
        assert(StandardTypeDescriptor::isNative(nativeType));
    }
    ~NativeInstruction() {
        // If (0) to reduce performance impact of template type checking
        if (0) NativeInstruction_NotANativeType<TMPLT>();
    }

protected:
    RegisterRef<TMPLT>* mOp1;
    RegisterRef<TMPLT>* mOp2;
    StandardTypeDescriptorOrdinal mNativeType;
};

FENNEL_END_NAMESPACE

#endif

// End NativeInstruction.h

