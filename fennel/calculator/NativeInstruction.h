/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/
#ifndef Fennel_NativeInstruction_Included
#define Fennel_NativeInstruction_Included

#include "boost/lexical_cast.hpp"
#include "fennel/calculator/Instruction.h"

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
template<> class NativeInstruction_NotANativeType<char> {};
template<> class NativeInstruction_NotANativeType<short> {};
template<> class NativeInstruction_NotANativeType<int> {};
template<> class NativeInstruction_NotANativeType<long> {};
template<> class NativeInstruction_NotANativeType<long long> {};
template<> class NativeInstruction_NotANativeType<unsigned char> {};
template<> class NativeInstruction_NotANativeType<unsigned short> {};
template<> class NativeInstruction_NotANativeType<unsigned int> {};
template<> class NativeInstruction_NotANativeType<unsigned long> {};
template<> class NativeInstruction_NotANativeType<unsigned long long> {};
template<> class NativeInstruction_NotANativeType<signed char> {};
template<> class NativeInstruction_NotANativeType<float> {};
template<> class NativeInstruction_NotANativeType<double> {};


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
    NativeInstruction(
        RegisterRef<TMPLT>* op1,
        StandardTypeDescriptorOrdinal nativeType)
        : mOp1(op1),
          mOp2(),
          mNativeType(nativeType)
    {
        assert(StandardTypeDescriptor::isNative(nativeType));
    }
    explicit
    NativeInstruction(
        RegisterRef<TMPLT>* op1,
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
        if (0) {
            NativeInstruction_NotANativeType<TMPLT>();
        }
    }

protected:
    RegisterRef<TMPLT>* mOp1;
    RegisterRef<TMPLT>* mOp2;
    StandardTypeDescriptorOrdinal mNativeType;
};

FENNEL_END_NAMESPACE

#endif

// End NativeInstruction.h

