/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 2004-2004 Disruptive Tech
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
//
// PointerInstruction
//
// Instruction->Pointer
//
// Template for all native types
*/
#ifndef Fennel_PointerInstruction_Included
#define Fennel_PointerInstruction_Included

#include "fennel/disruptivetech/calc/Instruction.h"

FENNEL_BEGIN_NAMESPACE


//! PointerSizeT is the "size" and "maxsize" for array lengths
//!
//! This typedef must be compatible with the size TupleStorageByteLength
//! But, to prevent problems between say 32bit and 64 bit machines, this
//! type is defined with an explicit length so that one program can be written
//! for both architectures.
typedef uint32_t PointerSizeT;
#define POINTERSIZET_STANDARD_TYPE STANDARD_TYPE_UINT_32

//! Only integral type that can be used in pointer algebra.
//!
//! Would be nice if this was signed, but the presence of 
//! both PointerAdd and PointerSub probably make it OK for
//! this to be unsigned. It is probably more convenient for
//! a compiler to have this the same type as PointerOperandT
//! to avoid a type conversion. 
//!
//! This typedef must be compatible with the size TupleStorageByteLength
//! But, to prevent problems between say 32bit and 64 bit machines, this
//! type is defined with an explicit length so that one program can be written
//! for both architectures.
typedef uint32_t PointerOperandT;
#define POINTEROPERANDT_STANDARD_TYPE STANDARD_TYPE_UINT_32

class PointerInstruction : public Instruction
{
public:
    explicit
    PointerInstruction() { }
    ~PointerInstruction() { }

protected:
    static vector<StandardTypeDescriptorOrdinal>
    regDesc(uint sizetArgs1,
            uint ptrArgs,
            StandardTypeDescriptorOrdinal type,
            uint operandArgs2) {
        vector<StandardTypeDescriptorOrdinal>v(sizetArgs1,
                                               POINTERSIZET_STANDARD_TYPE);
        uint i;
        for (i = 0; i < ptrArgs; i++) {
            v.push_back(type);
        }
        for (i = 0; i < operandArgs2; i++) {
            v.push_back(POINTEROPERANDT_STANDARD_TYPE);
        }
        return v;
    }
};


//
// PointerInstruction_NotAPointerType
//
// Force the use of a (non-pointer) native type.
// Note: You cannot use typedefs like int32_t here or the
// built-in names thereof won't work. By using the built-in
// type name, you can support the built-in and typedefs
// built on top. Also, signed char is somehow different
// than char. This is not true for short, int, long or
// long long.
//
template <class T> class PointerInstruction_NotAPointerType;
class PointerInstruction_NotAPointerType<char *> {} ;
class PointerInstruction_NotAPointerType<short *> {} ;
class PointerInstruction_NotAPointerType<int *> {} ;
class PointerInstruction_NotAPointerType<long *> {} ;
class PointerInstruction_NotAPointerType<long long *> {} ;
class PointerInstruction_NotAPointerType<unsigned char *> {} ;
class PointerInstruction_NotAPointerType<unsigned short *> {} ;
class PointerInstruction_NotAPointerType<unsigned int *> {} ;
class PointerInstruction_NotAPointerType<unsigned long *> {} ;
class PointerInstruction_NotAPointerType<unsigned long long *> {} ;
class PointerInstruction_NotAPointerType<signed char *> {} ;
class PointerInstruction_NotAPointerType<float *> {} ;
class PointerInstruction_NotAPointerType<double *> {} ;

FENNEL_END_NAMESPACE

#endif

// End PointerInstruction.h

