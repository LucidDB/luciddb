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
//
// InstructionRegisterSwitch.h
// Include this file if you intend to manipulate instructions directly, 
// otherwise include Calculator.h
//
*/

//! Note: Must ifdef out unused code, as a c++ compiler will
//! instantiate all paths and not attempt to do constant reduction or
//! otherwise follow the logic of the program.  Without this technique,
//! or something similar, the compiler would try to instantiate a
//! BoolAnd(RegisterRef<float>) which trips other compile-time
//! checks. (And is just generally a bad idea.

// Do not prevent multiple includes of this file. 

#ifdef Fennel_InstructionRegisterSwitch_NativeNotBool
#define Fennel_InstructionRegisterSwitch_Integral 1
#define Fennel_InstructionRegisterSwitch_Approx 1
#endif

#ifdef Fennel_InstructionRegisterSwitch_Array
#define Fennel_InstructionRegisterSwitch_Array_Text 1
#define Fennel_InstructionRegisterSwitch_Array_Binary 1
#endif

#if 0
{ // Feed/fool emacs auto-indent
#endif

#ifdef Fennel_InstructionRegisterSwitch_Integral
 case STANDARD_TYPE_INT_8:
     InstructionRegister::registerInstance<int8_t, INSTCLASS2>(type);
     break;
 case STANDARD_TYPE_UINT_8:
     InstructionRegister::registerInstance<uint8_t, INSTCLASS2>(type);
     break;
 case STANDARD_TYPE_INT_16:
     InstructionRegister::registerInstance<int16_t, INSTCLASS2>(type);
     break;
 case STANDARD_TYPE_UINT_16:
     InstructionRegister::registerInstance<uint16_t, INSTCLASS2>(type);
     break;
 case STANDARD_TYPE_INT_32:
     InstructionRegister::registerInstance<int32_t, INSTCLASS2>(type);
     break;
 case STANDARD_TYPE_UINT_32:
     InstructionRegister::registerInstance<uint32_t, INSTCLASS2>(type);
     break;
 case STANDARD_TYPE_INT_64:
     InstructionRegister::registerInstance<int64_t, INSTCLASS2>(type);
     break;
 case STANDARD_TYPE_UINT_64:
     InstructionRegister::registerInstance<uint64_t, INSTCLASS2>(type);
     break;
#endif


#ifdef Fennel_InstructionRegisterSwitch_Bool
 case STANDARD_TYPE_BOOL:
     InstructionRegister::registerInstance<bool, INSTCLASS2>(type);
     break;
#endif


#ifdef Fennel_InstructionRegisterSwitch_Approx
 case STANDARD_TYPE_REAL:
     InstructionRegister::registerInstance<float, INSTCLASS2>(type);
     break;
 case STANDARD_TYPE_DOUBLE:
     InstructionRegister::registerInstance<double, INSTCLASS2>(type);
     break;
#endif


#ifdef Fennel_InstructionRegisterSwitch_Array_Text
 case STANDARD_TYPE_CHAR:
     InstructionRegister::registerInstance<char*, INSTCLASS2>(type);
     break;
 case STANDARD_TYPE_VARCHAR:
     InstructionRegister::registerInstance<char*, INSTCLASS2>(type);
     break;
#endif


#ifdef Fennel_InstructionRegisterSwitch_Array_Binary
 case STANDARD_TYPE_BINARY:
     InstructionRegister::registerInstance<char*, INSTCLASS2>(type);
     break;
 case STANDARD_TYPE_VARBINARY:
     InstructionRegister::registerInstance<char*, INSTCLASS2>(type);
     break;
#endif


     // Be sure all defines are undefined as this file tends to be
     // included multiple times.
#undef Fennel_InstructionRegisterSwitch_NativeNotBool
#undef Fennel_InstructionRegisterSwitch_Array
#undef Fennel_InstructionRegisterSwitch_Integral
#undef Fennel_InstructionRegisterSwitch_Bool
#undef Fennel_InstructionRegisterSwitch_Approx
#undef Fennel_InstructionRegisterSwitch_Array_Text
#undef Fennel_InstructionRegisterSwitch_Array_Binary

#if 0
}   // Feed/fool emacs auto-indent
#endif


// End InstructionRegisterSwitch.h

