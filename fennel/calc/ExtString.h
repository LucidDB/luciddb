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
// Test Calculator object directly by instantiating instruction objects,
// creating programs, running them, and checking the register set values.
*/

#ifndef Fennel_ExtString_Included
#define Fennel_ExtString_Included

#include "fennel/common/CommonPreamble.h"
#include "fennel/calc/RegisterReference.h"
#include "fennel/calc/ExtendedInstruction.h"

FENNEL_BEGIN_NAMESPACE

//! Strcat. Ascii. dest = dest || str.
//!
//! Sets cbData to length for char as well as varchar. 
//!
//! If calling with fixed: Must call StrCatA3() first to have
//! length set correctly. Only then subsequent calls to strCatA2() are
//! possible.  If concatenating multiple strings, strCatA2 will honor
//! the intermediate length. 
//! After final call to strCatA2(), length should equal
//! width, to maintain fixed width string length == width. 
//! Behavior may be undefined if, after Calculator exits, length != width.
void
strCatA2(Calculator *pCalc,
         RegisterRef<char*> *result,
         RegisterRef<char*> *str1);

//! Strcat. Ascii. dest = str1 || str2.
//!
//! Sets cbData to length for char as well as varchar.
//! After final call to strCatA3(), length should equal
//! width, to maintain fixed width string length == width. 
//! Behavior may be undefined if, after Calculator exits, length != width.
//! If concatenating multiple strings, strCatA2 will honor
//! the intermediate length set by strCatAF3()
void
strCatAF3(Calculator *pCalc,
          RegisterRef<char*> *result,
          RegisterRef<char*> *str1,
          RegisterRef<char*> *str2);

//! StrCmp. Ascii.
void
strCmpA(Calculator *pCalc,
        RegisterRef<int32_t> *result,   
        RegisterRef<char*> *str1,
        RegisterRef<char*> *str2);

//! StrLen in Bits. Ascii.
void
strLenBitA(Calculator *pCalc,
           RegisterRef<int32_t> *result,   
           RegisterRef<char*> *str);

//! StrLen in Characters. Ascii.
void
strLenCharA(Calculator *pCalc,
            RegisterRef<int32_t> *result,   
            RegisterRef<char*> *str);

//! StrLen in Octets. Ascii.
void
strLenOctA(Calculator *pCalc,
           RegisterRef<int32_t> *result,   
           RegisterRef<char*> *str);

//! Overlay. Length unspecified -- to end. Ascii.
void
strOverlayA4(Calculator *pCalc,
             RegisterRef<char*> *result,
             RegisterRef<char*> *str,
             RegisterRef<char*> *overlay,
             RegisterRef<int32_t> *start);  

//! Overlay. Length specified. Ascii.
void
strOverlayA5(Calculator *pCalc,
             RegisterRef<char*> *result,
             RegisterRef<char*> *str,
             RegisterRef<char*> *overlay,
             RegisterRef<int32_t> *start,   
             RegisterRef<int32_t> *len);    


//! Position of find string in str string. Ascii.
void
strPosA(Calculator *pCalc,
        RegisterRef<int32_t>* result,
        RegisterRef<char*>* str,
        RegisterRef<char*>* find);

//! SubString. By reference. Length not specified -- to end. Ascii.
void
strSubStringA3(Calculator *pCalc,
               RegisterRef<char*>* result,
               RegisterRef<char*>* str,
               RegisterRef<int32_t>* start);

//! SubString. By Reference. Length specified. Ascii.
void
strSubStringA4(Calculator *pCalc,
               RegisterRef<char*>* result,
               RegisterRef<char*>* str,
               RegisterRef<int32_t>* start,
               RegisterRef<int32_t>* len);

//! ToLower. Ascii.
void
strToLowerA(Calculator *pCalc,
            RegisterRef<char*>* result,
            RegisterRef<char*>* str);


//! ToUpper. Ascii.
void
strToUpperA(Calculator *pCalc,
            RegisterRef<char*>* result,
            RegisterRef<char*>* str);


//! Trim. By Reference. Ascii.
void
strTrimA(Calculator *pCalc,
         RegisterRef<char*>* result,
         RegisterRef<char*>* str,
         RegisterRef<int32_t>* trimLeft,
         RegisterRef<int32_t>* trimRight);
        

void
strRegister();


FENNEL_END_NAMESPACE

#endif

// End SqlString.h
