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

// Note: Must ifdef out unused code, as a c++ compiler will
// instantiate all paths and not attempt to do constant reduction or
// otherwise follow the logic of the program.  Without this technique,
// or something similar, the compiler would try to instantiate a
// BoolAnd(RegisterRef<float>) which trips other compile-time
// checks. (And is just generally a bad idea.

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
switch (foo) { // Feed/fool emacs auto-indent
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

