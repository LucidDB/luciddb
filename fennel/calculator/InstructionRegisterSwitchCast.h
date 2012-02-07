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

#if 0
void dummy()
{
#endif

    if (type1 == STANDARD_TYPE_INT_8 && type2 == STANDARD_TYPE_INT_8) {
        InstructionRegister::registerInstance2<int8_t, int8_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_INT_8 && type2 == STANDARD_TYPE_UINT_8) {
        InstructionRegister::registerInstance2<int8_t, uint8_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_INT_8 && type2 == STANDARD_TYPE_INT_16) {
        InstructionRegister::registerInstance2<int8_t, int16_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_INT_8 && type2 == STANDARD_TYPE_UINT_16) {
        InstructionRegister::registerInstance2<int8_t, uint16_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_INT_8 && type2 == STANDARD_TYPE_INT_32) {
        InstructionRegister::registerInstance2<int8_t, int32_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_INT_8 && type2 == STANDARD_TYPE_UINT_32) {
        InstructionRegister::registerInstance2<int8_t, uint32_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_INT_8 && type2 == STANDARD_TYPE_INT_64) {
        InstructionRegister::registerInstance2<int8_t, int64_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_INT_8 && type2 == STANDARD_TYPE_UINT_64) {
        InstructionRegister::registerInstance2<int8_t, uint64_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_INT_8 && type2 == STANDARD_TYPE_REAL) {
        InstructionRegister::registerInstance2<int8_t, float, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_INT_8 && type2 == STANDARD_TYPE_DOUBLE) {
        InstructionRegister::registerInstance2<int8_t, double, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_UINT_8 && type2 == STANDARD_TYPE_INT_8) {
        InstructionRegister::registerInstance2<uint8_t, int8_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_UINT_8 && type2 == STANDARD_TYPE_UINT_8) {
        InstructionRegister::registerInstance2<uint8_t, uint8_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_UINT_8 && type2 == STANDARD_TYPE_INT_16) {
        InstructionRegister::registerInstance2<uint8_t, int16_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_UINT_8
        && type2 == STANDARD_TYPE_UINT_16)
    {
        InstructionRegister::registerInstance2<uint8_t, uint16_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_UINT_8 && type2 == STANDARD_TYPE_INT_32) {
        InstructionRegister::registerInstance2<uint8_t, int32_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_UINT_8
        && type2 == STANDARD_TYPE_UINT_32)
    {
        InstructionRegister::registerInstance2<uint8_t, uint32_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_UINT_8 && type2 == STANDARD_TYPE_INT_64) {
        InstructionRegister::registerInstance2<uint8_t, int64_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_UINT_8
        && type2 == STANDARD_TYPE_UINT_64)
    {
        InstructionRegister::registerInstance2<uint8_t, uint64_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_UINT_8 && type2 == STANDARD_TYPE_REAL) {
        InstructionRegister::registerInstance2<uint8_t, float, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_UINT_8 && type2 == STANDARD_TYPE_DOUBLE) {
        InstructionRegister::registerInstance2<uint8_t, double, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_INT_16 && type2 == STANDARD_TYPE_INT_8) {
        InstructionRegister::registerInstance2<int16_t, int8_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_INT_16 && type2 == STANDARD_TYPE_UINT_8) {
        InstructionRegister::registerInstance2<int16_t, uint8_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_INT_16 && type2 == STANDARD_TYPE_INT_16) {
        InstructionRegister::registerInstance2<int16_t, int16_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_INT_16
        && type2 == STANDARD_TYPE_UINT_16)
    {
        InstructionRegister::registerInstance2<int16_t, uint16_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_INT_16 && type2 == STANDARD_TYPE_INT_32) {
        InstructionRegister::registerInstance2<int16_t, int32_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_INT_16
        && type2 == STANDARD_TYPE_UINT_32)
    {
        InstructionRegister::registerInstance2<int16_t, uint32_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_INT_16 && type2 == STANDARD_TYPE_INT_64) {
        InstructionRegister::registerInstance2<int16_t, int64_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_INT_16
        && type2 == STANDARD_TYPE_UINT_64)
    {
        InstructionRegister::registerInstance2<int16_t, uint64_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_INT_16 && type2 == STANDARD_TYPE_REAL) {
        InstructionRegister::registerInstance2<int16_t, float, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_INT_16 && type2 == STANDARD_TYPE_DOUBLE) {
        InstructionRegister::registerInstance2<int16_t, double, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_UINT_16 && type2 == STANDARD_TYPE_INT_8) {
        InstructionRegister::registerInstance2<uint16_t, int8_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_UINT_16
        && type2 == STANDARD_TYPE_UINT_8)
    {
        InstructionRegister::registerInstance2<uint16_t, uint8_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_UINT_16
        && type2 == STANDARD_TYPE_INT_16)
    {
        InstructionRegister::registerInstance2<uint16_t, int16_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_UINT_16
        && type2 == STANDARD_TYPE_UINT_16)
    {
        InstructionRegister::registerInstance2<uint16_t, uint16_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_UINT_16
        && type2 == STANDARD_TYPE_INT_32)
    {
        InstructionRegister::registerInstance2<uint16_t, int32_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_UINT_16
        && type2 == STANDARD_TYPE_UINT_32)
    {
        InstructionRegister::registerInstance2<uint16_t, uint32_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_UINT_16
        && type2 == STANDARD_TYPE_INT_64)
    {
        InstructionRegister::registerInstance2<uint16_t, int64_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_UINT_16
        && type2 == STANDARD_TYPE_UINT_64)
    {
        InstructionRegister::registerInstance2<uint16_t, uint64_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_UINT_16 && type2 == STANDARD_TYPE_REAL) {
        InstructionRegister::registerInstance2<uint16_t, float, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_UINT_16
        && type2 == STANDARD_TYPE_DOUBLE)
    {
        InstructionRegister::registerInstance2<uint16_t, double, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_INT_32 && type2 == STANDARD_TYPE_INT_8) {
        InstructionRegister::registerInstance2<int32_t, int8_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_INT_32 && type2 == STANDARD_TYPE_UINT_8) {
        InstructionRegister::registerInstance2<int32_t, uint8_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_INT_32 && type2 == STANDARD_TYPE_INT_16) {
        InstructionRegister::registerInstance2<int32_t, int16_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_INT_32
        && type2 == STANDARD_TYPE_UINT_16)
    {
        InstructionRegister::registerInstance2<int32_t, uint16_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_INT_32 && type2 == STANDARD_TYPE_INT_32) {
        InstructionRegister::registerInstance2<int32_t, int32_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_INT_32
        && type2 == STANDARD_TYPE_UINT_32)
    {
        InstructionRegister::registerInstance2<int32_t, uint32_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_INT_32 && type2 == STANDARD_TYPE_INT_64) {
        InstructionRegister::registerInstance2<int32_t, int64_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_INT_32
        && type2 == STANDARD_TYPE_UINT_64)
    {
        InstructionRegister::registerInstance2<int32_t, uint64_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_INT_32 && type2 == STANDARD_TYPE_REAL) {
        InstructionRegister::registerInstance2<int32_t, float, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_INT_32 && type2 == STANDARD_TYPE_DOUBLE) {
        InstructionRegister::registerInstance2<int32_t, double, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_UINT_32 && type2 == STANDARD_TYPE_INT_8) {
        InstructionRegister::registerInstance2<uint32_t, int8_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_UINT_32
        && type2 == STANDARD_TYPE_UINT_8)
    {
        InstructionRegister::registerInstance2<uint32_t, uint8_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_UINT_32
        && type2 == STANDARD_TYPE_INT_16)
    {
        InstructionRegister::registerInstance2<uint32_t, int16_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_UINT_32
        && type2 == STANDARD_TYPE_UINT_16)
    {
        InstructionRegister::registerInstance2<uint32_t, uint16_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_UINT_32
        && type2 == STANDARD_TYPE_INT_32)
    {
        InstructionRegister::registerInstance2<uint32_t, int32_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_UINT_32
        && type2 == STANDARD_TYPE_UINT_32)
    {
        InstructionRegister::registerInstance2<uint32_t, uint32_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_UINT_32
        && type2 == STANDARD_TYPE_INT_64)
    {
        InstructionRegister::registerInstance2<uint32_t, int64_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_UINT_32
        && type2 == STANDARD_TYPE_UINT_64)
    {
        InstructionRegister::registerInstance2<uint32_t, uint64_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_UINT_32 && type2 == STANDARD_TYPE_REAL) {
        InstructionRegister::registerInstance2<uint32_t, float, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_UINT_32
        && type2 == STANDARD_TYPE_DOUBLE)
    {
        InstructionRegister::registerInstance2<uint32_t, double, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_INT_64 && type2 == STANDARD_TYPE_INT_8) {
        InstructionRegister::registerInstance2<int64_t, int8_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_INT_64 && type2 == STANDARD_TYPE_UINT_8) {
        InstructionRegister::registerInstance2<int64_t, uint8_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_INT_64 && type2 == STANDARD_TYPE_INT_16) {
        InstructionRegister::registerInstance2<int64_t, int16_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_INT_64
        && type2 == STANDARD_TYPE_UINT_16)
    {
        InstructionRegister::registerInstance2<int64_t, uint16_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_INT_64 && type2 == STANDARD_TYPE_INT_32) {
        InstructionRegister::registerInstance2<int64_t, int32_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_INT_64
        && type2 == STANDARD_TYPE_UINT_32)
    {
        InstructionRegister::registerInstance2<int64_t, uint32_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_INT_64 && type2 == STANDARD_TYPE_INT_64) {
        InstructionRegister::registerInstance2<int64_t, int64_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_INT_64
        && type2 == STANDARD_TYPE_UINT_64)
    {
        InstructionRegister::registerInstance2<int64_t, uint64_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_INT_64 && type2 == STANDARD_TYPE_REAL) {
        InstructionRegister::registerInstance2<int64_t, float, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_INT_64 && type2 == STANDARD_TYPE_DOUBLE) {
        InstructionRegister::registerInstance2<int64_t, double, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_UINT_64 && type2 == STANDARD_TYPE_INT_8) {
        InstructionRegister::registerInstance2<uint64_t, int8_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_UINT_64
        && type2 == STANDARD_TYPE_UINT_8)
    {
        InstructionRegister::registerInstance2<uint64_t, uint8_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_UINT_64
        && type2 == STANDARD_TYPE_INT_16)
    {
        InstructionRegister::registerInstance2<uint64_t, int16_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_UINT_64
        && type2 == STANDARD_TYPE_UINT_16)
    {
        InstructionRegister::registerInstance2<uint64_t, uint16_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_UINT_64
        && type2 == STANDARD_TYPE_INT_32)
    {
        InstructionRegister::registerInstance2<uint64_t, int32_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_UINT_64
        && type2 == STANDARD_TYPE_UINT_32)
    {
        InstructionRegister::registerInstance2<uint64_t, uint32_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_UINT_64
        && type2 == STANDARD_TYPE_INT_64)
    {
        InstructionRegister::registerInstance2<uint64_t, int64_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_UINT_64
        && type2 == STANDARD_TYPE_UINT_64)
    {
        InstructionRegister::registerInstance2<uint64_t, uint64_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_UINT_64 && type2 == STANDARD_TYPE_REAL) {
        InstructionRegister::registerInstance2<uint64_t, float, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_UINT_64
        && type2 == STANDARD_TYPE_DOUBLE)
    {
        InstructionRegister::registerInstance2<uint64_t, double, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_REAL && type2 == STANDARD_TYPE_INT_8) {
        InstructionRegister::registerInstance2<float, int8_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_REAL && type2 == STANDARD_TYPE_UINT_8) {
        InstructionRegister::registerInstance2<float, uint8_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_REAL && type2 == STANDARD_TYPE_INT_16) {
        InstructionRegister::registerInstance2<float, int16_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_REAL && type2 == STANDARD_TYPE_UINT_16) {
        InstructionRegister::registerInstance2<float, uint16_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_REAL && type2 == STANDARD_TYPE_INT_32) {
        InstructionRegister::registerInstance2<float, int32_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_REAL && type2 == STANDARD_TYPE_UINT_32) {
        InstructionRegister::registerInstance2<float, uint32_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_REAL && type2 == STANDARD_TYPE_INT_64) {
        InstructionRegister::registerInstance2<float, int64_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_REAL && type2 == STANDARD_TYPE_UINT_64) {
        InstructionRegister::registerInstance2<float, uint64_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_REAL && type2 == STANDARD_TYPE_REAL) {
        InstructionRegister::registerInstance2<float, float, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_REAL && type2 == STANDARD_TYPE_DOUBLE) {
        InstructionRegister::registerInstance2<float, double, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_DOUBLE && type2 == STANDARD_TYPE_INT_8) {
        InstructionRegister::registerInstance2<double, int8_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_DOUBLE && type2 == STANDARD_TYPE_UINT_8) {
        InstructionRegister::registerInstance2<double, uint8_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_DOUBLE && type2 == STANDARD_TYPE_INT_16) {
        InstructionRegister::registerInstance2<double, int16_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_DOUBLE
        && type2 == STANDARD_TYPE_UINT_16)
    {
        InstructionRegister::registerInstance2<double, uint16_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_DOUBLE && type2 == STANDARD_TYPE_INT_32) {
        InstructionRegister::registerInstance2<double, int32_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_DOUBLE
        && type2 == STANDARD_TYPE_UINT_32)
    {
        InstructionRegister::registerInstance2<double, uint32_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_DOUBLE && type2 == STANDARD_TYPE_INT_64) {
        InstructionRegister::registerInstance2<double, int64_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_DOUBLE
        && type2 == STANDARD_TYPE_UINT_64)
    {
        InstructionRegister::registerInstance2<double, uint64_t, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_DOUBLE && type2 == STANDARD_TYPE_REAL) {
        InstructionRegister::registerInstance2<double, float, INSTCLASS2>(
            type1, type2);
    } else if (type1 == STANDARD_TYPE_DOUBLE && type2 == STANDARD_TYPE_DOUBLE) {
        InstructionRegister::registerInstance2<double, double, INSTCLASS2>(
            type1, type2);
    } else {
        throw std::logic_error("InstructionRegisterSwitchCast.h");
    }

#if 0
}
#endif

// End InstructionRegisterSwitchCast.h
