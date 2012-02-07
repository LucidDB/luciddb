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

#include "fennel/common/CommonPreamble.h"
#include "fennel/calculator/SqlString.h"

FENNEL_BEGIN_NAMESPACE

int
SqlStrCat(
    char* dest,
    int destStorageBytes,
    int destLenBytes,
    char const * const str,
    int strLenBytes)
{
    if (destLenBytes + strLenBytes > destStorageBytes) {
        // SQL99 Part 2 Section 22.1 22-001 "String Data Right truncation"
        throw SqlState::instance().code22001();
    }

    memcpy(dest + destLenBytes, str, strLenBytes);
    return destLenBytes + strLenBytes;
}


int
SqlStrCat(
    char* dest,
    int destStorageBytes,
    char const * const str1,
    int str1LenBytes,
    char const * const str2,
    int str2LenBytes)
{
    if (str1LenBytes + str2LenBytes > destStorageBytes) {
        // SQL99 Part 2 Section 22.1 22-001
        // "String Data Right truncation"
        throw SqlState::instance().code22001();
    }

    memcpy(dest, str1, str1LenBytes);
    memcpy(dest + str1LenBytes, str2, str2LenBytes);
    return str1LenBytes + str2LenBytes;
}

int
SqlStrCmp_Bin(
    char const * const str1,
    int str1LenBytes,
    char const * const str2,
    int str2LenBytes)
{
    // First, check for differences in "common" length. If common length
    // are contains same values, declare the longer string "larger".
    int minLenBytes =
        str1LenBytes > str2LenBytes ? str2LenBytes : str1LenBytes;
    int memc = memcmp(str1, str2, minLenBytes);
    if (memc > 0) {
        // Normalize to -1, 0, 1
        return 1;
    } else if (memc < 0) {
        // Normalize to -1, 0, 1
        return -1;
    } else if (str1LenBytes == str2LenBytes) {
        // memc == 0
        // Equal length & contain same data -> equal
        return 0;
    } else if (str1LenBytes > str2LenBytes) {
        // Common contains same data, str1 is longer -> str1 > str2
        return 1;
    } else {
        // Common contains same data, str2 is longer -> str2 > str1
        return -1;
    }
}

int
SqlStrCpy_Var(
    char* dest,
    int destStorageBytes,
    char const * const str,
    int strLenBytes)
{
    if (strLenBytes > destStorageBytes) {
        // SQL99 Part 2 Section 22.1 22-001
        // "String Data Right truncation"
        throw SqlState::instance().code22001();
    }
    memcpy(dest, str, strLenBytes);
    return strLenBytes;
}

int
SqlStrLenBit(int strLenBytes)
{
    return 8 * strLenBytes;
}

int
SqlStrLenOct(int strLenBytes)
{
    return strLenBytes;
}


FENNEL_END_NAMESPACE

// End SqlString.cpp
