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

#ifndef Fennel_SqlStringBuffer_Included
#define Fennel_SqlStringBuffer_included

#include "fennel/common/TraceSource.h"

#include <boost/scoped_array.hpp>
#include <string>
#include <limits>
#include <vector>

using namespace fennel;
using namespace std;



// must support 0 length strings
class SqlStringBuffer
{
public:
    static const uint mBumperChar;
    static const int mBumperLen;

    explicit
    SqlStringBuffer(
        int storage,      // maximum size of string in characters
        int size,         // size of text, in characters, excluding padding
        int leftpad = 0,  // pad left with this many characters
        int rightpad = 0, // pad right with this many chararacters
        uint text = 'x',  // fill text w/this
        uint pad = ' ',   // pad w/this
        // Try to use something unaligned below:
        int leftBumper = mBumperLen,  // In characters
        int rightBumper = mBumperLen);

    bool verify();

    void randomize(
        uint start = 'A',
        uint lower = ' ',
        uint upper = '~');

    void patternfill(
        uint start = 'A',
        uint lower = ' ',
        uint upper = '~');


    char * mStr;           // valid string start. (includes left padding)
    char * mRightP;       // right bumper start. valid string ends 1 before here
    char * mLeftP;         // left bumper start.
    const int mStorage;    // maximum size (column width) of string
    const int mSize;       // size of string
    const int mLeftPad;    // length of left padding
    const int mRightPad;   // length of right padding
    const int mLeftBump;   // length of left bumper
    const int mRightBump;  // length of right bumper
    const int mTotal;      // size of string + bumpers
    string mS;

private:
};

class SqlStringBufferUCS2
{
public:
    static const uint mBumperChar;
    static const int mBumperLen;

    explicit
    SqlStringBufferUCS2(SqlStringBuffer const &src);

    explicit
    SqlStringBufferUCS2(
        SqlStringBuffer const &src,
        int leftBumper,
        int rightBumper);

    void init();
    bool verify();
    void randomize(
        uint start = 'A',
        uint lower = ' ',
        uint upper = '~');

    void patternfill(
        uint start = 'A',
        uint lower = ' ',
        uint upper = '~');

    string dump();
    bool equal(SqlStringBufferUCS2 const &other);

    char * mStr;           // valid string start. (includes left padding)
    char * mStrPostPad;    // valid string start, skipping left padding
    char * mRightP;       // right bumper start. valid string ends 1 before here
    char * mLeftP;         // left bumper start.
    const int mStorage;    // maximum size (column width) of string
    const int mSize;       // size of string
    const int mLeftPad;    // length of left padding
    const int mRightPad;   // length of right padding
    const int mLeftBump;   // length of left bumper
    const int mRightBump;  // length of right bumper
    const int mTotal;      // size of string + bumpers
    string mS;

private:
};

#endif

// End SqlStringBuffer.h
