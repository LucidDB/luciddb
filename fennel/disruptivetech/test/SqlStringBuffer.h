/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2004-2005 Disruptive Tech
// Copyright (C) 2005-2005 The Eigenbase Project
//
// This program is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by the Free
// Software Foundation; either version 2 of the License, or (at your option)
// any later version approved by The Eigenbase Project.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
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
    SqlStringBuffer(int storage,      // maximum size of string in characters
                    int size,         // size of text, in characters, excluding padding
                    int leftpad = 0,  // pad left with this many characters
                    int rightpad = 0, // pad right with this many chararacters
                    uint text = 'x',  // fill text w/this
                    uint pad = ' ',   // pad w/this
                    // Try to use something unaligned below:
                    int leftBumper = mBumperLen,  // In characters
                    int rightBumper = mBumperLen);

    bool verify();
    void randomize(uint start = 'A',
                   uint lower = ' ', 
                   uint upper = '~');
    void
    patternfill(uint start = 'A',
                uint lower = ' ',
                uint upper = '~');
    

    char * mStr;           // valid string start. (includes left padding)
    char * mRightP;        // right bumper start. valid string ends 1 before here
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
    SqlStringBufferUCS2(SqlStringBuffer const &src,
                        int leftBumper,
                        int rightBumper);

    void init();
    bool verify();
    void randomize(uint start = 'A',
                   uint lower = ' ', 
                   uint upper = '~');
    void patternfill(uint start = 'A',
                     uint lower = ' ',
                     uint upper = '~');
    string dump();
    bool equal(SqlStringBufferUCS2 const &other);

    char * mStr;           // valid string start. (includes left padding)
    char * mStrPostPad;    // valid string start, skipping left padding
    char * mRightP;        // right bumper start. valid string ends 1 before here
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
