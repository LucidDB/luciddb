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
*/

#ifndef Fennel_SqlStringBuffer_Included
#define Fennel_SqlStringBuffer_included

#include "fennel/common/CommonPreamble.h"
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

    SqlStringBuffer(int storage,         // maximum size (column width) of string
                    int size,            // size of text, excluding padding
                    int leftpad = 0,     // pad left with this many chars
                    int rightpad = 0,    // pad right with this many chars
                    uint text = 'x',     // fill text w/this
                    uint pad = ' ',      // pad w/this
                    int leftBumper = mBumperLen,  // try to pick something unaligned...
                    int rightBumper = mBumperLen) :
        mStorage(storage),
        mSize(size),
        mLeftPad(leftpad),
        mRightPad(rightpad),
        mLeftBump(leftBumper),
        mRightBump(rightBumper),
        mTotal(storage + leftBumper + rightBumper),
        mS(mTotal, mBumperChar)
    {
        assert(leftBumper > 0);
        assert(rightBumper > 0);
        assert(storage == size + leftpad + rightpad);

        mLeftP = const_cast<char *>(mS.c_str()); // Too abusive of string()?
        mStr = mLeftP + mLeftBump;
        mRightP = mStr + mStorage;

        string padS(mStorage, pad);
        string textS(size, text);

        mS.replace(mLeftBump, mStorage, padS, 0, mStorage); // pad all first
        mS.replace(mLeftBump + mLeftPad, mSize, textS, 0, mSize);
    }

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
    string vectortostring(vector<char> &v);
};

class SqlStringBufferUCS2 : public SqlStringBuffer
{
    SqlStringBufferUCS2(int storage,         // maximum size (column width) of string
                        int size,            // size of text, excluding padding
                        int leftpad = 0,     // pad left with this many chars
                        int rightpad = 0,    // pad right with this many chars
                        uint text = 'x',     // fill text w/this
                        uint pad = ' ',      // pad w/this
                        int leftBumper = mBumperLen,  // try to pick something unaligned...
                        int rightBumper = mBumperLen) :
        SqlStringBuffer(storage >> 1,
                        size >> 1,
                        leftpad >> 1,
                        rightpad >> 1,
                        leftBumper >> 1,   // keep things even
                        rightBumper >> 1)  // keep things even
    {
    }
                        
};


#endif
