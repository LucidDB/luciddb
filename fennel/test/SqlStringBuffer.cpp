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

#include "fennel/test/SqlStringBuffer.h"

using namespace fennel;
using namespace std;

const uint SqlStringBuffer::mBumperChar = '@';
const int SqlStringBuffer::mBumperLen = 3;


bool
SqlStringBuffer::verify()
{
    string verS(mTotal, mBumperChar);
    if (mS.compare(0, mLeftBump, verS, 0, mLeftBump)) {
        return false;
    }
    if (mS.compare(mLeftBump + mLeftPad + mSize + mRightPad,
                   mRightBump, verS, 0, mRightBump)) {
        return false;
    }
    return true;
}

void
SqlStringBuffer::randomize(uint start,
                           uint lower, 
                           uint upper)
{
    patternfill(start, lower, upper);
    string r(mStr, mSize);
    random_shuffle(r.begin(), r.end());
    mS.replace(mLeftBump + mLeftPad, mSize, r);
}

void
SqlStringBuffer::patternfill(uint start,
                             uint lower,
                             uint upper)
{
    uint c; // deal with overflow easier than char
    int toGen = mSize;
        
    string r;

    c = start;
    while(toGen) {
        r.push_back(static_cast<unsigned char>(c));
        toGen--;
        if (++c > upper) c = lower; 
    }
    mS.replace(mLeftBump + mLeftPad, mSize, r);
}

string
SqlStringBuffer::vectortostring(vector<char> &v)
{
    string s;
    vector<char>::iterator i = v.begin();
    while(i != v.end()) {
        s.push_back(*i);
        i++;
    }
    return s;
}
