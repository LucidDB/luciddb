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
*/

#include "fennel/common/CommonPreamble.h"
#include "fennel/disruptivetech/test/SqlStringBuffer.h"

using namespace fennel;
using namespace std;

#if !(defined LITTLEENDIAN || defined BIGENDIAN)
#error "endian not defined"
#endif

const uint SqlStringBuffer::mBumperChar = '@';
// use unaligned bumperlen for 1-byte-per-character strings
// to ensure generality
const int SqlStringBuffer::mBumperLen = 3;

SqlStringBuffer::
SqlStringBuffer(int storage,    // maximum size of string in characters
                int size,       // size of text, in characters, excluding padding
                int leftpad,    // pad left with this many characters
                int rightpad,   // pad right with this many chararacters
                uint text,      // fill text w/this
                uint pad,       // pad w/this
                int leftBumper, // In characters
                int rightBumper) : 
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
    uint c = start; // deal with overflow easier than char
    int toGen = mSize;
        
    string r;

    while(toGen) {
        r.push_back(static_cast<unsigned char>(c));
        toGen--;
        if (++c > upper) c = lower; 
    }
    mS.replace(mLeftBump + mLeftPad, mSize, r);
}


const uint SqlStringBufferUCS2::mBumperChar = '@';
// multi-byte strings will be aligned by tuple mechanism to
// at least short-word (2-byte) alignment.
const int SqlStringBufferUCS2::mBumperLen = 2;

SqlStringBufferUCS2::SqlStringBufferUCS2(SqlStringBuffer const &src) :
    mStorage(src.mStorage *2),
    mSize(src.mSize * 2),
    mLeftPad(src.mLeftPad * 2),
    mRightPad(src.mRightPad * 2),
    // force alignment when copying from potentially unaligned
    mLeftBump(src.mLeftBump & 1 ? src.mLeftBump + 1 : src.mLeftBump),
    mRightBump(src.mRightBump & 1 ? src.mRightBump + 1 : src.mRightBump),
    mTotal(src.mStorage * 2 + mLeftBump + mRightBump)
{
    mS.assign(mTotal, mBumperChar);

    init();

    char *srcP = src.mStr;
    char *dstP = mStr;
    int i = src.mStorage;
    while (i > 0) {
#ifdef LITTLEENDIAN
        *(dstP++) = *(srcP++);
        *(dstP++) = 0x00;
#else
        *(dstP++) = 0x00;
        *(dstP++) = *(srcP++);
#endif
        i--;
    }
}

SqlStringBufferUCS2::SqlStringBufferUCS2(SqlStringBuffer const &src,
                                         int leftBumper,
                                         int rightBumper) :
    mStorage(src.mStorage *2),
    mSize(src.mSize * 2),
    mLeftPad(src.mLeftPad * 2),
    mRightPad(src.mRightPad * 2),
    mLeftBump(leftBumper),
    mRightBump(rightBumper),
    mTotal(src.mStorage * 2 + leftBumper + rightBumper)
{
    // force alignment
    assert(!(mLeftBump & 1));
    assert(!(mRightBump & 1));

    mS.assign(mTotal, mBumperChar);

    init();

    char *srcP = src.mStr;
    char *dstP = mStr;
    int i = src.mStorage;
    while (i > 0) {
#ifdef LITTLEENDIAN
        *(dstP++) = *(srcP++);
        *(dstP++) = 0x00;
#else
        *(dstP++) = 0x00;
        *(dstP++) = *(srcP++);
#endif
        i--;
    }
}


void
SqlStringBufferUCS2::init()
{
    assert(mLeftBump > 0);
    assert(mRightBump > 0);
    assert(mStorage == mSize + mLeftPad + mRightPad);

    mLeftP = const_cast<char *>(mS.c_str()); // Too abusive of string()?
    mStr = mLeftP + mLeftBump;
    mStrPostPad = mStr + mLeftPad;
    mRightP = mStr + mStorage;
}


bool
SqlStringBufferUCS2::verify()
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
SqlStringBufferUCS2::randomize(uint start,
                               uint lower, 
                               uint upper)
{
    patternfill(start, lower, upper);

    vector <uint16_t>r;
    uint16_t tmp;
    int i = 0;
    while (i < mSize) {
#ifdef LITTLEENDIAN
        tmp = mStrPostPad[i+1] << 8 | mStrPostPad[i];
#else
        tmp = mStrPostPad[i] << 8 | mStrPostPad[i+1];
#endif
        r.push_back(tmp);
        i+=2;
    }


    random_shuffle(r.begin(), r.end());
    i = 0;
    vector<uint16_t>::iterator iter = r.begin();
    while (i < mSize) {
        tmp = *(iter++);
#ifdef LITTLEENDIAN
        mStrPostPad[i++] = tmp & 0xff;
        mStrPostPad[i++] = (tmp >> 8) & 0xff;
#else
        mStrPostPad[i++] = (tmp >> 8) & 0xff;
        mStrPostPad[i++] = tmp & 0xff;
#endif
    }
}

void
SqlStringBufferUCS2::patternfill(uint start,
                                 uint lower,
                                 uint upper)
{
    uint c = start;
    int i = 0;

    while(i < mSize) {
#ifdef LITTLEENDIAN
        mStrPostPad[i++] = c & 0xff;
        mStrPostPad[i++] = (c >> 8) & 0xff;
#else
        mStrPostPad[i++] = (c >> 8) & 0xff;
        mStrPostPad[i++] = c & 0xff;
#endif
        if (++c > upper) c = lower; 
    }
}

string
SqlStringBufferUCS2::dump()
{
    int i = 0;
    string ret;
    char buf[100];

    sprintf(buf, "DUMP: Storage=%d LeftBump=%d Size=%d RightBump=%d LP=%x Str=%x\n",
            mStorage, mLeftBump, mSize, mRightBump,
            (uint)mLeftP, (uint)mStr);
    ret += buf;

    i = 0;
    while (i < mTotal) {
        sprintf(buf, " %d:%x(%c)", i, mLeftP[i], (mLeftP[i] >= ' ' ? mLeftP[i] : '_'));
        ret += buf;
        i++;
    }
   
    ret += "\nLeft Bumper:\n";
    i = 0;
    while (i < mLeftBump) {
        sprintf(buf, "%d: 0x%x (%c)\n", i, mLeftP[i], mLeftP[i]);
        ret += buf;
        i++;
    }

    ret += "\nText:\n";
    i = 0;
    while(i < mStorage) {
        sprintf(buf, "%d: 0x%x (%c) 0x%x (%c)\n", i, 
                mStr[i], (mStr[i] >= ' ' ? mStr[i] : '_'),
                mStr[i+1], (mStr[i+1] >= ' ' ? mStr[i+1] : '_'));
        ret += buf;
        i+=2;
    }

    ret += "\nRight Bumper:\n";
    i = 0;
    while (i < mRightBump) {
        sprintf(buf, "%d: 0x%x (%c)\n", i, mStr[i + mStorage], mStr[i + mStorage]);
        ret += buf;
        i++;
    }

    return ret;
}

bool
SqlStringBufferUCS2::equal(SqlStringBufferUCS2 const &other)
{
    if (other.mTotal != mTotal) return false;
    if (memcmp(other.mLeftP, mLeftP, mTotal)) return false;
    return true;
}
