/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2006-2009 LucidEra, Inc.
// Copyright (C) 2006-2009 The Eigenbase Project
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

#include "fennel/common/CommonPreamble.h"
#include "fennel/common/FennelExcn.h"
#include "fennel/lucidera/hashexe/LhxHashGenerator.h"
#include <sstream>

using namespace std;

FENNEL_BEGIN_CPPFILE("$Id$");

static uint8_t LhxHashGeneratorMagicTable[256] =
{
    1,    87,   49,   12,   176,  178,  102,  166,  121,  193,  6,    84,
    249,  230,  44,   163,  14,   197,  213,  181,  161,  85,   218,  80,
    64,   239,  24,   226,  236,  142,  38,   200,  110,  177,  104,  103,
    141,  253,  255,  50,   77,   101,  81,   18,   45,   96,   31,   222,
    25,   107,  190,  70,   86,   237,  240,  34,   72,   242,  20,   226,
    236,  142,  38,   235,  97,   234,  57,   22,   60,   250,  82,   175,
    208,  5,    127,  199,  111,  62,   135,  248,  174,  169,  211,  58,
    66,   154,  106,  195,  245,  171,  17,   187,  182,  179,  0,    243,
    132,  56,   148,  75,   128,  133,  158,  100,  130,  126,  91,   13,
    153,  246,  216,  219,  119,  68,   223,  78,   83,   88,   201,  99,
    122,  11,   92,   32,   136,  114,  52,   10,   138,  30,   48,   183,
    156,  35,   61,   26,   143,  74,   251,  94,   129,  162,  63,   152,
    170,  7,    115,  167,  241,  206,  3,    150,  55,   59,   151,  220,
    90,   53,   23,   131,  125,  173,  15,   238,  79,   95,   89,   16,
    105,  137,  225,  224,  217,  160,  37,   123,  118,  73,   2,    157,
    46,   116,  9,    145,  134,  228,  207,  212,  202,  215,  69,   229,
    27,   188,  67,   124,  168,  252,  42,   4,    29,   108,  21,   247,
    19,   205,  39,   203,  233,  40,   186,  147,  198,  192,  155,  33,
    164,  191,  98,   204,  165,  180,  117,  76,   140,  36,   210,  172,
    41,   54,   159,  8,    185,  232,  113,  196,  231,  47,   146,  120,
    51,   65,   28,   144,  254,  221,  93,   189,  194,  139,  112,  43,
    71,   109,  184,  209
};

void LhxHashGenerator::init(uint levelInit)
{
    /*
     * The seed computation can generate different seeds for only levels 0 - 63
     * Level 64 will have the same seed as level 0. So if recursive
     * partitioning goes to 64 level, no further partitioning is possible.
     */
    if (levelInit > 63) {
        ostringstream errMsg;
        errMsg << " Hash recursion level can not be deeper than 63";
        throw FennelExcn(errMsg.str());
    }

    level = levelInit;
    magicTable = LhxHashGeneratorMagicTable;

    uint base = level * 4;
    hashValueSeed
        = (uint8_t(base) << 24)
        | (uint8_t(base + 1) << 16)
        | (uint8_t(base + 2) << 8)
        | (uint8_t(base + 3));
}

// REVIEW jvs 25-Aug-2006: Awww, the fancy bit-twiddling from Broadbase is
// gone.  Boris L. will be so disappointed.  I guess it will have to be
// someone's science fair project to see if any of it was worthwhile.

void LhxHashGenerator::hashOneBuffer(uint &hashValue, PConstBuffer pBuf,
    uint bufSize)
{
    uint numValueBytes = sizeof(uint);
    uint8_t byteForNull = 0xff;

    if (pBuf == NULL) {
        bufSize = 1;
        pBuf = &byteForNull;
    }

    for (int count = 0; count < bufSize; count ++) {
        PBuffer pByte = (PBuffer) &hashValue;

        for (int i = 0; i < numValueBytes; i++) {
            *pByte = magicTable[(*pByte) ^ (*pBuf)];
            pByte ++;
        }

        pBuf ++;
    }
}

void LhxHashGenerator::hashOneColumn(
    uint &hashValue,
    TupleDatum const &inputCol,
    LhxHashTrim isVarChar)
{
    uint trimmedLength = inputCol.cbData;
    PConstBuffer pData = inputCol.pData;

    if (pData) {
        /*
         * Only hash to the trimmed value.
         */
        if (isVarChar == HASH_TRIM_VARCHAR) {
            PConstBuffer pChar = pData + trimmedLength - 1;
            while ((pChar >= pData) && (*pChar == ' ')) {
                --pChar;
            }
            trimmedLength = pChar - pData + 1;
        } else if (isVarChar == HASH_TRIM_UNICODE_VARCHAR) {
            PConstBuffer pChar = pData + trimmedLength - 2;
            while ((pChar >= pData)
                && (*reinterpret_cast<uint16_t const *>(pChar) == ' '))
            {
                pChar -= 2;
            }
            trimmedLength = pChar - pData + 2;
        }
    }

    // REVIEW jvs 25-Aug-2006:  Since the call below uses
    // sizeof(TupleStorageByteLength), shouldn't trimmedLength
    // be declared to match?

    /*
     * First hash the length
     * However, ignore length field if pData is NULL.
     */
    if (pData) {
        hashOneBuffer(
            hashValue,
            (PConstBuffer) &(trimmedLength),
            sizeof(TupleStorageByteLength));
    }

    /*
     * then hash the data buffer
     */
    hashOneBuffer(
        hashValue,
        pData,
        trimmedLength);
}

uint LhxHashGenerator::hash(
    TupleData const &inputTuple,
    TupleProjection const &keyProjection,
    vector<LhxHashTrim> const &isKeyColVarChar)
{
    uint keyLength = keyProjection.size();

    /*
     * get initial hash value of 4 bytes.
     */
    uint hashValue = hashValueSeed;

    for (int i = 0; i < keyLength; i++) {
        TupleDatum const &col = inputTuple[keyProjection[i]];
        hashOneColumn(hashValue, col, isKeyColVarChar[i]);
    }

    /*
     * Note: if key size == 0 then the hash value is the same as the seed value.
     * This is the case for single group aggregate.
     */
    return hashValue;
}

FENNEL_END_CPPFILE("$Id$");

// End LhxHashGenerator.cpp
