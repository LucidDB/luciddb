/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Copyright (C) 2005-2009 The Eigenbase Project
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

#ifndef Fennel_LcsBitOps_Included
#define Fennel_LcsBitOps_Included

#include "math.h"

FENNEL_BEGIN_NAMESPACE

/**
 * Sets nBits(1,2,4) starting at whatBits in *pB, from the LSB of v
 */
inline void setBits(uint8_t *pB, uint nBits, uint whatBits, uint16_t v)
{
    *pB |= ((v & ((1 << nBits) -1)) << whatBits);
}

/**
 * Copies nBits(1,2,4), starting at fromBits from byte B to uint16_t V,
 * starting at toBits
 */
inline void readBits(
    uint8_t b, uint nBits, uint fromBits, uint16_t *v, uint toBits)
{
    *v |= (((b & (((1 << nBits) -1) << fromBits)) >> fromBits) << toBits);
}

/**
 * Calculates the # of bits it takes to encode n different values
 * correct the number so that no more then 2 vectors (1,2,4,8,16) wide are
 * required.
 *
 * @return number of bits required
 */
inline uint calcWidth(uint n)
{
    uint w;

    // find out how many bits are needed to represent n
    w = 0;
    if (n > 0) {
        n--;
    }
    while (n) {
        w++;
        n >>= 1;
    }

    // round up the width to a value which can be
    // represented by two bit vectors (where each vector
    // has length 1, 2, 4, 8, or 16
    switch (w) {
    case  7:
        w = 8;
        break;
    case 11:
        w = 12;
        break;
    case 13:
    case 14:
    case 15:
        w = 16;
        break;
    default:
        break;
    }

    return w;
}

// WidthVec stores width in bits of bit vectors
const uint          WIDTH_VECTOR_SIZE = 4;
typedef uint8_t     WidthVec[WIDTH_VECTOR_SIZE];
typedef WidthVec    *PWidthVec;
typedef uint8_t     *PtrVec[WIDTH_VECTOR_SIZE];
typedef PtrVec      *PPtrVec;

typedef void (*PBitVecFuncPtr)(uint16_t *v, const PtrVec p, uint pos);
typedef void (*PByteBitVecFuncPtr)(uint8_t *v, const PtrVec p, uint pos);

/*
 * Creates a vector of widths required to represent l bits
 */
inline uint bitVecWidth(uint l, WidthVec w)
{
    uint8_t po2;
    uint iW;
    WidthVec t;
    int i, j;

    for (po2 = 1, iW = 0; l ; l >>= 1, po2 *= 2) {
        if (l & 0x1) {
            t[iW++] = po2;
        }
    }

    for (i = iW - 1, j = 0; i >= 0 ; w[j++] = t[i--]) {
    }
    return iW;
}

/**
 * Calculates the offsets of the bitVecs, returns the number of bytes
 * the bitVecs will take
 *
 * @param iCount number of entries
 *
 * @param iW size of the vectors
 *
 * @param w bitVec width vector
 *
 * @param p bit vector that is set
 *
 * @param pVec vector storage
 */
inline uint bitVecPtr(
    uint iCount, uint iW, WidthVec w, PtrVec p, uint8_t *pVec)
{
    uint i;
    uint8_t *t;

    for (i = 0, t = pVec ; i < iW ; i++) {
        p[i] = t;
        t += ((w[i] * iCount + 7) / 8);
    }

    return t - pVec;
}

/**
 * Returns size of bit vector
 *
 * @param nRow number of rows
 *
 * @param iW size of the vectors
 *
 * @param w bitVec width vector
 */
inline uint sizeofBitVec(uint nRow, uint iW, WidthVec w)
{
    uint t;
    uint i;

    for (i = 0, t = 0; i < iW; i++) {
        t += ((w[i] * nRow + 7) / 8);
    }
    return t;
}

/**
 * Reads bit vectors
 *
 * @param v destination of ref numbers
 *
 * @param iV # of bit vectors
 *
 * @param w bitVec width vector
 *
 * @param p bitVec offsets
 *
 * @param pos first row of interest
 *
 * @param count how many rows to read
 */
inline void readBitVecs(
    uint16_t *v, uint iV, const WidthVec w, const PtrVec p, uint pos,
    uint count)
{
    uint        i, j, k;
    uint        b;

    // clear the destination
    memset(v, 0, sizeof(uint16_t) * count);

    // read bit arrays
    for (i = 0, b = 0; i < iV; i++) {
        // w[i] contains the width of the bit vector
        // read append each vector bits into v[i], b is the bit position
        // of the next append
        switch (w[i]) {
        case 16:
            memcpy(v, p[i] + pos*2, sizeof(uint16_t) * count);
            break;

        case 8:
            for (j = 0; j < count; j++) {
                v[j] = (p[i] + pos)[j];
            }
            break;

        case 4:
            for (j = 0, k = pos*4;  j < count; j++, k += 4) {
                readBits(p[i][k / 8], 4, k % 8, &v[j], b);
            }
            break;

        case 2:
            for (j = 0, k = pos*2; j < count; j++, k += 2) {
                readBits(p[i][k / 8], 2, k % 8, &v[j], b);
            }
            break;

        case 1:
            for (j = 0, k = pos; j < count; j++, k++) {
                readBits(p[i][k / 8], 1, k % 8, &v[j], b);
            }
            break;

        default:
            assert(false);          // unsupported width
            break;
        }

        b += w[i];
    }
}

/**
 * Sets bit vector to 0
 *
 * @param v destination of ref numbers
 *
 * @param p bitVec offsets
 *
 * @param pos first row of interest
 */
inline void readBitVec0(uint16_t *v, const PtrVec p, uint pos)
{
    // ARG_USED(p);
    // ARG_USED(pos);
    *v = 0;
}

/**
 * Reads one row from a bit vector with 1 or 2 vectors only
 *
 * @param v destination of ref numbers
 *
 * @param p bitVec offsets
 *
 * @param pos first row of interest
 */
inline void readBitVec16(uint16_t *v, const PtrVec p, uint pos)
{
    *v = *(p[0] + pos*2);
}

/**
 * Reads an 8-bit vector
 *
 * @param v destination of ref numbers
 *
 * @param p bitVec offsets
 *
 * @param pos first row of interest
 */
inline void readBitVec8(uint16_t *v, const PtrVec p, uint pos)
{
    *v = *(p[0] + pos);
}

/**
 * Reads a 4-bit vector
 *
 * @param v destination of ref numbers
 *
 * @param p bitVec offsets
 *
 * @param pos first row of interest
 */
inline void readBitVec4(uint16_t *v, const PtrVec p, uint pos)
{
    // clear the destination
    *v = 0;
    readBits(p[0][pos / 2], 4, (pos * 4) % 8, v, 0);
}

/**
 * Reads a 2-bit vector
 *
 * @param v destination of ref numbers
 *
 * @param p bitVec offsets
 *
 * @param pos first row of interest
 */
inline void readBitVec2(uint16_t *v, const PtrVec p, uint pos)
{
    // clear the destination
    *v = 0;
    readBits(p[0][pos / 4], 2, (pos * 2) % 8, v, 0);
}

/**
 * Reads a 1-bit vector
 *
 * @param v destination of ref numbers
 *
 * @param p bitVec offsets
 *
 * @param pos first row of interest
 */
inline void readBitVec1(uint16_t *v, const PtrVec p, uint pos)
{
    // clear the destination
    *v = 0;
    readBits(p[0][pos / 8], 1, pos % 8, v, 0);
}

/**
 * Reads a 12-bit vector (8 bits + 4 bits)
 *
 * @param v destination of ref numbers
 *
 * @param p bitVec offsets
 *
 * @param pos first row of interest
 */
inline void readBitVec12(uint16_t *v, const PtrVec p, uint pos)
{
    *v = *(p[0] + pos);
    readBits(p[1][pos / 2], 4, (pos * 4) % 8, v, 8);
}

/**
 * Reads a 10-bit vector (8 bits + 2 bits)
 *
 * @param v destination of ref numbers
 *
 * @param p bitVec offsets
 *
 * @param pos first row of interest
 */
inline void readBitVec10(uint16_t *v, const PtrVec p, uint pos)
{
    *v = *(p[0] + pos);
    readBits(p[1][pos / 4], 2, (pos * 2) % 8, v, 8);
}

/**
 * Reads a 9-bit vector (8 bits + 1 bit)
 *
 * @param v destination of ref numbers
 *
 * @param p bitVec offsets
 *
 * @param pos first row of interest
 */
inline void readBitVec9(uint16_t *v, const PtrVec p, uint pos)
{
    *v = *(p[0] + pos);
    readBits(p[1][pos / 8], 1, pos % 8, v, 8);
}

/**
 * Reads a 6 bit vector (4 bits + 2 bits)
 *
 * @param v destination of ref numbers
 *
 * @param p bitVec offsets
 *
 * @param pos first row of interest
 */
inline void readBitVec6(uint16_t *v, const PtrVec p, uint pos)
{
    // clear the destination
    *v = 0;
    readBits(p[0][pos / 2], 4, (pos * 4) % 8, v, 0);
    readBits(p[1][pos / 4], 2, (pos * 2) % 8, v, 4);
}

/**
 * Reads a 5-bit vector (4 bits + 1 bit)
 *
 * @param v destination of ref numbers
 *
 * @param p bitVec offsets
 *
 * @param pos first row of interest
 */
inline void readBitVec5(uint16_t *v, const PtrVec p, uint pos)
{
    // clear the destination
    *v = 0;
    readBits(p[0][pos / 2], 4, (pos * 4) % 8, v, 0);
    readBits(p[1][pos / 8], 1, pos % 8, v, 4);
}

/**
 * Reads a 3-bit vector (2 bits + 1 bit)
 *
 * @param v destination of ref numbers
 *
 * @param p bitVec offsets
 *
 * @param pos first row of interest
 */
inline void readBitVec3(uint16_t *v, const PtrVec p, uint pos)
{
    // clear the destination
    *v = 0;
    readBits(p[0][pos / 4], 2, (pos * 2) % 8, v, 0);
    readBits(p[1][pos / 8], 1, pos % 8, v, 2);
}

FENNEL_END_NAMESPACE

#endif

// End LcsBitOps.h
