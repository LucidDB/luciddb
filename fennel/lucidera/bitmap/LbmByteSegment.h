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

#ifndef Fennel_LbmByteSegment_Included
#define Fennel_LbmByteSegment_Included

#include "fennel/common/ByteBuffer.h"
#include "fennel/lucidera/bitmap/LbmSegment.h"
#include "fennel/tuple/TupleData.h"

FENNEL_BEGIN_NAMESPACE

/**
 * This class encapsulates a single byte segment, as opposed to
 * a tuple which contains a set of them
 */
class LbmByteSegment
{
public:
    static const uint bitsInByte[];

    LbmByteNumber byteNum;
    PBuffer byteSeg;
    uint len;

    inline void reset()
    {
        byteNum = (LbmByteNumber) 0;
        byteSeg = NULL;
        len = 0;
    }

    /**
     * Returns the Srid of the starting byte (not the first rid set)
     */
    inline LcsRid getSrid() const
    {
        return byteNumberToRid(byteNum);
    }

    /**
     * Whether the segment has valid data
     */
    inline bool isNull() const
    {
        return byteSeg == NULL;
    }

    /**
     * Returns the end byte number
     */
    inline LbmByteNumber getEnd() const
    {
        return byteNum + len;
    }

    /**
     * Returns the end rid (one past last valid rid)
     */
    inline LcsRid getEndRid() const
    {
        return byteNumberToRid(getEnd());
    }

    /**
     * Ensures the segment begins with the requested byte number.
     * As a result, the beginning of the segment or even the entire
     * segment may be truncated.
     *
     * This function assumes bytes are reverse order.
     */
    void advanceToByteNum(LbmByteNumber newStartByteNum)
    {
        // ignore null values
        if (isNull()) {
            return;
        }

        // check if the segment will have valid data after truncation
        if (getEnd() <= newStartByteNum) {
            reset();
            return;
        }

        // advance the segment in place if required
        if (byteNum < newStartByteNum) {
            uint diff = opaqueToInt(newStartByteNum - byteNum);
            byteNum += diff;
            byteSeg -= diff;
            len -= diff;
        }
    }

    /**
     * Count the number of bits in the current byte segment
     */
    uint countBits()
    {
        return countBits(byteSeg - len + 1, len);
    }

    /**
     * Counts the number of rows represented by a bitmap datum.
     * An empty datum represents a single row.
     */
    static uint countBits(TupleDatum const &datum)
    {
        if (datum.pData == NULL || datum.cbData == 0) {
            return 1;
        }
        return countBits(datum.pData, datum.cbData);
    }

    /**
     * Counts the number of bits in an array
     */
    static uint countBits(PConstBuffer pBuf, uint len)
    {
        uint total = 0;
        for (uint i = 0; i < len; i++) {
            total += bitsInByte[pBuf[i]];
        }
        return total;
    }

    static void verifyBitsInByte()
    {
        for (uint i = 0; i < 256; i++) {
            uint slowBits = 0;
            for (uint j = 0; j < 8; j++) {
                if (i & (1 << j)) {
                    slowBits++;
                }
            }
            assert (slowBits == bitsInByte[i]);
        }
    }

    /**
     * Prints a byte segment.
     *
     * This function assumes bytes are in order.
     */
    void print(std::ostream &output)
    {
        output << std::dec << opaqueToInt(byteNum) << ".";
        output << std::dec << len << " (";
        for (uint i = 0; i < len; i++) {
            uint val = byteSeg[i];
            if (i > 0) {
                output << ",";
            }
            output << std::hex << val;
        }
        output << ")" << std::endl;
    }
};

FENNEL_END_NAMESPACE

#endif

// End LbmByteSegment.h
