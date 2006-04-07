/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2006-2006 LucidEra, Inc.
// Copyright (C) 2006-2006 The Eigenbase Project
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

#ifndef Fennel_LhxHashGenerator_Included
#define Fennel_LhxHashGenerator_Included

#include "fennel/common/CommonPreamble.h"
#include "fennel/tuple/TupleData.h"
#include "fennel/tuple/TupleDescriptor.h"

FENNEL_BEGIN_NAMESPACE

/**
 * A hash function generator class that uses different seed values for each
 * level, so that with the same key, different hash values are generated for
 * different levels.
 *
 * @author Rushan Chen
 * @version $Id$
 */
class LhxHashGenerator
{
private:
    /*
     * Level at which the hash value seed is generated.
     */
	uint level;

    /*
     * The seed value.
     */
	uint hashValueSeed;

    /*
     * The maigc table(same as the one used in LcsHash.cpp).
     */
	uint8_t *magicTable;

    /**
     * Compute hash value from value stored in a buffer.
     *
     * @param[out] hashValue
     * @param[in] pBuf buffer containing the value
     * @param[in] bufSize size of the buffer
     */
	void hashOneBuffer(uint &hashValue, PConstBuffer pBuf, uint bufSize);

    /**
     * Compute hash value, from both value and length information, for a
     * TupleDatum.
     *
     * @param[out] hashValue
     * @param[in] inputCol input TupleDatum
     */
	void hashOneColumn(uint &hashValue, TupleDatum const &inputCol);

public:
    /**
     * Initialize the generator. Different levels have different seed values.
     */
	void init(uint levelInit);

    /**
     * Get level information for this hash generator.
     */
	uint getLevel();

    /**
     * Compute hash value for a TupleData, on both value and length
     * information.
     *
     * @param[input] inputTuple
     * @param[input] keyProjection which fields in the input tuple are used
     *
     * @return the hash value
     */
	uint hash(TupleData const &inputTuple, 
              TupleProjection const &keyProjection);

    /**
     * Compute hash value for a TupleDatum, on both value and length
     * information.
     *
     * @param[input] inputCol
     *
     * @return the hash value
     */
	uint hash(TupleDatum const &inputCol);

    /**
     * Compute hash value from value stored in a buffer.
     *
     * @param[in] pBuf buffer containing the value
     * @param[in] bufSize size of the buffer
     *
     * @return the hash value
     */
	uint hash(PConstBuffer pBuf, uint bufSize);
};

inline uint LhxHashGenerator::getLevel()
{
    return level;
}

inline uint LhxHashGenerator::hash(TupleDatum const &inputCol)
{
    uint hashValue = hashValueSeed;
    hashOneColumn(hashValue, inputCol);
    return hashValue;
}

inline uint LhxHashGenerator::hash(PConstBuffer pBuf, uint bufSize)
{
    uint hashValue = hashValueSeed;
    hashOneBuffer(hashValue, pBuf, bufSize);
    return hashValue;
}

FENNEL_END_NAMESPACE

#endif

// End LhxHashGenerator.h
