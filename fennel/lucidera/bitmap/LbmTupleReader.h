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

#ifndef Fennel_LbmTupleReader_Included
#define Fennel_LbmTupleReader_Included

#include "fennel/exec/ExecStreamDefs.h"
#include "fennel/tuple/TupleData.h"
#include "fennel/lucidera/bitmap/LbmSegment.h"

FENNEL_BEGIN_NAMESPACE

typedef TupleData *PTupleData;

/**
 * LbmTupleReader is an interface for reading bitmap tuples
 *
 * @author John Pham
 * @version $Id$
 */
class LbmTupleReader
{
public:
    virtual ~LbmTupleReader();

    /**
     * Reads an input tuple. The tuple read remains valid until the next
     * call to this method.
     *
     * @return EXECRC_YIELD if read was successful, EXECRC_EOS if there
     * was no more data to be read, or EXECRC_BUF_UNDERFLOW if an input
     * stream buffer was exhausted
     */
    virtual ExecStreamResult read(PTupleData &pTupleData) = 0;
};

/**
 * LbmStreamTupleReader is a base class for reading bitmap tuples
 * from an input stream
 */
class LbmStreamTupleReader : public LbmTupleReader
{
    /**
     * Input stream accessor
     */
    SharedExecStreamBufAccessor pInAccessor;

    /**
     * Pointer to a tupledata containing the input bitmap tuple
     */
    PTupleData pInputTuple;

public:
    /**
     * Initializes reader to start reading bitmap tuples from a specified
     * input stream
     *
     * @param pInAccessorInit input stream accessor
     *
     * @param bitmapSegTupleInit tuple data for reading tuples
     */
    void init(
        SharedExecStreamBufAccessor &pInAccessorInit,
        TupleData &bitmapSegTupleInit);

    // implement LbmTupleReader
    ExecStreamResult read(PTupleData &pTupleData);
};

/**
 * LbmSingleTupleReader is a class satisyfing the bitmap tuple reader
 * interface for a single input tuple
 */
class LbmSingleTupleReader : public LbmTupleReader
{
    /**
     * Whether the segment reader has a tuple to return
     */
    bool hasTuple;

    /**
     * Pointer to a tuple data containing the input bitmap tuple
     */
    PTupleData pInputTuple;

public:
    /**
     * Initializes reader to return a specified tuple
     *
     * @param bitmapSegTupleInit tuple data for reading tuples
     */
    void init(TupleData &bitmapSegTupleInit);

    // implement LbmTupleReader
    ExecStreamResult read(PTupleData &pTupleData);
};

FENNEL_END_NAMESPACE

#endif

// End LbmTupleReader.h
