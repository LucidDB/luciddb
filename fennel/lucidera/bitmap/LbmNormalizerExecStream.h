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

#ifndef Fennel_LbmNormalizerExecStream_Included
#define Fennel_LbmNormalizerExecStream_Included

#include "fennel/lucidera/bitmap/LbmByteSegment.h"
#include "fennel/lucidera/bitmap/LbmSeqSegmentReader.h"

#include "fennel/exec/ConduitExecStream.h"
#include "fennel/tuple/TupleAccessor.h"
#include "fennel/tuple/TupleData.h"

FENNEL_BEGIN_NAMESPACE

/**
 * This structure defines parameters for instantiating an
 * LbmNormalizerExecStream
 */
struct LbmNormalizerExecStreamParams : public ConduitExecStreamParams
{
    TupleProjection keyProj;
};

/**
 * The bitmap normalizer stream expands bitmap data to tuples. It can be
 * used to select data directly from a bitmap index. Bitmap data comes in
 * the form:<br>
 * [key1, key2, ..., keyn, srid, bitmap directory, bitmap data]<br>
 * <br>
 * It is converted to the form: <br>
 * [key1, key2, ..., keyn] (repeated for each bit)<br>
 * <br>
 * Rather than returning all keys, it is possible to return a projection
 * of the keys. Rather than tossing the RID (row id) it is possible to
 * return it as a column.
 *
 * @author John Pham
 * @version $Id$
 */
class FENNEL_LBM_EXPORT LbmNormalizerExecStream
    : public ConduitExecStream
{
    /**
     * Input descriptor, accessor, and data
     */
    TupleDescriptor keyBitmapDesc;
    TupleAccessor keyBitmapAccessor;
    TupleData keyBitmapData;

    /**
     * Projection of key columns from input stream
     */
    TupleProjection keyProj;
    TupleDescriptor keyDesc;
    TupleData keyData;

    /**
     * Reads input tuples
     */
    LbmSeqSegmentReader segmentReader;

    /**
     * Whether a tuple is ready to be produced
     */
    bool producePending;

    /**
     * Number of pending tuples in segment
     */
    uint nTuplesPending;

    /**
     * Last segment read
     */
    LbmByteSegment segment;

    /**
     * Reads a segment, returning EXECRC_YIELD on success
     */
    ExecStreamResult readSegment();

    /**
     * Produces a pending tuple to the output stream
     */
    bool produceTuple();

public:
    virtual void prepare(LbmNormalizerExecStreamParams const &params);
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
};

FENNEL_END_NAMESPACE

#endif

// End LbmNormalizerExecStream.h
