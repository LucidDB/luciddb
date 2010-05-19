/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
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

#ifndef Fennel_MergeExecStream_Included
#define Fennel_MergeExecStream_Included

#include "fennel/exec/ConfluenceExecStream.h"

#include <vector>

FENNEL_BEGIN_NAMESPACE

/**
 * MergeExecStreamParams defines parameters for instantiating
 * a MergeExecStream.
 */
struct FENNEL_EXEC_EXPORT MergeExecStreamParams
    : public ConfluenceExecStreamParams
{
    /**
     * Whether the stream should execute in parallel mode.
     */
    bool isParallel;

    explicit MergeExecStreamParams();
};

/**
 * MergeExecStream produces the UNION ALL of any number of inputs.  All inputs
 * must have identical tuple shape; as a result, the merge can be done
 * buffer-wise rather than tuple-wise, with no copying.
 *
 *<p>
 *
 * In non-parallel mode, the implementation fully reads input i before moving
 * on to input i+1, starting with input 0.  In parallel mode, data from any
 * input is accepted as soon as it's ready.  Other possibilities for the future
 * are non-parallel round-robin (read one buffer from input 0, 1, 2, ... and
 * then back around again over and over) and sorted (merge tuples in sorted
 * order, like the top of a merge-sort; the latter would best be done in a new
 * SortedMergeExecStream since it requires tuple-wise logic).
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FENNEL_EXEC_EXPORT MergeExecStream
    : public ConfluenceExecStream
{
    /**
     * 0-based ordinal of next input from which to read.
     */
    uint iInput;

    /**
     * Number of inputs which have reached the EOS state; this stream's
     * execution is only done once all of them have.
     */
    uint nInputsEOS;

    /**
     * A bit vector indicating exactly which inputs have reached EOS.
     * The number of bits set here should always equal nInputsEOS.
     */
    std::vector<bool> inputEOS;

    /**
     * Whether this stream is executing in parallel mode.
     */
    bool isParallel;

    /**
     * End of input buffer currently being consumed.
     */
    PConstBuffer pLastConsumptionEnd;

public:
    // implement ExecStream
    virtual void prepare(MergeExecStreamParams const &params);
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
    virtual ExecStreamBufProvision getOutputBufProvision() const;
};

FENNEL_END_NAMESPACE

#endif

// End MergeExecStream.h
