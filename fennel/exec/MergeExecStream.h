/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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

FENNEL_BEGIN_NAMESPACE

/**
 * MergeExecStreamParams defines parameters for instantiating
 * a MergeExecStream.
 */
struct MergeExecStreamParams : public ConfluenceExecStreamParams
{
};
    
/**
 * MergeExecStream produces the union of any number of inputs.  All inputs
 * must have identical tuple shape; as a result, the merge can be
 * done buffer-wise rather than tuple-wise, with no copying.
 *
 *<p>
 *
 * Current implementation is sequential: it fully reads input i before moving
 * on to input i+1, starting with input 0.  Future enhancements might include
 * round-robin (read one buffer from input 0, 1, 2, ... and then back around
 * again over and over), parallel (accept any input as soon as it's ready), and
 * sorted (merge tuples in sorted order, like the top of a merge-sort;
 * this would best be done in a new SortedMergeExecStream since it
 * requires tuple-wise logic).
 *
 * @author John V. Sichi
 * @version $Id$
 */
class MergeExecStream : public ConfluenceExecStream
{
    /**
     * 0-based ordinal of next input from which to read.
     */
    uint iInput;

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
