/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2007 The Eigenbase Project
// Copyright (C) 2007 SQLstream, Inc.
// Copyright (C) 2007 Dynamo BI Corporation
// Portions Copyright (C) 2004 John V. Sichi
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

#ifndef Fennel_NestedLoopJoinExecStream_Included
#define Fennel_NestedLoopJoinExecStream_Included

#include "fennel/exec/CartesianJoinExecStream.h"
#include "fennel/exec/DynamicParam.h"

FENNEL_BEGIN_NAMESPACE

/**
 * Dynamic parameter used to pass a join key value from the left input to
 * the right input
 */
struct FENNEL_EXEC_EXPORT NestedLoopJoinKey
{
    DynamicParamId dynamicParamId;
    uint leftAttributeOrdinal;

    NestedLoopJoinKey(DynamicParamId id, uint offset)
        : dynamicParamId(id),
        leftAttributeOrdinal(offset)
    {
    }
};

/**
 * NestedLoopJoinExecStream defines parameters for instantiating a
 * NestedLoopJoinExecStream.
 */
struct FENNEL_EXEC_EXPORT NestedLoopJoinExecStreamParams
    : public CartesianJoinExecStreamParams
{
    std::vector<NestedLoopJoinKey> leftJoinKeys;
};

/**
 * NestedLoopJoinExecStream performs a nested loop join between two inputs
 * by iterating over the first input once and opening and re-iterating over the
 * second input for each tuple from the first.  Join keys from the first
 * (left) input are passed to the second (right) input through dynamic
 * parameters.  An optional third input will do any pre-processing required
 * prior to executing the actual nested loop join.
 *
 * <p>
 * NOTE: The input that does pre-processing needs to be the third input because
 * it may need to be opened before other streams in the overall stream graph.
 * Due to the reverse topological sort order in which a stream graph is opened,
 * the last input into a stream is opened before any of the other inputs.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
class FENNEL_EXEC_EXPORT NestedLoopJoinExecStream
    : public CartesianJoinExecStream
{
    /**
     * True if pre-processing on third input completed
     */
    bool preProcessingDone;

    /**
     * Dynamic parameters corresponding to the left join keys
     */
    std::vector<NestedLoopJoinKey> leftJoinKeys;

    virtual bool checkNumInputs();

    /**
     * Creates temporary index used in nested loop join
     *
     * @return EXECRC_BUF_UNDERFLOW if request to create temporary index
     * hasn't been initiated yet
     */
    virtual ExecStreamResult preProcessRightInput();

    /**
     * Passes join keys from the left input to the right input using dynamic
     * parameters
     */
    virtual void processLeftInput();

public:
    // implement ExecStream
    virtual void prepare(NestedLoopJoinExecStreamParams const &params);
    virtual void open(bool restart);
};

FENNEL_END_NAMESPACE

#endif

// End NestedLoopJoinExecStream.h
