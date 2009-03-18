/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2004-2007 John V. Sichi
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

#ifndef Fennel_CartesianJoinExecStream_Included
#define Fennel_CartesianJoinExecStream_Included

#include "fennel/exec/ConfluenceExecStream.h"
#include "fennel/tuple/TupleAccessor.h"
#include "fennel/tuple/TupleData.h"

FENNEL_BEGIN_NAMESPACE

/**
 * CartesianJoinExecStreamParams defines parameters for instantiating a
 * CartesianJoinExecStream.
 *
 *<p>
 *
 * TODO:  Take a join filter?
 */
struct CartesianJoinExecStreamParams : public ConfluenceExecStreamParams
{
    bool leftOuter;
};

/**
 * CartesianJoinExecStream produces the Cartesian product of two input
 * streams.  The first input will be iterated only once, while the second
 * input will be opened and re-iterated for each tuple from the first input.
 * Optionally, additional processing can be applied on the records read from
 * the first input before iterating over the second input.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class CartesianJoinExecStream : public ConfluenceExecStream
{
protected:
    bool leftOuter;
    bool rightInputEmpty;
    TupleData outputData;
    SharedExecStreamBufAccessor pLeftBufAccessor;
    SharedExecStreamBufAccessor pRightBufAccessor;
    SharedExecStream pRightInput;
    uint nLeftAttributes;

    /**
     * @return true if the number of inputs to the stream is correct
     */
    virtual bool checkNumInputs();

    /**
     * Executes any pre-processing required on the right input
     *
     * @return EXECRC_YIELD if pre-processing successful
     */
    virtual ExecStreamResult preProcessRightInput();

    /**
     * Processes the left input after it has been read from the input stream
     */
    virtual void processLeftInput();

public:
    // implement ExecStream
    virtual void prepare(CartesianJoinExecStreamParams const &params);
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
};

FENNEL_END_NAMESPACE

#endif

// End CartesianJoinExecStream.h
