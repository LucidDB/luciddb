/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2004-2005 John V. Sichi
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

#ifndef Fennel_ReshapeExecStream_Included
#define Fennel_ReshapeExecStream_Included

#include "fennel/common/FemEnums.h"
#include "fennel/exec/ConduitExecStream.h"
#include "fennel/tuple/TupleProjectionAccessor.h"

#include <boost/shared_array.hpp>

FENNEL_BEGIN_NAMESPACE

/**
 * ReshapeExecStreamParams defines parameters for ReshapeExecStream.
 */
struct ReshapeExecStreamParams : public ConduitExecStreamParams
{
    /**
     * Comparison operator. Set to COMP_NOOP if no comparisons need to be done.
     * Note that NULLs are currently treated like any other value.  I.e., 
     * COMP_EQ will match NULL values.
     */
    CompOperator compOp;

    /**
     * Optional comparison tuple
     */
    boost::shared_array<FixedBuffer> pCompTupleBuffer;

    /**
     * Optional tuple projection representing the columns to be compared
     * against the comparison tuple
     */
    TupleProjection inputCompareProj;

    /**
     * Tuple projection of the output tuple from the input stream
     */
    TupleProjection outputProj;
};

/**
 * ReshapeExecStream takes its input stream, applies optional filtering
 * on the input, projects specified columns from the input stream, and performs
 * some very simple casting.  Namely,
 *
 * <pre><code>
 * char(x) -> varchar(y)
 * varchar(x) -> varchar(y)
 * type not null -> type null
 * type null -> type not null, iff there is a != null filter on the column
 * </code></pre>
 *
 * @author Zelaine Fong
 * @version $Id$
 */
class ReshapeExecStream : public ConduitExecStream
{
    /**
     * Comparison operator
     */
    CompOperator compOp;

    /**
     * TupleData corresponding to the comparison tuple that will be compared
     * with the input stream
     */
    TupleData paramCompareData;

    /**
     * Tuple projection accessor for the columns to be compared against the
     * comparison tuple
     */
    TupleProjectionAccessor inputCompareProjAccessor;

    /**
     * Tuple data corresponding to the input tuple data to be used in
     * comparisons
     */
    TupleData inputCompareData;

    /**
     * Tuple descriptor for the comparison tuple
     */
    TupleDescriptor compTupleDesc;

    /**
     * Tuple projection accessor for the output tuple
     */
    TupleProjectionAccessor outputProjAccessor;

    /**
     * Tuple descriptor from the input stream corresponding to the output
     * projection columns
     */
    TupleDescriptor inputOutputDesc;

    /**
     * Tuple data corresponding to the output row in the input stream
     */
    TupleData inputOutputData;

    /**
     * Output descriptor
     */
    TupleDescriptor outputDesc;

    /**
     * Tuple data for the output tuple
     */
    TupleData outputData;

    /**
     * True if the inputOutputTupleDesc does not match the output tuple
     * descriptor, and therefore simple casting is required
     */
    bool castRequired;

    /**
     * True if production of a tuple to the output stream is pending
     */
    bool producePending;

    /**
     * Verifies that in the case where simple casting is required, the
     * types of the input and output meet the restrictions on the type
     * of casting supported.
     *
     * @param compareProj projection representing the input columns that
     * have filters on them
     * @param inputTupleDesc tuple descriptor for the input columns
     * @param outputTupleDesc tuple descriptor for the output columns
     *
     * @return always returns true
     */
    bool checkCastTypes(
        const TupleProjection &compareProj,
        const TupleDescriptor &inputTupleDesc,
        const TupleDescriptor &outputTupleDesc);

    /**
     * Determines whether a column has a null filter on it
     *
     * @param compareProj projection representing the input columns that have
     * filters on them
     * @param colno column number being checked
     *
     * @return true if the column does have a null filter check on it
     */
    bool nullFilter(const TupleProjection &compareProj, uint colno);

    /**
     * Compares input data against the base comparison tuple.
     *
     * @return true if the data returns true for the comparison operation
     */
    bool compareInput();

    /**
     * Copies the columns from the input tuple to the output tuple, performing
     * truncation as part of casting, if needed
     */
    void castOutput();

public:
    // implement ExecStream
    virtual void prepare(ReshapeExecStreamParams const &params);
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
    virtual void closeImpl();
};

FENNEL_END_NAMESPACE

#endif

// End ReshapeExecStream.h
