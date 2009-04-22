/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2004-2009 John V. Sichi
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
#include "fennel/exec/DynamicParam.h"
#include "fennel/tuple/TupleProjectionAccessor.h"

#include <boost/shared_array.hpp>

FENNEL_BEGIN_NAMESPACE

/**
 * Structure used to store information about dynamic parameters used by
 * the ReshapeExecStream
 */
struct FENNEL_EXEC_EXPORT ReshapeParameter
{
    /**
     * Dynamic parameter id
     */
    DynamicParamId dynamicParamId;

    /**
     * Offset within the input tuple that this parameter should be compared
     * against.  If no comparison needs to be done, the offset is set to MAXU.
     */
    uint compareOffset;

    /**
     * If true, append this parameter to the end of the output tuple, in the
     * order of the parameter ids
     */
    bool outputParam;

    ReshapeParameter(
        DynamicParamId dynamicParamIdInit,
        uint compareOffsetInit,
        bool outputParamInit) :
        dynamicParamId(dynamicParamIdInit),
        compareOffset(compareOffsetInit),
        outputParam(outputParamInit)
    {
    }
};

/**
 * ReshapeExecStreamParams defines parameters for ReshapeExecStream.
 */
struct FENNEL_EXEC_EXPORT ReshapeExecStreamParams
    : public ConduitExecStreamParams
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
     * against the comparison tuple and/or dynamic parameters.  The columns
     * being compared against dynamic parameters appear at the end of
     * the projection.
     */
    TupleProjection inputCompareProj;

    /**
     * Tuple projection of the output tuple from the input stream
     */
    TupleProjection outputProj;

    /**
     * Dynamic parameters used by this stream that are compared to the input
     * row and/or written to the output tuple
     */
    std::vector<ReshapeParameter> dynamicParameters;
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
 * In addition, input can be passed into the stream through dynamic parameters.
 * Those dynamic parameters can be compared as well as written to the output
 * stream.  Parameters written to the output stream are appended to the end
 * of each input stream tuple in the order in which the parameters are
 * specified.  They cannot be cast; so therefore their types must match the
 * expected output types.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
class FENNEL_EXEC_EXPORT ReshapeExecStream
    : public ConduitExecStream
{
    /**
     * Comparison operator
     */
    CompOperator compOp;

    /**
     * Dynamic parameters
     */
    std::vector<ReshapeParameter> dynamicParameters;

    /**
     * True if the dynamic parameters have been read.  They are only read
     * once when the stream is first executed
     */
    bool paramsRead;

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
     * Buffer used to store comparison tuple passed in as a parameter
     */
    boost::shared_array<FixedBuffer> compTupleBuffer;

    /**
     * Tuple descriptor for the comparison tuple
     */
    TupleDescriptor compTupleDesc;

    /**
     * Number of dynamic parameters that are to be compared
     */
    uint numCompDynParams;

    /**
     * Tuple projection used in the case when the comparison is non-equality
     */
    TupleProjection lastKey;

    /**
     * Tuple descriptor corresponding to the last key
     */
    TupleDescriptor lastKeyDesc;

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
     * Initializes tuple descriptors and data that will be used in
     * comparisons
     *
     * @param params stream parameters
     *
     * @param inputDesc descriptor of the input tuple stream
     *
     * @param inputAccessor accessor for the input tuple stream
     */
    void initCompareData(
        ReshapeExecStreamParams const &params,
        TupleDescriptor const &inputDesc,
        TupleAccessor const &inputAccessor);

    /**
     * Copies the comparison tuple from the input buffer and unmarshals it
     * into a specified TupleData
     *
     * @param tupleDesc TupleDescriptor corresponding to the TupleData
     *
     * @param tupleData TupleData that the tuple will be unmarshalled into
     *
     * @param tupleBuffer input tuple buffer
     */
    void copyCompareTuple(
        TupleDescriptor const &tupleDesc,
        TupleData &tupleData,
        PBuffer tupleBuffer);

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
     * Reads the dynamic parameters and setups the comparison and output
     * tuple datas to point to the parameter tuple datas.  Note that the
     * parameter values are accessed by reference, so it's expected that
     * the parameter values will remain fixed during stream execution.
     */
    void readDynamicParams();

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
