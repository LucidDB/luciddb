/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2004-2009 SQLstream, Inc.
// Copyright (C) 2009-2009 LucidEra, Inc.
// Portions Copyright (C) 1999-2009 John V. Sichi
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

#ifndef Fennel_CalcExecStream_Included
#define Fennel_CalcExecStream_Included

#include "fennel/exec/ConduitExecStream.h"
#include "fennel/calculator/CalcCommon.h"
#include "fennel/tuple/TupleAccessor.h"
#include "fennel/tuple/TupleData.h"

FENNEL_BEGIN_NAMESPACE

/**
 * CalcExecStreamParams defines parameters for instantiating a
 * CalcExecStream.
 */
struct CalcExecStreamParams : public ConduitExecStreamParams
{
    std::string program;
    bool isFilter;
    bool stopOnCalcError;
    CalcExecStreamParams()
        : program(), isFilter(false), stopOnCalcError(true) {}
};

/**
 * CalcExecStream reads tuples from a child stream and performs
 * calculations of SQL expressions.  For every input tuple which passes a
 * boolean filter expression, an output tuple is computed based on projection
 * expressions.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FENNEL_CALCULATOR_EXPORT CalcExecStream
    : public ConduitExecStream
{
    /**
     * TupleDescriptor for input tuples.
     */
    TupleDescriptor inputDesc;

    /**
     * TupleData for input tuples.
     */
    TupleData inputData;

    /**
     * TupleData for output tuples.
     */
    TupleData outputData;

    /**
     * The Calculator object which does the real work.
     */
    SharedCalculator pCalc;


    /**
     * If this stream filters tuples, pFilterDatum refers to the boolean
     * TupleDatum containing the filter status; otherwise, pFilterDatum is
     * NULL, and the result cardinality is always equal to the input
     * cardinality.
     */
    TupleDatum const *pFilterDatum;

    /** when true, execute() aborts on a Calculator error;
     * when false, it skips the offending row.
     */
    bool stopOnCalcError;

public:
    virtual void prepare(CalcExecStreamParams const &params);
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
};

FENNEL_END_NAMESPACE

#endif

// End CalcExecStream.h
