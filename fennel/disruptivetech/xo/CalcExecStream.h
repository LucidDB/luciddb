/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 2004-2004 Disruptive Tech
// Copyright (C) 1999-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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
#include "fennel/disruptivetech/calc/CalcCommon.h"
#include "fennel/tuple/TupleAccessor.h"
#include "fennel/tuple/TupleData.h"

FENNEL_BEGIN_NAMESPACE

/**
 * CalcExecStreamParams defines parameters for instantiating a
 * CalcExecStream.
 */
struct CalcExecStreamParams : public ExecStreamParams
{
    TupleDescriptor outputTupleDesc;
    
    std::string program;

    bool isFilter;
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
class CalcExecStream : public ConduitExecStream
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

public:
    virtual void prepare(CalcExecStreamParams const &params);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
};

FENNEL_END_NAMESPACE

#endif

// End CalcExecStream.h
