/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2003-2005 Disruptive Tech
// Copyright (C) 2005-2005 The Eigenbase Project
// Portions Copyright (C) 1999-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later Eigenbase-approved version.
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

#ifndef Fennel_CalcExecutionStream_Included
#define Fennel_CalcExecutionStream_Included

#include "fennel/xo/ExecutionStream.h"
#include "fennel/disruptivetech/calc/CalcCommon.h"
#include "fennel/tuple/TupleAccessor.h"
#include "fennel/tuple/TupleData.h"

FENNEL_BEGIN_NAMESPACE

// DEPRECATED
    
/**
 * CalcExecutionStreamParams defines parameters for instantiating a
 * CalcExecutionStream.
 */
struct CalcExecutionStreamParams
{
    std::string program;

    bool isFilter;
};

// REVIEW jvs 25-Mar-2005: CalcExecutionStream should inherit from
// ExecutionStream.  However, this requires virtual inheritance, which we can't
// use without dynamic_cast.  We should really find a fix for the
// JNI/dynamic_cast conflict.

/**
 * CalcExecutionStream reads tuples from a child stream and performs
 * calculations of SQL expressions.  For every input tuple which passes a
 * boolean filter expression, an output tuple is computed based on projection
 * expressions.
 */
class CalcExecutionStream : virtual public TraceSource
{
protected:
    /**
     * TupleDescriptor for input tuples.
     */
    TupleDescriptor inputDesc;
    
    /**
     * TupleDescriptor for output tuples.
     */
    TupleDescriptor outputDesc;
    
    /**
     * TupleAccessor for input tuples.
     */
    TupleAccessor inputAccessor;

    /**
     * TupleAccessor for output tuples.
     */
    TupleAccessor outputAccessor;

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
    
    virtual ~CalcExecutionStream();

    PConstBuffer readNextBuffer(
        ByteInputStream &inputStream,
        PConstBuffer &pInputBufferEnd);

    /**
     * Executes the calculator on a stream of tuples.
     *
     * @param inputStream input stream of tuples
     *
     * @param pOutputBuffer start of output buffer to receive
     * calculated results
     *
     * @param pOutputBufferEnd end of output buffer to receive
     * calculated results
     *
     * @return actual end of output data; may be less than
     * pOutputBufferEnd if end of input stream is reached
     */
    PBuffer execute(
        ByteInputStream &inputStream,
        PBuffer pOutputBuffer,
        PBuffer pOutputBufferEnd);
    
    void prepare(
        CalcExecutionStreamParams const &params,
        TupleDescriptor const &inputDesc,
        TupleDescriptor const &paramOutputDesc);
    
    TupleDescriptor const &getOutputDesc() const;
    void closeImpl();
};

FENNEL_END_NAMESPACE

#endif

// End CalcExecutionStream.h
