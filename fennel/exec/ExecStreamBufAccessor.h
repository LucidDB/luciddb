/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 2004-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

#ifndef Fennel_ExecStreamBufAccessor_Included
#define Fennel_ExecStreamBufAccessor_Included

#include "fennel/exec/ExecStreamDefs.h"
#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/tuple/TupleFormat.h"
#include "fennel/tuple/TupleAccessor.h"
#include "fennel/tuple/TupleOverflowExcn.h"

#include <boost/utility.hpp>

FENNEL_BEGIN_NAMESPACE

/**
 * ExecStreamBufAccessor defines access to the buffer memory via
 * which ExecStreams transfer data.  For more information, see SchedulerDesign.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class ExecStreamBufAccessor : public boost::noncopyable
{
    PBuffer pBufStart;

    PBuffer pBufEnd;
    
    PBuffer pProducer;
    
    PBuffer pConsumer;
    
    ExecStreamBufProvision provision;

    ExecStreamBufState state;

    TupleDescriptor tupleDesc;
    
    TupleFormat tupleFormat;

    TupleAccessor tupleProductionAccessor;

    TupleAccessor tupleConsumptionAccessor;

    uint cbBuffer;
    
public:
    inline explicit ExecStreamBufAccessor();
    
    virtual ~ExecStreamBufAccessor()
    {
    }

    /**
     * Initializes the buffer provision mode of this accessor.
     *
     * @param provision new provision mode
     */
    inline void setProvision(ExecStreamBufProvision provision);

    /**
     * Initializes the shape for tuples to be accessed.
     *
     * @param tupleDesc logical descriptor for tuples
     *
     * @param tupleFormat physical layout for tuples
     */
    inline void setTupleShape(
        TupleDescriptor const &tupleDesc,
        TupleFormat tupleFormat = TUPLE_FORMAT_STANDARD);

    /**
     * Initializes this accessor to the idle unprovided state.
     */
    inline void clear();
    
    /**
     * Provides empty buffer space into which producers will write data;
     * called by consumer.
     *
     * @param pStart first byte of empty buffer
     *
     * @param pEnd end of empty buffer
     *
     * @param reusable whether the buffer can be reused after it is consumed
     */
    inline void provideBufferForProduction(
        PBuffer pStart,
        PBuffer pEnd,
        bool reusable);

    /**
     * Provides a buffer full of data which consumers will read; called
     * by producer.
     *
     * @param pStart first byte of data buffer
     *
     * @param pEnd end of data buffer
     */
    inline void provideBufferForConsumption(
        PConstBuffer pStart,
        PConstBuffer pEnd);

    /**
     * Requests production of data; called by consumer when it
     * exhausts existing data and needs more.
     */
    inline void requestProduction();

    /**
     * Marks end of stream; called by producer when it knows it will
     * not be producing any more data.
     */
    inline void markEOS();

    /**
     * Indicates amount of data that has been written into buffer;
     * called by producer.  The usual sequence is
     *
     *<ol>
     *<li>use getProductionStart() and getProductionEnd() to
     * determine the available buffer area
     *<li>write data into the buffer contiguously starting
     * from getProductionStart()
     *<li>call produceData
     *</ol>
     *
     * @param pEnd end of data produced (between getProductionStart()
     * and getProductionEnd())
     */
    inline void produceData(PBuffer pEnd);

    /**
     * Indicates amount of data that has been read from buffer;
     * called by consumer.  The usual sequence is
     *
     *<ol>
     *<li>use getConsumptionStart() and getConsumptionEnd() to
     * determine the available data
     *<li>read data out of the buffer contiguously starting
     * from getConsumptionStart()
     *<li>call consumeData
     *</ol>
     *
     * @param pEnd end of data consumed (between getConsumptionStart()
     * and getConsumptionEnd())
     */
    inline void consumeData(PConstBuffer pEnd);

    /**
     * Accesses start of buffer to be consumed; called by consumer.
     *
     * @return pointer to first byte of buffer to be consumed
     */
    inline PConstBuffer getConsumptionStart();

    /**
     * Accesses end of buffer to be consumed; called by consumer.
     *
     * @return pointer to first byte past end of buffer to be consumed
     */
    inline PConstBuffer getConsumptionEnd();

    /**
     * Computes the number of contiguous bytes available to be consumed from
     * this buffer.
     *
     * @return number of bytes available for consumption
     */
    inline uint getConsumptionAvailable();

    /**
     * Accesses start of buffer into which data should be produced; called by
     * producer.
     *
     * @return pointer to first byte of buffer into which data should be
     * produced
     */
    inline PBuffer getProductionStart();

    /**
     * Accesses end of buffer into which data should be produced; called by
     * producer.
     *
     * @return pointer to first byte past end of buffer into which data should
     * be produced
     */
    inline PBuffer getProductionEnd();

    /**
     * Computes the number of contiguous bytes of buffer space available for
     * producing data into this buffer.
     *
     * @return number of bytes available for production
     */
    inline uint getProductionAvailable();

    /**
     * Retrieves the state of this accessor.
     *
     * @return ExecStreamBufState
     */
    inline ExecStreamBufState getState();

    /**
     * Retrieves the buffer provision mode of this accessor.
     *
     * @return ExecStreamBufProvision
     */
    inline ExecStreamBufProvision getProvision();

    /**
     * Retrieves the descriptor for tuples being accessed.
     *
     * @return TupleDescriptor
     */
    inline TupleDescriptor const &getTupleDesc();

    /**
     * Retrieves the format for tuples being accessed.
     *
     * @return TupleFormat
     */
    inline TupleFormat getTupleFormat();

    /**
     * Validates the size of a tuple, throwing a TupleOverflowExcn
     * if it is bigger than the maximum buffer size.
     *
     * @param tupleData tuple to be produced
     */
    inline void validateTupleSize(TupleData const &tupleData);

    /**
     * Attempts to marshal a tuple into the production buffer, 
     * placing the first byte at getProductionStart().
     *
     * @return true if tuple was successfully marshalled
     * (in which case produceData is called as a side-effect);
     * false if tuple could not fit into remaining buffer
     */
    inline bool produceTuple(TupleData const &tupleData);

    /**
     * Accesses a tuple from getConsumptionStart() but does not unmarshal it or
     * consumer it.  Once this is called, it may not be called again until
     * consumeTuple has been called.
     *
     * @return same as getConsumptionTupleAccessor()
     */
    inline TupleAccessor &accessConsumptionTuple();

    /**
     * Unmarshals a tuple from getConsumptionStart() but does
     * not consume it.  Once this is called,  it may not be called
     * again until consumeTuple has been called.
     *
     * @param tupleData receives unmarshalled data
     *
     * @param iFirstDatum see TupleAccessor::unmarshal
     */
    inline void unmarshalTuple(TupleData &tupleData, uint iFirstDatum = 0);

    /**
     * Consumes last tuple accessed via accessConsumptionTuple() or
     * unmarshalTuple().
     */
    inline void consumeTuple();

    /**
     * @return whether accessConsumptionTuple() or unmarshalTuple() has been
     * called without a corresponding call to consumeTuple()
     */
    inline bool isTupleConsumptionPending() const;

    /**
     * @return a TupleAccessor suitable for use in consumption
     */
    inline TupleAccessor &getConsumptionTupleAccessor();

    /**
     * @return a TupleAccessor suitable for use in tracing buffer contents
     */
    inline TupleAccessor &getTraceTupleAccessor();
};

inline ExecStreamBufAccessor::ExecStreamBufAccessor()
{
    clear();
    provision = BUFPROV_NONE;
    state = EXECBUF_EOS;
    tupleFormat = TUPLE_FORMAT_STANDARD;
    cbBuffer = 0;
}

inline void ExecStreamBufAccessor::setProvision(
    ExecStreamBufProvision provisionInit)
{
    assert(provision == BUFPROV_NONE);
    provision = provisionInit;
}

inline void ExecStreamBufAccessor::setTupleShape(
    TupleDescriptor const &tupleDescInit,
    TupleFormat tupleFormatInit)
{
    tupleDesc = tupleDescInit;
    tupleFormat = tupleFormatInit;
    tupleProductionAccessor.compute(tupleDesc, tupleFormat);
    tupleConsumptionAccessor.compute(tupleDesc, tupleFormat);
}

inline void ExecStreamBufAccessor::clear()
{
    pBufStart = NULL;
    pBufEnd = NULL;
    pProducer = NULL;
    pConsumer = NULL;
    cbBuffer = 0;
    state = EXECBUF_IDLE;
    tupleProductionAccessor.resetCurrentTupleBuf();
    tupleConsumptionAccessor.resetCurrentTupleBuf();
}

inline void ExecStreamBufAccessor::provideBufferForProduction(
    PBuffer pStart,
    PBuffer pEnd,
    bool reusable)
{
    assert((state != EXECBUF_NEED_CONSUMPTION) && (state != EXECBUF_EOS));
    assert(provision == BUFPROV_CONSUMER);
    pBufStart = pStart;
    pBufEnd = pEnd;
    pProducer = pStart;
    pConsumer = pStart;
    cbBuffer = pEnd - pStart;
    state = EXECBUF_NEED_PRODUCTION;

    if (!reusable) {
        // indicate that this buffer is not reusable
        pBufStart = NULL;
    }
}

inline void ExecStreamBufAccessor::provideBufferForConsumption(
    PConstBuffer pStart,
    PConstBuffer pEnd)
{
    assert((state != EXECBUF_NEED_CONSUMPTION) && (state != EXECBUF_EOS));
    assert(provision == BUFPROV_PRODUCER);
    pBufStart = const_cast<PBuffer>(pStart);
    pBufEnd = const_cast<PBuffer>(pEnd);
    pConsumer = pBufStart;
    pProducer = pBufEnd;
    state = EXECBUF_NEED_CONSUMPTION;

    // indicate that this buffer is not reusable
    pBufStart = NULL;
}

inline void ExecStreamBufAccessor::requestProduction()
{
    assert(state == EXECBUF_IDLE);
    state = EXECBUF_NEED_PRODUCTION;
    pProducer = pBufStart;
    pConsumer = pBufStart;
}

inline void ExecStreamBufAccessor::markEOS()
{
    assert(state != EXECBUF_NEED_CONSUMPTION);
    assert(pProducer == pConsumer);
    clear();
    state = EXECBUF_EOS;
}

inline PConstBuffer ExecStreamBufAccessor::getConsumptionStart()
{
    return pConsumer;
}

inline PConstBuffer ExecStreamBufAccessor::getConsumptionEnd()
{
    return pProducer;
}

inline uint ExecStreamBufAccessor::getConsumptionAvailable()
{
    return getConsumptionEnd() - getConsumptionStart();
}

inline PBuffer ExecStreamBufAccessor::getProductionStart()
{
    return pProducer;
}

inline PBuffer ExecStreamBufAccessor::getProductionEnd()
{
    return pBufEnd;
}

inline uint ExecStreamBufAccessor::getProductionAvailable()
{
    return getProductionEnd() - getProductionStart();
}

inline ExecStreamBufState ExecStreamBufAccessor::getState()
{
    return state;
}

inline ExecStreamBufProvision ExecStreamBufAccessor::getProvision()
{
    return provision;
}

inline TupleFormat ExecStreamBufAccessor::getTupleFormat()
{
    return tupleFormat;
}

inline TupleDescriptor const &ExecStreamBufAccessor::getTupleDesc()
{
    return tupleDesc;
}

inline void ExecStreamBufAccessor::produceData(PBuffer pEnd)
{
    assert(state != EXECBUF_EOS);
    assert(pEnd > getProductionStart());
    assert(pEnd <= getProductionEnd());
    pProducer = pEnd;
    state = EXECBUF_NEED_CONSUMPTION;
}

inline void ExecStreamBufAccessor::consumeData(PConstBuffer pEnd)
{
    assert(state == EXECBUF_NEED_CONSUMPTION);
    assert(pEnd > getConsumptionStart());
    assert(pEnd <= getConsumptionEnd());
    pConsumer = const_cast<PBuffer>(pEnd);
    if (pConsumer == getConsumptionEnd()) {
        state = EXECBUF_IDLE;
    }
}

inline void ExecStreamBufAccessor::validateTupleSize(
    TupleData const &tupleData)
{
    if (!tupleProductionAccessor.isBufferSufficient(tupleData,  cbBuffer)) {
        uint cbTuple = tupleProductionAccessor.getByteCount(tupleData);
        throw TupleOverflowExcn(tupleDesc, tupleData, cbTuple, cbBuffer);
    }
}

inline bool ExecStreamBufAccessor::produceTuple(TupleData const &tupleData)
{
    assert(state != EXECBUF_EOS);

    if (tupleProductionAccessor.isBufferSufficient(
            tupleData, getProductionAvailable()))
    {
        tupleProductionAccessor.marshal(tupleData, getProductionStart());
        produceData(
            getProductionStart()
            + tupleProductionAccessor.getCurrentByteCount());
        return true;
    } else {
        validateTupleSize(tupleData);
        return false;
    }
}

inline TupleAccessor &ExecStreamBufAccessor::accessConsumptionTuple()
{
    assert(state == EXECBUF_NEED_CONSUMPTION);
    assert(!tupleConsumptionAccessor.getCurrentTupleBuf());
    
    tupleConsumptionAccessor.setCurrentTupleBuf(getConsumptionStart());
    return tupleConsumptionAccessor;
}

inline void ExecStreamBufAccessor::unmarshalTuple(
    TupleData &tupleData, uint iFirstDatum)
{
    accessConsumptionTuple();
    tupleConsumptionAccessor.unmarshal(tupleData, iFirstDatum);
}

inline void ExecStreamBufAccessor::consumeTuple()
{
    assert(tupleConsumptionAccessor.getCurrentTupleBuf());
    
    consumeData(
        getConsumptionStart() + tupleConsumptionAccessor.getCurrentByteCount());
    tupleConsumptionAccessor.resetCurrentTupleBuf();
}

inline bool ExecStreamBufAccessor::isTupleConsumptionPending() const
{
    if (tupleConsumptionAccessor.getCurrentTupleBuf()) {
        return true;
    } else {
        return false;
    }
}

inline TupleAccessor &ExecStreamBufAccessor::getConsumptionTupleAccessor()
{
    return tupleConsumptionAccessor;
}

inline TupleAccessor &ExecStreamBufAccessor::getTraceTupleAccessor()
{
    // this can be used for tracing since we don't need its state
    // across calls to produceTuple
    return tupleProductionAccessor;
}

FENNEL_END_NAMESPACE

#endif

// End ExecStreamBufAccessor.h
