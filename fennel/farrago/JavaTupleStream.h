/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 1999-2004 John V. Sichi.
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

#ifndef Fennel_JavaTupleStream_Included
#define Fennel_JavaTupleStream_Included

#include "fennel/xo/TupleStream.h"
#include "fennel/common/ByteInputStream.h"
#include "fennel/segment/SegPageLock.h"
#include "fennel/farrago/CmdInterpreter.h"

#include <jni.h>
#include <boost/scoped_ptr.hpp>

FENNEL_BEGIN_NAMESPACE

/**
 * JavaTupleStreamParams defines parameters for instantiating a JavaTupleStream.
 */
struct JavaTupleStreamParams : public TupleStreamParams
{
    CmdInterpreter::StreamGraphHandle *pStreamGraphHandle;
    
    int javaTupleStreamId;
};

/**
 * JavaTupleStream produces a stream of tuples read from a stream produced by a
 * Java object.
 */
class JavaTupleStream : public TupleStream, private ByteInputStream
{
    TupleDescriptor outputTupleDesc;

    CmdInterpreter::StreamGraphHandle *pStreamGraphHandle;
    
    int javaTupleStreamId;
    
    /**
     * The Java subclass instance of net.sf.farrago.query.JavaTupleStream.
     */
    jobject javaTupleStream;

    /**
     * Java instance of java.nio.ByteBuffer used for passing tuple data.
     */
    jobject javaByteBuffer;
    
    /**
     * Accessor for scratch segment.
     */
    SegmentAccessor scratchAccessor;
    
    /**
     * Lock on buffer used to fetch data from Java.
     */
    SegPageLock bufferLock;
    
    // implement ByteInputStream
    virtual void readNextBuffer();

public:
    explicit JavaTupleStream();
    void prepare(JavaTupleStreamParams const &params);
    virtual void open(bool restart);
    virtual void closeImpl();
    virtual ByteInputStream &getProducerResultStream();
    virtual TupleDescriptor const &getOutputDesc() const;
    virtual BufferProvision getResultBufferProvision() const;
    virtual BufferProvision getInputBufferRequirement() const;
};

FENNEL_END_NAMESPACE

#endif

// End JavaTupleStream.h
