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

#ifndef Fennel_JavaSourceExecStream_Included
#define Fennel_JavaSourceExecStream_Included

#include "fennel/exec/SingleOutputExecStream.h"
#include "fennel/segment/SegPageLock.h"
#include "fennel/farrago/CmdInterpreter.h"

#include <jni.h>
#include <boost/scoped_ptr.hpp>

FENNEL_BEGIN_NAMESPACE

/**
 * JavaSourceExecStreamParams defines parameters for instantiating a
 * JavaSourceExecStream.
 */
struct JavaSourceExecStreamParams : public SingleOutputExecStreamParams
{
    CmdInterpreter::StreamGraphHandle *pStreamGraphHandle;
    
    int javaTupleStreamId;
};

/**
 * JavaSourceExecStream gets input from a Java object and outputs a stream of tuples.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class JavaSourceExecStream : public SingleOutputExecStream
{
    CmdInterpreter::StreamGraphHandle *pStreamGraphHandle;
    int javaTupleStreamId;

protected:
    /**
     * The Java peer, an instance of a net.sf.farrago.query.JavaTupleStream;
     */
    jobject javaTupleStream;


    /**
     * Accessor for scratch segment.
     */
    SegmentAccessor scratchAccessor;

    /**
     * Lock on buffer used to fetch data from Java. Can this support 2 buffers??
     */
    SegPageLock bufferLock;

public:
    explicit JavaSourceExecStream();

    // implement ExecStream
    virtual void prepare(JavaSourceExecStreamParams const &params);
    virtual void open(bool restart);
    virtual void closeImpl();
    virtual ExecStreamBufProvision getOutputBufProvision() const;
};

FENNEL_END_NAMESPACE

#endif

// End JavaSourceExecStream.h
