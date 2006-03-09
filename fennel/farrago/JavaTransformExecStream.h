/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2006-2006 The Eigenbase Project
// Copyright (C) 2006-2006 Disruptive Tech
// Copyright (C) 2006-2006 LucidEra, Inc.
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
#ifndef Fennel_JavaTransformExecStream_Included
#define Fennel_JavaTransformExecStream_Included

#include "fennel/exec/ExecStreamDefs.h"
#include "fennel/exec/ConfluenceExecStream.h"
#include "fennel/farrago/CmdInterpreter.h"
#include "fennel/segment/SegPageLock.h"
#include "fennel/tuple/TupleData.h"
#include <string>
#include <iostream>

#include <jni.h>

FENNEL_BEGIN_NAMESPACE

struct JavaTransformExecStreamParams : 
    virtual public ConfluenceExecStreamParams
{
    /**
     * Java class name.
     */
    std::string javaClassName;

    /**
     * StreamGraphHandle pointer for accessing FarragoRuntimeContext.
     */
    CmdInterpreter::StreamGraphHandle *pStreamGraphHandle;

    JavaTransformExecStreamParams()
        : javaClassName("") {}
};


/**
 * JavaTransformExecStream represents a sequence of Java transforms
 * encapsulated within a Fennel ExecStream.
 */
class JavaTransformExecStream :	virtual public ConfluenceExecStream
{
    CmdInterpreter::StreamGraphHandle *pStreamGraphHandle;

    std::string farragoTransformClassName;

    jobject outputByteBuffer;

    /**
     * The Java peer, an instance of a net.sf.farrago.runtime.FarragoTransform.
     */
    jobject farragoTransform;

    /**
     * Accessor for scratch segment.
     */
    SegmentAccessor scratchAccessor;

    /**
     * Lock on buffer used to fetch data from Java.
     */
    SegPageLock bufferLock;

  public:
    JavaTransformExecStream();

    // implement ExecStream
    virtual void prepare(JavaTransformExecStreamParams const &params);
    virtual void getResourceRequirements(
        ExecStreamResourceQuantity &minQuantity,
        ExecStreamResourceQuantity &optQuantity);
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
    virtual void closeImpl();
    virtual ExecStreamBufProvision getOutputBufProvision() const;
};

FENNEL_END_NAMESPACE

#endif 

// End JavaTransformExecStream.h
