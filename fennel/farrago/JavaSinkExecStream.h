/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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

#ifndef Fennel_JavaSinkExecStream_Included
#define Fennel_JavaSinkExecStream_Included

#include "fennel/exec/SingleInputExecStream.h"
#include "fennel/farrago/CmdInterpreter.h"

#include <jni.h>
#include <boost/scoped_ptr.hpp>

FENNEL_BEGIN_NAMESPACE

/**
 * JavaSinkExecStreamParams defines parameters for instantiating a
 * JavaSinkExecStream.
 */
struct JavaSinkExecStreamParams : public SingleInputExecStreamParams
{
    CmdInterpreter::StreamGraphHandle *pStreamGraphHandle;
    int javaFennelPipeIterId;
};

/**
 * JavaSinkExecStream reads its tuples from an upstream execution object and
 * pumps them into Java.
 *
 * @author jhyde
 * @version $Id$
 */
class JavaSinkExecStream : public SingleInputExecStream
{
    CmdInterpreter::StreamGraphHandle *pStreamGraphHandle;
    int javaFennelPipeIterId;
    jobject javaFennelPipeIter;

public:
    explicit JavaSinkExecStream();

    // implement ExecStream
    virtual void prepare(JavaSinkExecStreamParams const &params);
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
    virtual void closeImpl();
};

FENNEL_END_NAMESPACE

#endif

// End JavaSinkExecStream.h
