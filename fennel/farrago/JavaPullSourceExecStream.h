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

#ifndef Fennel_JavaPullSourceExecStream_Included
#define Fennel_JavaPullSourceExecStream_Included

#include "fennel/farrago/JavaSourceExecStream.h"

FENNEL_BEGIN_NAMESPACE

/**
 * JavaPullSourceExecStreamParams defines parameters for instantiating a
 * JavaPullSourceExecStream.
 */
struct JavaPullSourceExecStreamParams : public JavaSourceExecStreamParams
{
};

/**
 * JavaPullSourceExecStream is a "pull mode" version of JavaSourceExecStream:
 * this XO calls its java peer as a subroutine to provide a buffer
 * of marshalled data. Consequently the execute() method can block as the input
 * 
 *
 * @author John V. Sichi
 * @version $Id$
 */
class JavaPullSourceExecStream : public JavaSourceExecStream
{
    /**
     * Java instance of java.nio.ByteBuffer used for passing tuple data.
     */
    jobject javaByteBuffer;
    
public:
    explicit JavaPullSourceExecStream();

    // implement ExecStream
    virtual void prepare(JavaPullSourceExecStreamParams const &params);
    virtual void getResourceRequirements(
        ExecStreamResourceQuantity &minQuantity,
        ExecStreamResourceQuantity &optQuantity);
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
    virtual void closeImpl();
};

FENNEL_END_NAMESPACE

#endif

// End JavaPullSourceExecStream.h
