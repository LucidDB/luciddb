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

#ifndef Fennel_JavaPushSourceExecStream_Included
#define Fennel_JavaPushSourceExecStream_Included

#include "fennel/farrago/JavaSourceExecStream.h"

FENNEL_BEGIN_NAMESPACE

/**
 * JavaPushSourceExecStreamParams defines parameters for instantiating a
 * JavaPushSourceExecStream.
 */
struct JavaPushSourceExecStreamParams : public JavaSourceExecStreamParams
{
};

/**
 * JavaPushSourceExecStream is a "push mode" version of JavaSourceExecStream:
 * it runs in parallel with its java peer.
 * The peer runs in its own thread, fetching data from a Java Iterator and
 * marshalling it into fennel form.
 * To execute() the XO  asks the peer for a buffer of marshalled data,
 * but if none is ready it does not wait.
 *
 * @author Marc Berkowitz
 * @version $Id$
 */
class JavaPushSourceExecStream : public JavaSourceExecStream
{
    // java method handles (ByteBuffer)
    jmethodID methBufferSize;

    // java method handles (JavaPushTupleStream)
    jmethodID methOpenStream;
    jmethodID methCloseStream;
    jmethodID methGetBuffer;
    jmethodID methFreeBuffer;


    // allocate 2 buffers, one for java peer to fill in background,
    // and one for us to expose to our consumer.
    // Swapping the buffers is managed by our java peer.
    static const int NBUFFERS = 2;

    // the readable ByteBuffer that belongs to us
    jobject rdBuffer;

public:
    explicit JavaPushSourceExecStream();

    // implement ExecStream
    virtual void prepare(JavaPushSourceExecStreamParams const &params);
    virtual void getResourceRequirements(
        ExecStreamResourceQuantity &minQuantity,
        ExecStreamResourceQuantity &optQuantity);
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
    virtual void closeImpl();
};

FENNEL_END_NAMESPACE

#endif

// End JavaPushSourceExecStream.h
