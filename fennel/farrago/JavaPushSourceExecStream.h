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
#include "fennel/tuple/TupleAccessor.h"

FENNEL_BEGIN_NAMESPACE

/**
 * JavaPushSourceExecStreamParams defines parameters for instantiating a
 * JavaPushSourceExecStream.
 */
struct JavaPushSourceExecStreamParams : public JavaSourceExecStreamParams
{
    uint nbuffers;                      // min 2
    uint bufferSize;                    // min size is max tuple size
    JavaPushSourceExecStreamParams();
};

inline JavaPushSourceExecStreamParams::JavaPushSourceExecStreamParams()
{
    nbuffers = 2;
    bufferSize = 0;                     // default to tuple size
}


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

    uint nbuffers;
    uint bufferSize;

    // the current input ByteBuffer (which belongs to us, not to our java peer):
    jobject rdBuffer;
    PConstBuffer rdBufferStart;         // buffer bounds
    PConstBuffer rdBufferEnd;
    PConstBuffer rdBufferPosn;          // next tuple to read
    bool rdBufferEOS;                   // saw EOS indicated by rdBuffer

    void releaseReadBuffer();           // frees the input buffer
    bool getReadBuffer();               // gets a new input buffer from the peer

    /// copies whole marshalled tuples from one buffer to another
    /// @param acc suitable TupleAccessor
    /// @returns count of byte copied. 0 means nothing fit.
    uint copyRows(TupleAccessor& acc, PBuffer dest, PBuffer destEnd, PConstBuffer src, PConstBuffer srcEnd);

public:
    explicit JavaPushSourceExecStream();

    // implement ExecStream
    virtual void prepare(JavaPushSourceExecStreamParams const &params);
    virtual ExecStreamBufProvision getOutputBufProvision() const;
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
