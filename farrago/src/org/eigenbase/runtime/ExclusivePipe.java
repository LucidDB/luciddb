/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2002 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 2003 John V. Sichi
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
package org.eigenbase.runtime;

import java.nio.*;


/**
 * A synchronization primitive which allows producer and a consumer to use the
 * same {@link java.nio.Buffer} without treading on each other's feet.
 *
 * <p>This class is <em>synchronous</em>: only one of the producer and consumer
 * has access at a time. There is only one buffer, and data is not copied.
 *
 * <p>The byte buffer is fixed in size. The producer writes up to the maximum
 * number of bytes into the buffer, then yields. The consumer must read all of
 * the data in the buffer before yielding back to the producer.
 *
 * @author jhyde
 * @version $Id$
 * @testcase
 */
public class ExclusivePipe
    extends Interlock
{
    //~ Instance fields --------------------------------------------------------

    private final ByteBuffer buf;

    //~ Constructors -----------------------------------------------------------

    public ExclusivePipe(ByteBuffer buf)
    {
        this.buf = buf;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Returns the buffer.
     */
    public ByteBuffer getBuffer()
    {
        return buf;
    }

    public void beginWriting()
    {
        super.beginWriting();
        buf.clear(); // don't need to synchronize -- we hold the semaphore
    }

    public void beginReading()
    {
        super.beginReading();
        buf.flip(); // don't need to synchronize -- we hold the semaphore
    }
}

// End ExclusivePipe.java
