/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 SQLstream, Inc.
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
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
package net.sf.farrago.fennel;

import java.util.logging.*;

import net.sf.farrago.trace.*;
import net.sf.farrago.util.*;


/**
 * FennelJavaHandles are FarragoAllocations which ensure that handles returned
 * by FennelStorage.newObjectHandle get closed under all circumstances.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FennelJavaHandle
    implements FarragoAllocation
{
    //~ Static fields/initializers ---------------------------------------------

    private static final Logger tracer =
        FarragoTrace.getFennelJavaHandleTracer();

    //~ Instance fields --------------------------------------------------------

    private long objectHandle;

    //~ Constructors -----------------------------------------------------------

    FennelJavaHandle(long objectHandle)
    {
        this.objectHandle = objectHandle;
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoAllocation
    public void closeAllocation()
    {
        if (objectHandle == 0) {
            return;
        }
        tracer.fine(this.toString());
        FennelStorage.deleteObjectHandle(objectHandle);
    }

    /**
     * @return the native handle as a long
     */
    public long getLongHandle()
    {
        assert (objectHandle != 0);
        return objectHandle;
    }
}

// End FennelJavaHandle.java
