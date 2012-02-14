/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
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
