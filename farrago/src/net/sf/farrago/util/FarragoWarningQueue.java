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
package net.sf.farrago.util;

import java.sql.*;

import java.util.logging.*;

import net.sf.farrago.trace.*;


/**
 * FarragoWarningQueue provides an implementation for objects such as {@link
 * Connection} which store a queue of warnings.
 *
 * @author John Sichi
 * @version $Id$
 */
public class FarragoWarningQueue
{
    //~ Static fields/initializers ---------------------------------------------

    private static final Logger tracer =
        FarragoTrace.getFarragoJdbcEngineDriverTracer();

    //~ Instance fields --------------------------------------------------------

    private SQLWarning warnings;

    //~ Methods ----------------------------------------------------------------

    /**
     * Retrieves warnings which have accumulated on this queue. See {@link
     * Connection#getWarnings}.
     *
     * @return warnings which have accumulated
     */
    public SQLWarning getWarnings()
    {
        return warnings;
    }

    /**
     * Clears accumulated warnings. See {@link Connection#clearWarnings}.
     */
    public void clearWarnings()
    {
        warnings = null;
    }

    /**
     * Posts a warning to this queue.
     *
     * @param warning a single warning to be posted; warning.getNextWarning()
     * must be null on entry, since queue handles chaining itself
     */
    public synchronized void postWarning(SQLWarning warning)
    {
        assert (warning.getNextWarning() == null);

        tracer.warning(warning.getMessage());

        if (warnings == null) {
            warnings = warning;
        } else {
            // TODO jvs 12-Nov-2006:  avoid O(n^2) chaining cost here
            warnings.setNextWarning(warning);
        }
    }
}

// End FarragoWarningQueue.java
