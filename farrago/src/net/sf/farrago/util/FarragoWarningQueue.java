/*
// $Id$
// Farrago is an extensible data management system.
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
