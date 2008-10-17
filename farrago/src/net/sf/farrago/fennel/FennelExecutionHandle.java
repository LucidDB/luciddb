/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
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

/**
 * FennelExecutionHandle provides a handle for passing execution state from
 * Farrago to Fennel.  The object containing the execution state is allocated
 * in Fennel and then accessed from Farrago via the handle.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class FennelExecutionHandle
{
    //~ Instance fields --------------------------------------------------------

    /**
     * The execution handle used in Farrago to access the Fennel object.  Set
     * to 0 if the handle is invalid.
     */
    private long execHandle;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates the execution state object and its corresponding handle.
     */
    public FennelExecutionHandle()
    {
        this.execHandle = FennelStorage.newExecutionHandle();
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * @return the Fennel execution handle
     */
    public long getHandle()
    {
        return execHandle;
    }
    
    /**
     * Deallocates the Fennel object corresponding to the handle.
     */
    public synchronized void delete()
    {
        if (execHandle != 0) {
            FennelStorage.deleteExecutionHandle(execHandle);
            execHandle = 0;
        }
    }
    
    /**
     * Cancels execution of the statement corresponding to this handle.
     */
    public synchronized void cancelExecution()
    {
        if (execHandle != 0) {
            FennelStorage.cancelExecution(execHandle);
        }
    }
}

// End FennelExecHandle.java
