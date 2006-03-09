/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2006 The Eigenbase Project
// Copyright (C) 2005-2006 Disruptive Tech
// Copyright (C) 2005-2006 LucidEra, Inc.
// Portions Copyright (C) 2003-2006 John V. Sichi
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
package net.sf.farrago.session;

import net.sf.farrago.fennel.*;
import net.sf.farrago.util.*;


/**
 * FarragoSessionRuntimeContext defines runtime support routines needed by
 * generated code.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FarragoSessionRuntimeContext extends FarragoAllocationOwner
{
    //~ Methods ---------------------------------------------------------------

    /**
     * Loads the Fennel portion of an execution plan (either creating
     * a new XO graph or reusing a cached instance).
     *
     * @param xmiFennelPlan XMI representation of plan definition
     */
    public void loadFennelPlan(final String xmiFennelPlan);

    /**
     * Opens all streams, including the Fennel portion of the execution plan.
     * This should only be called after all Java TupleStreams have been
     * created.
     */
    public void openStreams();

    /**
     * Requests cancellation of this execution (either for asynchronous
     * abort, or because execution has ended).
     */
    public void cancel();

    /**
     * Throws an exception if execution has been canceled.
     */
    public void checkCancel();
    
    /**
     * @return FennelStreamGraph pinned by loadFennelPlan
     */
    public FennelStreamGraph getFennelStreamGraph();

    /**
     * Pushes a routine invocation onto the context stack.
     *
     * @param udrContext context holder for routine invocation
     * instance within statement being executed
     *
     * @param allowSql whether SQL execution should be allowed in
     * this routine
     */
    public void pushRoutineInvocation(
        FarragoSessionUdrContext udrContext,
        boolean allowSql);

    /**
     * Pops a routine invocation from the context stack.
     */
    public void popRoutineInvocation();

    /**
     * Handles an exception caught by invocation of a routine.
     *
     * @param ex exception
     *
     * @param methodName name of external Java method
     *
     * @return exception to be re-thrown
     */
    public RuntimeException handleRoutineInvocationException(
        Throwable ex, String methodName);
    
    /**
     * Configures a custom class loader used to load extra classes that
     * may be needed during statement runtime.  Specifically, allows
     * Fennel to load implementations of FarragoTransform. 
     */
    public void setStatementClassLoader(ClassLoader classLoader);
}


// End FarragoSessionRuntimeContext.java
