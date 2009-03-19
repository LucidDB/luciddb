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
package net.sf.farrago.session;

import net.sf.farrago.catalog.*;
import net.sf.farrago.fennel.*;
import net.sf.farrago.util.*;

import org.eigenbase.reltype.*;


/**
 * FarragoSessionRuntimeContext defines runtime support routines needed by
 * generated code.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FarragoSessionRuntimeContext
    extends FarragoAllocationOwner
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Loads the Fennel portion of an execution plan (either creating a new XO
     * graph or reusing a cached instance).
     *
     * @param xmiFennelPlan XMI representation of plan definition
     */
    public void loadFennelPlan(final String xmiFennelPlan);

    /**
     * Opens all streams, including the Fennel portion of the execution plan.
     * This should only be called after all Java TupleStreams have been created.
     */
    public void openStreams();

    /**
     * Requests cancellation of this execution (either for asynchronous abort,
     * or because execution has ended).
     */
    public void cancel();

    /**
     * Throws an exception if execution has been canceled.
     */
    public void checkCancel();

    /**
     * Associates an execution handle with the runtime context.
     *
     * @param execHandle the execution handle
     */
    public void setExecutionHandle(FennelExecutionHandle execHandle);

    /**
     * Sets the state of the top-level cursor associated with this context.
     * {@link #checkCancel} is called both before the fetch request
     * (active=true) and after the fetch (active=false). Not called for internal
     * cursors such as UDX inputs and cursors opened via reentrant SQL from
     * UDRs.
     *
     * @param active true if cursor is beginning a fetch request; false if
     * cursor is ending a fetch request
     */
    public void setCursorState(boolean active);

    /**
     * Waits for cursor state to be reset to active=false (returns immediately
     * if cursor is not currently active).
     */
    public void waitForCursor();

    /**
     * @return FennelStreamGraph pinned by loadFennelPlan
     */
    public FennelStreamGraph getFennelStreamGraph();

    /**
     * Retrieves the FennelStreamHandle corresponding to a stream
     *
     * @param globalStreamName name of the stream that you are searching for
     * @param isInput
     *
     * @return FennelStreamHandle corresponding to the stream specified by the
     * name parameter
     */
    public FennelStreamHandle getStreamHandle(
        String globalStreamName,
        boolean isInput);

    /**
     * Pushes a routine invocation onto the context stack.
     *
     * @param udrContext context holder for routine invocation instance within
     * statement being executed
     * @param allowSql whether SQL execution should be allowed in this routine
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
     * @param methodName name of external Java method
     *
     * @return exception to be re-thrown
     */
    public RuntimeException handleRoutineInvocationException(
        Throwable ex,
        String methodName);

    /**
     * Configures a custom class loader used to load extra classes that may be
     * needed during statement runtime. Specifically, allows Fennel to load
     * implementations of FarragoTransform.
     */
    public void setStatementClassLoader(ClassLoader classLoader);

    /**
     * Gets the row type for instantiating a result set.
     *
     * @param resultSetName name of result set stored by optimizer
     *
     * @return corresponding row type
     */
    public RelDataType getRowTypeForResultSet(String resultSetName);

    /**
     * @return session on behalf of which this runtime context is executing
     */
    public FarragoSession getSession();

    /**
     * @return FarragoRepos for use by extension projects
     */
    public FarragoRepos getRepos();

    /**
     * Detaches the current MDR session from the running thread. The detached
     * session is stored for later re-attachment and is automatically
     * re-attached and closed if when the runtime context is closed.
     */
    public void detachMdrSession();

    /**
     * Re-attaches a detached MDR session to the currently running thread, if
     * any was previously detached.
     */
    public void reattachMdrSession();

    /**
     * @return queue of warnings posted to this runtime context
     */
    public FarragoWarningQueue getWarningQueue();
}

// End FarragoSessionRuntimeContext.java
