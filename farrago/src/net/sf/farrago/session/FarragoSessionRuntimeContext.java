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
     * @param globalStreamName name of the stream
     * @param isInput
     *   true: find the adapter intepolated after the stream;
     *   false: find the stream itself.
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
     * @param impersonatedUser name of user to impersonate, or null for
     * no impersonation
     */
    public void pushRoutineInvocation(
        FarragoSessionUdrContext udrContext,
        boolean allowSql,
        String impersonatedUser);

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
