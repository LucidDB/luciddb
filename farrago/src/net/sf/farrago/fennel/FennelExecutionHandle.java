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

/**
 * FennelExecutionHandle provides a handle for passing execution state from
 * Farrago to Fennel. The object containing the execution state is allocated in
 * Fennel and then accessed from Farrago via the handle.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class FennelExecutionHandle
{
    //~ Instance fields --------------------------------------------------------

    /**
     * The execution handle used in Farrago to access the Fennel object. Set to
     * 0 if the handle is invalid.
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

// End FennelExecutionHandle.java
