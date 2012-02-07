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

import org.eigenbase.relopt.*;


/**
 * FarragoSessionPlanner represents a query planner/optimizer associated with a
 * specific FarragoPreparingStmt.
 *
 * @author stephan
 * @version $Id$
 */
public interface FarragoSessionPlanner
    extends RelOptPlanner
{
    //~ Methods ----------------------------------------------------------------

    /**
     * @return the FarragoSessionPreparingStmt associated with this planner.
     */
    public FarragoSessionPreparingStmt getPreparingStmt();

    /**
     * Notifies this planner that registration for a particular SQL/MED plugin
     * is about to start, meaning the plugin might call the planner via methods
     * such as {@link RelOptPlanner#addRule}.
     *
     * @param serverClassName name of class implementing FarragoMedDataServer
     */
    public void beginMedPluginRegistration(String serverClassName);

    /**
     * Notifies this planner that registration has ended for the SQL/MED plugin
     * whose identity was last passed to beginMedPluginRegistration.
     */
    public void endMedPluginRegistration();
}

// End FarragoSessionPlanner.java
