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
package net.sf.farrago.query;

import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.namespace.*;


/**
 * FarragoQueryColumnSet represents a specialization of {@link
 * FarragoMedColumnSet} which knows how to interact with {@link
 * FarragoPreparingStmt}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FarragoQueryColumnSet
    extends FarragoMedColumnSet
{
    //~ Methods ----------------------------------------------------------------

    /**
     * @return the FarragoPreparingStmt acting on this column set
     */
    public FarragoPreparingStmt getPreparingStmt();

    /**
     * Sets the FarragoPreparingStmt acting on this column set.
     *
     * @param stmt the FarragoPreparingStmt to set
     */
    public void setPreparingStmt(FarragoPreparingStmt stmt);

    /**
     * @return the CwmNamedColumnSet corresponding to this column set
     */
    public CwmNamedColumnSet getCwmColumnSet();

    /**
     * Sets the CwmNamedColumnSet corresponding to this column set.
     *
     * @param cwmColumnSet the CwmNamedColumnSet, or null if this column set is
     * not defined in the catalog
     */
    public void setCwmColumnSet(CwmNamedColumnSet cwmColumnSet);
}

// End FarragoQueryColumnSet.java
