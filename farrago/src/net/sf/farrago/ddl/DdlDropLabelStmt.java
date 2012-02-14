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
package net.sf.farrago.ddl;

import net.sf.farrago.catalog.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.session.*;


/**
 * DdlDropLabelStmt extends DdlDropStmt to remove obsolete label statistics.
 * Repository design constraints related to upgrade prevent the statistics from
 * being associated with the label itself. They are instead associated
 * implicitly via time stamps. Therefore, drop rules are insufficient to trigger
 * the necessary deletions. Avoid this pattern whenever possible!
 *
 * @author Stephan Zuercher
 */
public class DdlDropLabelStmt
    extends DdlDropStmt
{
    //~ Constructors -----------------------------------------------------------

    /**
     * Constructs a new DdlDropLabelStmt.
     *
     * @param droppedElement FemLabel dropped by this stmt
     * @param restrict whether DROP RESTRICT is in effect
     */
    public DdlDropLabelStmt(
        FemLabel droppedElement,
        boolean restrict)
    {
        super(droppedElement, restrict);
    }

    //~ Methods ----------------------------------------------------------------

    // override DdlStmt
    public void preValidate(FarragoSessionDdlValidator ddlValidator)
    {
        FarragoRepos repos = ddlValidator.getRepos();
        boolean usePreviewRefDelete =
            repos.getEnkiMdrRepos().supportsPreviewRefDelete();

        // Remove stats associated with the label being dropped.  Note that
        // this needs to be done before the label is deleted from the
        // catalog.
        FarragoCatalogUtil.removeObsoleteStatistics(
            (FemLabel) getModelElement(),
            repos,
            usePreviewRefDelete);

        super.preValidate(ddlValidator);
    }
}

// End DdlDropLabelStmt.java
