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

import java.util.*;

import net.sf.farrago.cwm.core.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.session.*;

import org.eigenbase.sql.*;


/**
 * DdlCreateStmt represents a DDL CREATE statement of any kind.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class DdlCreateStmt
    extends DdlStmt
{
    //~ Instance fields --------------------------------------------------------

    DdlReplaceOptions replaceOptions;

    //~ Constructors -----------------------------------------------------------

    /**
     * Constructs a new DdlCreateStmt.
     *
     * @param createdElement top-level element created by this stmt
     */
    public DdlCreateStmt(
        CwmModelElement createdElement,
        DdlReplaceOptions replaceOptions)
    {
        super(createdElement);
        this.replaceOptions = replaceOptions;
    }

    //~ Methods ----------------------------------------------------------------

    // implement DdlStmt
    public void visit(DdlVisitor visitor)
    {
        visitor.visit(this);
    }

    public DdlReplaceOptions getReplaceOptions()
    {
        return replaceOptions;
    }

    // refine DdlStmt
    public void postCommit(FarragoSessionDdlValidator ddlValidator)
    {
        if (ddlValidator instanceof DdlValidator) {
            DdlValidator val = (DdlValidator) ddlValidator;
            val.handlePostCommit(getModelElement(), "Create");
        }
    }
}

// End DdlCreateStmt.java
