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

import net.sf.farrago.cwm.core.*;
import net.sf.farrago.session.*;


/**
 * DdlDropStmt represents a DDL DROP statement of any kind.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class DdlDropStmt
    extends DdlStmt
{
    //~ Instance fields --------------------------------------------------------

    private boolean restrict;

    //~ Constructors -----------------------------------------------------------

    /**
     * Constructs a new DdlDropStmt.
     *
     * @param droppedElement top-level element dropped by this stmt
     * @param restrict whether DROP RESTRICT is in effect
     */
    public DdlDropStmt(
        CwmModelElement droppedElement,
        boolean restrict)
    {
        super(droppedElement);
        this.restrict = restrict;
    }

    //~ Methods ----------------------------------------------------------------

    // override DdlStmt
    public boolean isDropRestricted()
    {
        return restrict;
    }

    // override DdlStmt
    public void preValidate(FarragoSessionDdlValidator ddlValidator)
    {
        super.preValidate(ddlValidator);

        // Delete the top-level object.  DdlValidator will take care of the
        // rest.
        ddlValidator.deleteObject(getModelElement());
    }

    // implement DdlStmt
    public void visit(DdlVisitor visitor)
    {
        visitor.visit(this);
    }
}

// End DdlDropStmt.java
