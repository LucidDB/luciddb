/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2004-2005 John V. Sichi
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
package net.sf.farrago.ddl;

import net.sf.farrago.resource.*;
import net.sf.farrago.session.*;

import org.eigenbase.sql.*;


/**
 * DdlSetSchemaStmt represents the DDL statement SET SCHEMA.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class DdlSetSchemaStmt
    extends DdlSetContextStmt
{
    //~ Instance fields --------------------------------------------------------

    private SqlIdentifier schemaName;

    //~ Constructors -----------------------------------------------------------

    /**
     * Constructss a new DdlSetSchemaStmt.
     *
     * @param valueExpr value expression for new catalog
     */
    public DdlSetSchemaStmt(SqlNode valueExpr)
    {
        super(valueExpr);
    }

    //~ Methods ----------------------------------------------------------------

    // implement DdlStmt
    public void visit(DdlVisitor visitor)
    {
        visitor.visit(this);
    }

    // implement DdlStmt
    public void preValidate(FarragoSessionDdlValidator ddlValidator)
    {
        super.preValidate(ddlValidator);
        if (parsedExpr instanceof SqlIdentifier) {
            schemaName = (SqlIdentifier) parsedExpr;
            if (schemaName.names.length < 1) {
                schemaName = null;
            }
            if (schemaName.names.length > 2) {
                schemaName = null;
            }
        }
        if (schemaName == null) {
            throw FarragoResource.instance().ValidatorSetSchemaInvalidExpr.ex(
                ddlValidator.getRepos().getLocalizedObjectName(valueString));
        }
    }

    public SqlIdentifier getSchemaName()
    {
        return schemaName;
    }
}

// End DdlSetSchemaStmt.java
