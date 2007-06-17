/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2004-2007 John V. Sichi
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
 * DdlSetCatalogStmt represents the DDL statement SET CATALOG.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class DdlSetCatalogStmt
    extends DdlSetContextStmt
{
    //~ Instance fields --------------------------------------------------------

    private SqlIdentifier catalogName;

    //~ Constructors -----------------------------------------------------------

    /**
     * Constructs a new DdlSetCatalogStmt.
     *
     * @param valueExpr value expression for new catalog
     */
    public DdlSetCatalogStmt(SqlNode valueExpr)
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
            catalogName = (SqlIdentifier) parsedExpr;
            if (!catalogName.isSimple()) {
                catalogName = null;
            }
        }
        if (catalogName == null) {
            throw FarragoResource.instance().ValidatorSetCatalogInvalidExpr.ex(
                ddlValidator.getRepos().getLocalizedObjectName(valueString));
        }
    }

    public SqlIdentifier getCatalogName()
    {
        return catalogName;
    }
}

// End DdlSetCatalogStmt.java
