/*
// $Id$
// Farrago is a relational database management system.
// Copyright (C) 2004-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/
package net.sf.farrago.ddl;

import net.sf.farrago.session.*;
import net.sf.farrago.resource.*;

import org.eigenbase.sql.*;

/**
 * DdlSetCatalogStmt represents the DDL statement SET CATALOG.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class DdlSetCatalogStmt extends DdlSetContextStmt
{
    private SqlIdentifier catalogName;
    
    /**
     * Constructs a new DdlSetCatalogStmt.
     *
     * @param valueExpr value expression for new catalog
     */
    public DdlSetCatalogStmt(SqlNode valueExpr)
    {
        super(valueExpr);
    }
    
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
            throw FarragoResource.instance().newValidatorSetCatalogInvalidExpr(
                ddlValidator.getRepos().getLocalizedObjectName(valueString));
        }
    }

    public SqlIdentifier getCatalogName()
    {
        return catalogName;
    }
}

// End DdlSetCatalogStmt.java
