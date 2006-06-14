/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 The Eigenbase Project
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

package com.disruptivetech.farrago.sql.advise;

import org.eigenbase.util.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;

/**
 * <code>SqlAdvisorValidator</code> is used by {@link SqlAdvisor} to traverse
 * the parse tree of a SQL statement, not for validation purpose but for
 * setting up the scopes and namespaces to facilitate retrieval of SQL
 * statement completion hints.
 *
 * @author tleung
 * @version $Id$
 *
 * @since Jan 16, 2005
 */
public class SqlAdvisorValidator extends SqlValidatorImpl
{
    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a SqlAdvisor validator.
     *
     * @pre opTab != null
     * @pre // node is a "query expression" (per SQL standard)
     * @pre catalogReader != null
     * @pre typeFactory != null
     */
    public SqlAdvisorValidator(
        SqlOperatorTable opTab,
        SqlValidatorCatalogReader catalogReader,
        RelDataTypeFactory typeFactory,
        Compatible compatible)
    {
        super(opTab, catalogReader, typeFactory, compatible);
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * Registers all the SqlIdentifiers in this parse tree into a map keyed by
     * their respective ParserPostion.  Performs the same for the associated
     * scopes
     * @param id
     * @param scope
     */
    public void validateIdentifier(SqlIdentifier id, SqlValidatorScope scope)
    {
        String ppstring = id.getParserPosition().toString();
        sqlIds.put(ppstring, id);
        idScopes.put(ppstring, scope);
    }

    public SqlNode expand(SqlNode expr, SqlValidatorScope scope)
    {
        // Disable expansion. It doesn't help us come up with better hints.
        return expr;
    }

    public SqlNode expandOrderExpr(SqlSelect select, SqlNode orderExpr)
    {
        // Disable expansion. It doesn't help us come up with better hints.
        return orderExpr;
    }

    /**
     * Calls the parent class method and mask Farrago exception thrown.
     */
    public RelDataType deriveType(
        SqlValidatorScope scope,
        SqlNode operand)
    {
        // REVIEW Do not mask Error (indicates a serious system problem) or
        //   UnsupportedOperationException (a bug).
        // I have to mask UnsupportedOperationException because 
        // SqlValidatorImpl.getValidatedNodeType throws it for an unrecognized
        // identifier node
        // I have to mask Error as well because AbstractNamespace.getRowType 
        // called in super.deriveType can do a Util.permAssert that throws Error
        try {
            return super.deriveType(scope, operand);
        } catch (EigenbaseException e) {
            return unknownType;
        } catch (UnsupportedOperationException e) {
            return unknownType;
        } catch (Error e) {
            return unknownType;
        }
    }

    // we do not need to validate from clause for traversing the parse tree
    // because there is no SqlIdentifier in from clause that need to be
    // registered into sqlIds map
    protected void validateFrom(
        SqlNode node,
        RelDataType targetRowType,
        SqlValidatorScope scope)
    {
    }

    /**
     * Calls the parent class method and masks Farrago exception thrown.
     */
    protected void validateWhereClause(SqlSelect select)
    {
        try {
            super.validateWhereClause(select);
        } catch (EigenbaseException e) {
        }
    }

    /**
     * Calls the parent class method and masks Farrago exception thrown.
     */
    protected void validateHavingClause(SqlSelect select)
    {
        try {
            super.validateHavingClause(select);
        } catch (EigenbaseException e) {
        }
    }
}

// End of SqlAdvisorValidator.java

