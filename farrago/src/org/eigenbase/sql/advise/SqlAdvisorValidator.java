/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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

package org.eigenbase.sql.advise;

import org.eigenbase.sql.*;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import net.sf.farrago.util.FarragoException;

/**
 * <code>SqlAdvisorValidator</code> is used by SqlAdvisor to traverse the parse  * tree of a SQL statement, not for validation purpose but for setting up the 
 * scopes and namespaces to facilitate retrieval of SQL statement completion 
 * hints
 *
 * @author tleung
 * @version $Id$
 *
 * @since Jan 16, 2005
 */
public class SqlAdvisorValidator extends SqlValidator
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
        CatalogReader catalogReader,
        RelDataTypeFactory typeFactory)
    {   
        super(opTab, catalogReader, typeFactory);
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * Registers all the SqlIdentifiers in this parse tree into a map keyed by
     * their respective ParserPostion.  Performs the same for the associated
     * scopes
     * @param id
     * @param scope
     */
    public void validateIdentifier(SqlIdentifier id, Scope scope)
    {
        String ppstring = id.getParserPosition().toString();
        sqlids.put(ppstring, id);
        idscopes.put(ppstring, scope);
    }

    /**
     * call the parent class method and mask any exception thrown
     */
    public RelDataType deriveType(
        Scope scope,
        SqlNode operand)
    {
        try {
            return super.deriveType(scope, operand);
        }
        catch (FarragoException e) {
            return unknownType;
        }
        catch (UnsupportedOperationException e) {
            return unknownType;
        }
        catch (Error e) {
            return unknownType;
        }
    }

    // we do not need to validate from clause for traversing the parse tree
    // because there is no SqlIdentifier in from clause that need to be 
    // registered into sqlids map
    protected void validateFrom(
        SqlNode node,
        RelDataType targetRowType,
        Scope scope)
    {
    }

    /**
     * call the parent class method and mask any exception thrown
     */
    protected void validateWhereClause(SqlSelect select)
    {
        try {
            super.validateWhereClause(select);
        }
        catch (FarragoException e) {
        }
    }

    /**
     * call the parent class method and mask any exception thrown
     */
    protected void validateHavingClause(SqlSelect select)
    {
        try {
            super.validateHavingClause(select);
        }
        catch (FarragoException e) {
        }
    }
} // End of SqlAdvisorValidator
