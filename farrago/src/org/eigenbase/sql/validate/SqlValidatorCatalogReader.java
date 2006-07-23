/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2004-2005 The Eigenbase Project
// Copyright (C) 2004-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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
package org.eigenbase.sql.validate;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;


/**
 * Supplies catalog information for {@link SqlValidator}.
 *
 * <p>This interface only provides a thin API to the underlying repository, and
 * this is intentional. By only presenting the repository information of
 * interest to the validator, we reduce the dependency on exact mechanism to
 * implement the repository. It is also possible to construct mock
 * implementations of this interface for testing purposes.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 25, 2003
 */
public interface SqlValidatorCatalogReader
{

    //~ Methods ----------------------------------------------------------------

    /**
     * Finds a table with the given name, possibly qualified.
     *
     * @return named table, or null if not found
     */
    SqlValidatorTable getTable(String [] names);

    /**
     * Finds a user-defined type with the given name, possibly qualified.
     *
     * <p>NOTE jvs 12-Feb-2005: the reason this method is defined here instead
     * of on RelDataTypeFactory is that it has to take into account
     * context-dependent information such as SQL schema path, whereas a type
     * factory is context-independent.
     *
     * @return named type, or null if not found
     */
    RelDataType getNamedType(SqlIdentifier typeName);

    /**
     * Gets schema object names as specified. They can be schema or table
     * object. If names array contain 1 element, return all schema names and all
     * table names under the default schema (if that is set) If names array
     * contain 2 elements, treat 1st element as schema name and return all table
     * names in this schema
     *
     * @param names the array contains either 2 elements representing a
     * partially qualified object name in the format of 'schema.object', or an
     * unqualified name in the format of 'object'
     *
     * @return the list of all object (schema and table) names under the above
     * criteria
     */
    SqlMoniker [] getAllSchemaObjectNames(String [] names);
}

// End SqlValidatorCatalogReader.java
