/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2004-2007 The Eigenbase Project
// Copyright (C) 2004-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
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

import java.util.List;

import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;


/**
 * Extends {@link SqlValidator} to allow discovery of useful data such as fully
 * qualified names of sql objects, alternative valid sql objects that can be
 * used in the SQL statement (dubbed as hints)
 *
 * @author tleung
 * @version $Id$
 * @since Jul 7, 2005
 */
public interface SqlValidatorWithHints
    extends SqlValidator
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Looks up completion hints for a syntatically correct SQL statement that
     * has been parsed into an expression tree. (Note this should be called
     * after {@link #validate(org.eigenbase.sql.SqlNode)}.
     *
     * @param topNode top of expression tree in which to lookup completion hints
     *
     * @param pos indicates the position in the sql statement we want to get
     * completion hints for. For example, "select a.ename, b.deptno from
     * sales.emp a join sales.dept b "on a.deptno=b.deptno where empno=1";
     * setting pos to 'Line 1, Column 17' returns all the possible column names
     * that can be selected from sales.dept table setting pos to 'Line 1, Column
     * 31' returns all the possible table names in 'sales' schema
     *
     * @return an array of {@link SqlMoniker} (sql identifiers) that can fill in
     * at the indicated position
     */
    public List<SqlMoniker> lookupHints(SqlNode topNode, SqlParserPos pos);

    /**
     * Looks up the fully qualified name for a {@link SqlIdentifier} at a given
     * Parser Position in a parsed expression tree Note: call this only after
     * {@link #validate} has been called.
     *
     * @param topNode top of expression tree in which to lookup the qualfied
     * name for the SqlIdentifier
     * @param pos indicates the position of the {@link SqlIdentifier} in the sql
     * statement we want to get the qualified name for
     *
     * @return a string of the fully qualified name of the {@link SqlIdentifier}
     * if the Parser position represents a valid {@link SqlIdentifier}. Else
     * return an empty string
     */
    public SqlMoniker lookupQualifiedName(SqlNode topNode, SqlParserPos pos);
}

// End SqlValidatorWithHints.java
