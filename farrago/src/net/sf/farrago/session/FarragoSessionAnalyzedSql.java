/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
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
package net.sf.farrago.session;

import java.sql.*;
import java.util.*;

import org.eigenbase.reltype.*;

/**
 * FarragoSessionAnalyzedSql contains the results of the analyzeSql
 * call used while processing DDL statements such as CREATE VIEW
 * and CREATE FUNCTION which contain SQL expressions.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoSessionAnalyzedSql
{
    //~ Instance fields -------------------------------------------------------

    /**
     * The text of the SQL expression after expansion by the validator.  This
     * contains no context-dependent information (e.g. all objects are fully
     * qualified), so it can be stored in the catalog.
     */
    public String canonicalString;

    /**
     * Set of catalog objects on which the expression directly
     * depends.
     */
    public Set dependencies;

    /**
     * Metadata for result set returned when the expression is executed:
     * a row type for a query expression, or a single type
     * for a non-query expression.
     */
    public RelDataType resultType;

    /**
     * Metadata for parameters used as input to the expression.
     */
    public RelDataType paramRowType;

    /**
     * True if the expression is a query with a top-level ORDER BY.
     */
    public boolean hasTopLevelOrderBy;

    /**
     * True if the expression contains dynamic parameter markers.
     */
    public boolean hasDynamicParams;
}


// End FarragoSessionAnalyzedSql.java
