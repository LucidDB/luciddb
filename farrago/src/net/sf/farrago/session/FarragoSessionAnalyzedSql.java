/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 SQLstream, Inc.
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
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

import java.util.*;

import net.sf.farrago.cwm.core.*;
import net.sf.farrago.fem.sql2003.*;

import org.eigenbase.rel.metadata.*;
import org.eigenbase.reltype.*;


/**
 * FarragoSessionAnalyzedSql contains the results of the analyzeSql call used
 * while processing SQL expressions contained by DDL statements such as CREATE
 * VIEW and CREATE FUNCTION.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoSessionAnalyzedSql
{
    //~ Instance fields --------------------------------------------------------

    /**
     * True if post-optimization analysis was requested. If false, some fields
     * noted below are invalid.
     */
    public boolean optimized;

    /**
     * The text of the SQL expression after expansion by the validator. This
     * contains no context-dependent information (e.g. all objects are fully
     * qualified), so it can be stored in the catalog.
     */
    public String canonicalString;

    /**
     * Set of catalog objects on which the expression directly depends.
     */
    public Set<CwmModelElement> dependencies;

    /**
     * Metadata for result set returned when the expression is executed: a row
     * type for a query expression, or a single type for a non-query expression.
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

    /**
     * Information about column origins, in same order as resultType row; null
     * if expression is not a query.
     */
    public List<Set<RelColumnOrigin>> columnOrigins;

    /**
     * Estimated number of rows returned; invalid if expression is not a query,
     * or no optimization was requested.
     */
    public double rowCount;

    //~ Constructors -----------------------------------------------------------

    public FarragoSessionAnalyzedSql()
    {
    }

    //~ Methods ----------------------------------------------------------------

    public void setResultType(RelDataType resultType)
    {
        this.resultType = resultType;
    }

    public void setModality(FemAbstractColumnSet colSet)
    {
        colSet.setModality(ModalityTypeEnum.MODALITYTYPE_RELATIONAL);
    }
}

// End FarragoSessionAnalyzedSql.java
