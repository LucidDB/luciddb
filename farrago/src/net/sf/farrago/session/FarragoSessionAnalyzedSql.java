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
package net.sf.farrago.session;

import java.util.*;

import net.sf.farrago.cwm.core.*;
import net.sf.farrago.fem.sql2003.*;

import org.eigenbase.rel.metadata.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.util.SqlString;


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
     * The text of the SQL expression before expansion by the validator.
     */
    public SqlString rawString;

    /**
     * The text of the SQL expression after expansion by the validator. This
     * contains no context-dependent information (e.g. all objects are fully
     * qualified), so it can be stored in the catalog.
     *
     * <p>Its type is {@link SqlString} to remind developers to apply hygienic
     * practices to prevent SQL injection and poorly quoted identifiers and
     * literals.
     */
    public SqlString canonicalString;

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
