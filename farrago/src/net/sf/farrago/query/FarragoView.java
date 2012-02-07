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
package net.sf.farrago.query;

import net.sf.farrago.fem.sql2003.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.util.SqlBuilder;
import org.eigenbase.util.*;


/**
 * An implementation of RelOptTable for accessing a view managed by Farrago.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FarragoView
    extends FarragoQueryNamedColumnSet
{
    //~ Instance fields --------------------------------------------------------

    private final String datasetName;
    private final ModalityType modality;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FarragoView object.
     *
     * @param cwmView catalog definition for view
     * @param rowType type for rows produced by view
     * @param datasetName Name of sample dataset, or null to use vanilla
     * @param modality Modality of the view (relational versus streaming)
     */
    FarragoView(
        FemLocalView cwmView,
        RelDataType rowType,
        String datasetName,
        ModalityType modality)
    {
        super(cwmView, rowType);
        this.datasetName = datasetName;
        this.modality = modality;
    }

    //~ Methods ----------------------------------------------------------------

    public FemLocalView getFemView()
    {
        return (FemLocalView) getCwmColumnSet();
    }

    // implement RelOptTable
    public RelNode toRel(
        RelOptCluster cluster,
        RelOptConnection connection)
    {
        String queryString = getFemView().getQueryExpression().getBody();
        if (datasetName != null) {
            final SqlBuilder buf = new SqlBuilder(SqlDialect.EIGENBASE);
            buf.append(
                ((modality == ModalityTypeEnum.MODALITYTYPE_STREAM)
                 ? "SELECT STREAM"
                 : "SELECT"))
                .append(" * FROM (")
                .append(queryString)
                .append(") AS x TABLESAMPLE SUBSTITUTE (")
                .literal(datasetName)
                .append(")");
            queryString = buf.getSql();
        }
        return expandView(queryString);
    }

    private RelNode expandView(String queryString)
    {
        try {
            // REVIEW:  cache view definition?
            RelNode rel =
                getPreparingStmt().expandView(
                    getRowType(),
                    queryString);

            // NOTE jvs 22-Jan-2007:  It would be nice if we could
            // state that we only need a rename here (not a cast)
            // since the view column types should have been updated
            // as part of doing CREATE OR REPLACE on any objects
            // the view depends on.  However, this is not the case
            // for direct SQL/MED references without an
            // explicit CREATE FOREIGN TABLE or IMPORT FOREIGN SCHEMA,
            // since there the external system can change at
            // any time.

            rel = RelOptUtil.createCastRel(rel, rowType, true);
            rel = getPreparingStmt().flattenTypes(rel, false);
            return rel;
        } catch (Throwable e) {
            throw Util.newInternal(
                e,
                "Error while parsing view definition:  " + queryString);
        }
    }
}

// End FarragoView.java
