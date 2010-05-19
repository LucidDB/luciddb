/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 2003 John V. Sichi
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
