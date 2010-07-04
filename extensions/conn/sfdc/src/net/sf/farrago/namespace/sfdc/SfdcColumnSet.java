/*
// $Id$
// SFDC Connector is an Eigenbase SQL/MED connector for Salesforce.com
// Copyright (C) 2010 The Eigenbase Project
// Copyright (C) 2010 SQLstream, Inc.
// Copyright (C) 2010 DynamoBI Corporation
//
// This library is free software; you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation; either version 2.1 of the License, or (at
// your option) any later version.
//
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public
// License along with this library; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
 */
package net.sf.farrago.namespace.sfdc;

import java.util.List;

import net.sf.farrago.catalog.FarragoCatalogUtil;
import net.sf.farrago.namespace.FarragoMedColumnSet;
import net.sf.farrago.namespace.impl.MedAbstractColumnSet;
import net.sf.farrago.query.FarragoUserDefinedRoutine;
import net.sf.farrago.resource.FarragoResource;

import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.SqlParser;


/**
 * SfdcColumnSet provides an implementation of the {@link FarragoMedColumnSet}
 * interface.
 *
 * @author Sunny Choi
 * @version $Id$
 */
class SfdcColumnSet
    extends MedAbstractColumnSet
{
    //~ Static fields/initializers ---------------------------------------------

    public static final String PROP_OBJECT = "OBJECT";

    //~ Instance fields --------------------------------------------------------

    // ~ Instance fields -------------------------------------------------------

    final SfdcDataServer server;
    String udxSpecificName;
    String object;
    String fields;
    String types;
    RelDataType origRowType;
    RelDataType srcRowType;
    RelDataType rowType;

    //~ Constructors -----------------------------------------------------------

    // ~ Constructors ----------------------------------------------------------

    SfdcColumnSet(
        SfdcDataServer server,
        String [] localName,
        RelDataType rowType,
        RelDataType origRowType,
        RelDataType srcRowType,
        String object,
        String fields,
        String types)
    {
        super(localName, null, origRowType, null, null);

        this.server = server;
        this.object = object;
        this.fields = fields;
        this.types = types;
        this.origRowType = origRowType;
        this.srcRowType = srcRowType;
        this.rowType = rowType;

        if (this.object.endsWith("_LOV")) {
            this.udxSpecificName = "sys_boot.farrago.sfdc_lov";
            this.object = object.substring(0, (object.length() - 4));
        } else if (this.object.endsWith("_deleted")) {
            this.udxSpecificName = "sys_boot.farrago.sfdc_deleted";
            this.object = object.substring(0, (object.length() - 8));
        } else {
            this.udxSpecificName = "sys_boot.farrago.sfdc_query";
        }
    }

    //~ Methods ----------------------------------------------------------------

    // ~ Methods ---------------------------------------------------------------

    // implement RelOptTable
    public RelNode toRel(RelOptCluster cluster, RelOptConnection connection)
    {
        RexBuilder rexBuilder = cluster.getRexBuilder();
        RexNode [] rexNodes = null;

        // lov
        if (this.udxSpecificName.equals("sys_boot.farrago.sfdc_lov")) {
            RexNode arg1 = rexBuilder.makeLiteral(this.object);
            rexNodes = new RexNode[] { arg1 };
            // deleted
        } else if (
            this.udxSpecificName.equals("sys_boot.farrago.sfdc_deleted"))
        {
            RexNode arg1 = rexBuilder.makeLiteral(this.object);
            RexNode arg2 = rexBuilder.makeLiteral("");
            RexNode arg3 = rexBuilder.makeLiteral("");
            rexNodes = new RexNode[] { arg1, arg2, arg3 };
            // query
        } else {
            String query = "select " + fields + " from " + object;
            RexNode arg1 = rexBuilder.makeLiteral(query);
            RexNode arg2 = rexBuilder.makeLiteral(types);
            rexNodes = new RexNode[] { arg1, arg2 };
        }

        // Call to super handles the rest.
        RelNode udxRel =
            toUdxRel(
                cluster,
                connection,
                this.udxSpecificName,
                server.getServerMofId(),
                rexNodes);

        return toLenientRel(cluster, udxRel, this.origRowType, this.srcRowType);
    }

    // override FarragoMedColumnSet
    // return a SfdcUdxRel instead of a FarragoJavaUdxRel
    protected RelNode toUdxRel(
        RelOptCluster cluster,
        RelOptConnection connection,
        String udxSpecificName,
        String serverMofId,
        RexNode [] args)
    {
        // Parse the specific name of the UDX.
        SqlIdentifier udxId;
        try {
            SqlParser parser = new SqlParser(udxSpecificName);
            SqlNode parsedId = parser.parseExpression();
            udxId = (SqlIdentifier) parsedId;
        } catch (Exception ex) {
            throw FarragoResource.instance().MedInvalidUdxId.ex(
                udxSpecificName,
                ex);
        }

        // Look up the UDX in the catalog.
        List list =
            getPreparingStmt().getSqlOperatorTable().lookupOperatorOverloads(
                udxId,
                SqlFunctionCategory.UserDefinedSpecificFunction,
                SqlSyntax.Function);
        FarragoUserDefinedRoutine udx = null;
        if (list.size() == 1) {
            Object obj = list.iterator().next();
            if (obj instanceof FarragoUserDefinedRoutine) {
                udx = (FarragoUserDefinedRoutine) obj;
                if (!FarragoCatalogUtil.isTableFunction(udx.getFemRoutine())) {
                    // Not a UDX.
                    udx = null;
                }
            }
        }
        if (udx == null) {
            throw FarragoResource.instance().MedUnknownUdx.ex(udxId.toString());
        }

        // UDX wants all types nullable, so construct a corresponding
        // type descriptor for the result of the call.
        RexBuilder rexBuilder = cluster.getRexBuilder();
        RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
        RelDataType resultType =
            typeFactory.createTypeWithNullability(
                this.rowType,
                true);

        // Create a relational algebra expression for invoking the UDX.
        RexNode rexCall = rexBuilder.makeCall(udx, args);

        RelNode udxRel =
            new SfdcUdxRel(
                cluster,
                rexCall,
                resultType,
                serverMofId,
                this,
                udx);

        // Optimizer wants us to preserve original types,
        // so cast back for the final result.
        return RelOptUtil.createCastRel(udxRel, this.rowType, true);
    }
}

// End SfdcColumnSet.java
