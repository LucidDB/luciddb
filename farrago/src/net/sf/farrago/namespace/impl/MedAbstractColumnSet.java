/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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
package net.sf.farrago.namespace.impl;

import java.util.*;

import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.namespace.*;
import net.sf.farrago.query.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.catalog.*;

import org.eigenbase.rex.*;
import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;


/**
 * MedAbstractColumnSet is an abstract base class for implementations
 * of the {@link FarragoMedColumnSet} interface.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class MedAbstractColumnSet extends RelOptAbstractTable
    implements FarragoQueryColumnSet
{
    //~ Instance fields -------------------------------------------------------

    private final String [] localName;
    private final String [] foreignName;
    private Properties tableProps;
    private Map columnPropMap;
    private FarragoPreparingStmt preparingStmt;
    private CwmNamedColumnSet cwmColumnSet;
    private SqlAccessType allowedAccess;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new MedAbstractColumnSet.
     *
     * @param localName name of this ColumnSet as it will be known
     * within the Farrago system
     *
     * @param foreignName name of this ColumnSet as it is known
     * on the foreign server; may be null if no meaningful name
     * exists
     *
     * @param rowType row type descriptor
     *
     * @param tableProps table-level properties
     *
     * @param columnPropMap column-level properties (map from column name to
     * Properties object)
     */
    protected MedAbstractColumnSet(
        String [] localName,
        String [] foreignName,
        RelDataType rowType,
        Properties tableProps,
        Map columnPropMap)
    {
        super(null, localName[localName.length - 1], rowType);
        this.localName = localName;
        this.foreignName = foreignName;
        this.tableProps = tableProps;
        this.columnPropMap = columnPropMap;
        this.allowedAccess = SqlAccessType.ALL;
    }

    //~ Methods ---------------------------------------------------------------

    // implement RelOptTable
    public String [] getQualifiedName()
    {
        return localName;
    }

    /**
     * @return the name this ColumnSet is known by within
     * the Farrago system
     */
    public String [] getLocalName()
    {
        return localName;
    }

    /**
     * @return the name of this ColumnSet as it is known on the foreign server
     */
    public String [] getForeignName()
    {
        return foreignName;
    }

    /**
     * @return options specified by CREATE FOREIGN TABLE
     */
    public Properties getTableProperties()
    {
        return tableProps;
    }

    /**
     * @return map (from column name to Properties) of column options specified
     * by CREATE FOREIGN TABLE
     */
    public Map getColumnPropertyMap()
    {
        return columnPropMap;
    }

    // implement FarragoQueryColumnSet
    public FarragoPreparingStmt getPreparingStmt()
    {
        return preparingStmt;
    }

    // implement FarragoQueryColumnSet
    public void setPreparingStmt(FarragoPreparingStmt stmt)
    {
        preparingStmt = stmt;
    }

    // implement FarragoQueryColumnSet
    public void setCwmColumnSet(CwmNamedColumnSet cwmColumnSet)
    {
        this.cwmColumnSet = cwmColumnSet;
    }

    // implement FarragoQueryColumnSet
    public CwmNamedColumnSet getCwmColumnSet()
    {
        return cwmColumnSet;
    }

    // implement SqlValidatorTable
    public boolean isMonotonic(String columnName)
    {
        return false;
    }

    // implement SqlValidatorTable
    public SqlAccessType getAllowedAccess()
    {
        return allowedAccess;
    }

    public void setAllowedAccess(SqlAccessType allowedAccess)
    {
        this.allowedAccess = allowedAccess;
    }

    /**
     * Provides an implementation of the toRel interface method
     * in terms of an underlying UDX.
     *
     * @param cluster same as for toRel
     *
     * @param connection same as for toRel
     *
     * @param udxSpecificName specific name with which the UDX was created
     * (either via the SPECIFIC keyword or the invocation name if SPECIFIC was
     * not specified); this can be a qualified name, possibly with quoted
     * identifiers, e.g. x.y.z or x."y".z
     *
     * @param serverMofId if not null, the invoked UDX can access the data
     * server with the given MOFID at runtime via {@link
     * FarragoUdrRuntime.getDataServerRuntimeSupport}
     *
     * @param args arguments to UDX invocation
     *
     * @return generated relational expression producing the UDX results
     */
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
        List<SqlOperator> list =
            getPreparingStmt().getSqlOperatorTable().lookupOperatorOverloads(
                udxId,
                SqlFunctionCategory.UserDefinedSpecificFunction,
                SqlSyntax.Function);
        FarragoUserDefinedRoutine udx = null;
        if (list.size() == 1) {
            SqlOperator obj = list.get(0);
            if (obj instanceof FarragoUserDefinedRoutine) {
                udx = (FarragoUserDefinedRoutine) obj;
                if (!FarragoCatalogUtil.isTableFunction(udx.getFemRoutine())) {
                    // Not a UDX.
                    udx = null;
                }
            }
        }
        if (udx == null) {
            throw FarragoResource.instance().MedUnknownUdx.ex(
                udxId.toString());
        }

        // UDX wants all types nullable, so construct a corresponding
        // type descriptor for the result of the call.
        RexBuilder rexBuilder = cluster.getRexBuilder();
        RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
        RelDataType resultType = typeFactory.createTypeWithNullability(
            getRowType(), true);
        
        // Create a relational algebra expression for invoking the UDX.
        RexNode rexCall = rexBuilder.makeCall(udx, args);
        RelNode udxRel =
            new FarragoJavaUdxRel(
                cluster, rexCall, resultType, serverMofId,
                RelNode.emptyArray);

        // Optimizer wants us to preserve original types,
        // so cast back for the final result.
        return RelOptUtil.createCastRel(
            udxRel,
            getRowType(),
            true);
    }
}


// End MedAbstractColumnSet.java
