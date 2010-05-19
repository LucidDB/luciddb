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
package net.sf.farrago.namespace.impl;

import java.util.*;

import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.namespace.*;
import net.sf.farrago.query.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.validate.*;


/**
 * MedAbstractColumnSet is an abstract base class for implementations of the
 * {@link FarragoMedColumnSet} interface.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class MedAbstractColumnSet
    extends RelOptAbstractTable
    implements FarragoQueryColumnSet
{
    //~ Instance fields --------------------------------------------------------

    private final String [] localName;
    private final String [] foreignName;
    private Properties tableProps;
    private Map<String, Properties> columnPropMap;
    private FarragoPreparingStmt preparingStmt;
    private CwmNamedColumnSet cwmColumnSet;
    private SqlAccessType allowedAccess;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new MedAbstractColumnSet.
     *
     * @param localName name of this ColumnSet as it will be known within the
     * Farrago system
     * @param foreignName name of this ColumnSet as it is known on the foreign
     * server; may be null if no meaningful name exists
     * @param rowType row type descriptor
     * @param tableProps table-level properties
     * @param columnPropMap column-level properties (map from column name to
     * Properties object)
     */
    protected MedAbstractColumnSet(
        String [] localName,
        String [] foreignName,
        RelDataType rowType,
        Properties tableProps,
        Map<String, Properties> columnPropMap)
    {
        super(null, localName[localName.length - 1], rowType);
        this.localName = localName;
        this.foreignName = foreignName;
        this.tableProps = tableProps;
        this.columnPropMap = columnPropMap;
        this.allowedAccess = SqlAccessType.ALL;
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptTable
    public String [] getQualifiedName()
    {
        return localName;
    }

    /**
     * @return the name this ColumnSet is known by within the Farrago system
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
    public Map<String, Properties> getColumnPropertyMap()
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
    public SqlMonotonicity getMonotonicity(String columnName)
    {
        return SqlMonotonicity.NotMonotonic;
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
     * Provides an implementation of the toRel interface method in terms of an
     * underlying UDX.
     *
     * @param cluster same as for toRel
     * @param connection same as for toRel
     * @param udxSpecificName specific name with which the UDX was created
     * (either via the SPECIFIC keyword or the invocation name if SPECIFIC was
     * not specified); this can be a qualified name, possibly with quoted
     * identifiers, e.g. x.y.z or x."y".z
     * @param serverMofId if not null, the invoked UDX can access the data
     * server with the given MOFID at runtime via {@link
     * net.sf.farrago.runtime.FarragoUdrRuntime#getDataServerRuntimeSupport}
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
        // TODO jvs 13-Oct-2006:  phase out these vestigial parameters
        assert (cluster == getPreparingStmt().getRelOptCluster());
        assert (connection == getPreparingStmt());

        return FarragoJavaUdxRel.newUdxRel(
            getPreparingStmt(),
            getRowType(),
            udxSpecificName,
            serverMofId,
            args,
            RelNode.emptyArray);
    }

    /**
     * Converts one RelNode to another RelNode with specified RowType. New
     * columns are filled with nulls.
     *
     * @param cluster same as for toRel
     * @param child original RelNode
     * @param targetRowType RowType to map to
     * @param srcRowType RowType of external data source
     */
    protected RelNode toLenientRel(
        RelOptCluster cluster,
        RelNode child,
        RelDataType targetRowType,
        RelDataType srcRowType)
    {
        ArrayList<RexNode> rexNodeList = new ArrayList();
        RexBuilder rexBuilder = cluster.getRexBuilder();
        FarragoWarningQueue warningQueue =
            getPreparingStmt().getStmtValidator().getWarningQueue();
        String objectName = this.localName[this.localName.length - 1];

        HashMap<String, RelDataType> srcMap = new HashMap();
        for (RelDataTypeField srcField : srcRowType.getFieldList()) {
            srcMap.put(srcField.getName(), srcField.getType());
        }

        ArrayList<String> allTargetFields = new ArrayList();
        int index = 0;
        for (RelDataTypeField targetField : targetRowType.getFieldList()) {
            allTargetFields.add(targetField.getName());
            RelDataType type;

            // target field is in child
            if ((index = child.getRowType().getFieldOrdinal(
                        targetField.getName()))
                != -1)
            {
                if ((type = srcMap.get(targetField.getName()))
                    != targetField.getType())
                {
                    // field type has been cast
                    warningQueue.postWarning(
                        FarragoResource.instance().TypeChangeWarning.ex(
                            objectName,
                            targetField.getName(),
                            type.toString(),
                            targetField.getType().toString()));
                }
                rexNodeList.add(
                    new RexInputRef(
                        index,
                        child.getRowType().getField(
                            targetField.getName()).getType()));
            } else { // target field is not in child
                rexNodeList.add(
                    rexBuilder.makeCast(
                        targetField.getType(),
                        rexBuilder.constantNull()));

                // check if type-incompatibility between source and target
                if ((type = srcMap.get(targetField.getName())) != null) {
                    warningQueue.postWarning(
                        FarragoResource.instance().IncompatibleTypeChangeWarning
                        .ex(
                            objectName,
                            targetField.getName(),
                            type.toString(),
                            targetField.getType().toString()));
                } else {
                    // field in target has been deleted in source
                    warningQueue.postWarning(
                        FarragoResource.instance().DeletedFieldWarning.ex(
                            objectName,
                            targetField.getName()));
                }
            }
        }

        // check if data source has added fields
        for (String srcField : srcMap.keySet()) {
            if (!allTargetFields.contains(srcField)) {
                warningQueue.postWarning(
                    FarragoResource.instance().AddedFieldWarning.ex(
                        objectName,
                        srcField));
            }
        }

        // create a new RelNode.
        RelNode calcRel =
            CalcRel.createProject(
                child,
                rexNodeList,
                null);

        return RelOptUtil.createCastRel(
            calcRel,
            targetRowType,
            true);
    }
}

// End MedAbstractColumnSet.java
