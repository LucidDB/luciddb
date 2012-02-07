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

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.relational.*;

import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.validate.*;


/**
 * An abstract base for implementations of RelOptTable which access data
 * described by Farrago's catalog.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class FarragoQueryNamedColumnSet
    extends RelOptAbstractTable
    implements FarragoQueryColumnSet
{
    //~ Instance fields --------------------------------------------------------

    /**
     * Catalog definition of column set.
     */
    private CwmNamedColumnSet cwmColumnSet;

    /**
     * Refinement for RelOptAbstractTable.schema.
     */
    private FarragoPreparingStmt preparingStmt;

    /**
     * Allowed access
     */
    private SqlAccessType allowedAccess;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FarragoQueryNamedColumnSet object.
     *
     * @param cwmColumnSet catalog definition for column set
     * @param rowType type for rows stored in column set
     */
    FarragoQueryNamedColumnSet(
        CwmNamedColumnSet cwmColumnSet,
        RelDataType rowType)
    {
        super(
            null,
            cwmColumnSet.getName(),
            rowType);
        this.cwmColumnSet = cwmColumnSet;
        this.allowedAccess =
            FarragoCatalogUtil.getTableAllowedAccess(cwmColumnSet);
    }

    //~ Methods ----------------------------------------------------------------

    // override RelOptAbstractTable
    public String [] getQualifiedName()
    {
        SqlIdentifier id = FarragoCatalogUtil.getQualifiedName(cwmColumnSet);
        return id.names;
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

    // implement FarragoMedColumnSet
    public String[] getForeignName()
    {
        // this method is not appropriate to all column sets; subclasses for
        // which it is appropriate should override
        throw new UnsupportedOperationException();
    }

    // implement FarragoMedColumnSet
    public final String[] getLocalName()
    {
        return getQualifiedName();
    }
}

// End FarragoQueryNamedColumnSet.java
