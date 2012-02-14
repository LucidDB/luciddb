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
package net.sf.farrago.ddl;

import java.util.*;

import javax.jmi.reflect.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.namespace.util.*;
import net.sf.farrago.session.*;


/**
 * DdlTruncateStmt represents a DDL TRUNCATE statement of any kind.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class DdlTruncateStmt
    extends DdlStmt
    implements DdlMultipleTransactionStmt
{
    //~ Instance fields --------------------------------------------------------

    private String tableMofId;
    private RefClass tableClass;
    private List<String> indexMofIds;

    //~ Constructors -----------------------------------------------------------

    /**
     * Constructs a new DdlTruncateStmt.
     *
     * @param truncatedElement top-level element truncated by this stmt
     */
    public DdlTruncateStmt(CwmModelElement truncatedElement)
    {
        super(truncatedElement, true);
        tableMofId = truncatedElement.refMofId();
        tableClass = truncatedElement.refClass();
    }

    //~ Methods ----------------------------------------------------------------

    // implement DdlStmt
    public void visit(DdlVisitor visitor)
    {
        visitor.visit(this);
    }

    // implement DdlMultipleTransactionStmt
    public void prepForExecuteUnlocked(
        FarragoSessionDdlValidator ddlValidator,
        FarragoSession session)
    {
        indexMofIds = new ArrayList<String>();
        CwmTable table = (CwmTable) getModelElement();
        Collection<FemLocalIndex> tableIndexes =
            FarragoCatalogUtil.getTableIndexes(session.getRepos(), table);
        for (FemLocalIndex index : tableIndexes) {
            indexMofIds.add(index.refMofId());
        }
    }

    // implement DdlMultipleTransactionStmt
    public void executeUnlocked(
        FarragoSessionDdlValidator ddlValidator,
        FarragoSession session)
    {
        FarragoSessionIndexMap baseIndexMap = ddlValidator.getIndexMap();
        FarragoDataWrapperCache wrapperCache =
            ddlValidator.getDataWrapperCache();
        for (String indexMofId : indexMofIds) {
            baseIndexMap.dropIndexStorage(wrapperCache, indexMofId, true);
        }
    }

    // implement DdlMultipleTransactionStmt
    public boolean completeRequiresWriteTxn()
    {
        return true;
    }

    // implement DdlMultipleTransactionStmt
    public void completeAfterExecuteUnlocked(
        FarragoSessionDdlValidator ddlValidator,
        FarragoSession session,
        boolean success)
    {
        if (!success) {
            // NOTE jvs 11-Dec-2008:  I'm not sure whether anything
            // can cause a TRUNCATE to fail, but if it does fail, we
            // shouldn't reset the rowcounts.
            return;
        }
        FemAbstractColumnSet table =
            (FemAbstractColumnSet) session.getRepos().getEnkiMdrRepos()
            .getByMofId(
                tableMofId,
                tableClass);
        session.getPersonality().resetRowCounts(table);
    }
}

// End DdlTruncateStmt.java
