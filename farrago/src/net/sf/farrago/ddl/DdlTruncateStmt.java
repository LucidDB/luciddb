/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
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
package net.sf.farrago.ddl;

import java.util.*;

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

    private CwmTable table;

    private Collection<FemLocalIndex> tableIndexes;

    //~ Constructors -----------------------------------------------------------

    /**
     * Constructs a new DdlTruncateStmt.
     *
     * @param truncatedElement top-level element truncated by this stmt
     */
    public DdlTruncateStmt(CwmModelElement truncatedElement)
    {
        super(truncatedElement, true);
        this.table = (CwmTable) truncatedElement;
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
        tableIndexes = FarragoCatalogUtil.getTableIndexes(
            session.getRepos(), table);
    }

    // implement DdlMultipleTransactionStmt
    public void executeUnlocked(
        FarragoSessionDdlValidator ddlValidator,
        FarragoSession session)
    {
        FarragoSessionIndexMap baseIndexMap = ddlValidator.getIndexMap();
        FarragoDataWrapperCache wrapperCache = 
            ddlValidator.getDataWrapperCache();
        for (FemLocalIndex index : tableIndexes) {
            // REVIEW: SWZ: 2008-02-26: This method might inadvertently access
            // the repository outside a txn by navigating links on index.
            baseIndexMap.dropIndexStorage(wrapperCache, index, true);
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
        // REVIEW jvs 8-Dec-2008:  can anything cause
        // success=false?
        
        session.getPersonality().resetRowCounts((FemAbstractColumnSet) table);
    }
}

// End DdlTruncateStmt.java
