/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2006 LucidEra, Inc.
// Copyright (C) 2006-2006 The Eigenbase Project
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
package com.lucidera.farrago;

import java.util.*;
import java.util.logging.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.db.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.session.*;

import org.apache.commons.transaction.locking.*;
import org.apache.commons.transaction.util.*;

import org.eigenbase.relopt.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.util.*;


/**
 * LucidDbTxnMgr implements the {@link FarragoSessionTxnMgr} interface with
 * locking semantics customized for LucidDB.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class LucidDbTxnMgr
    extends FarragoDbNullTxnMgr
{
    //~ Static fields/initializers ---------------------------------------------

    // TODO jvs 15-Mar-2006:  start a new LucidDbTrace.java file?
    private static final Logger tracer =
        Logger.getLogger(LucidDbTxnMgr.class.getName());

    //~ Instance fields --------------------------------------------------------

    private final LockManager2 lockMgr;

    //~ Constructors -----------------------------------------------------------

    LucidDbTxnMgr()
    {
        // NOTE jvs 8-Feb-2007:  This does nothing unless someone
        // actually enables the corresponding log4j settings AND
        // java.util.logging settings
        LoggerFacade loggerFacade = new Jdk14Logger(tracer);
        lockMgr = new GenericLockManager(2, loggerFacade);
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoSessionTxnMgr
    public FarragoSessionTxnId beginTxn(FarragoSession session)
    {
        return super.beginTxn(session);
    }

    // override FarragoDbNullTxnMgr
    protected void accessTable(
        FarragoSessionTxnId txnId,
        List<String> localTableName,
        TableAccessMap.Mode accessType)
    {
        super.accessTable(txnId, localTableName, accessType);

        SqlIdentifier sqlId =
            new SqlIdentifier(
                (String []) localTableName.toArray(Util.emptyStringArray),
                SqlParserPos.ZERO);
        String renderedTableName = sqlId.toString();

        if (accessType != TableAccessMap.Mode.READ_ACCESS) {
            // X-lock the table to exclude writers on the same table
            acquireLock(txnId, localTableName, renderedTableName, 2);
        }
    }

    // implement FarragoSessionTxnMgr
    public void endTxn(
        FarragoSessionTxnId txnId,
        FarragoSessionTxnEnd endType)
    {
        super.endTxn(txnId, endType);
        tracer.fine(
            "Transaction " + txnId + " releasing all table and database locks");
        lockMgr.releaseAll(txnId);
    }

    private void acquireLock(
        FarragoSessionTxnId txnId,
        Object resourceId,
        String renderedName,
        int lockLevel)
    {
        tracer.fine(
            "Transaction " + txnId + " attempting to acquire "
            + ((lockLevel == 1) ? "shared" : "exclusive")
            + " lock on "
            + renderedName);
        if (lockMgr.tryLock(txnId, resourceId, lockLevel, true)) {
            tracer.fine(
                "Transaction " + txnId + " acquired lock successfully");
            return;
        }
        throw FarragoResource.instance().LockDenied.ex(
            renderedName);
    }
}

// End LucidDbTxnMgr.java
