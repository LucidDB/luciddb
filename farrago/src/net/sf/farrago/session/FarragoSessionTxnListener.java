/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2007 The Eigenbase Project
// Copyright (C) 2006-2007 Disruptive Tech
// Copyright (C) 2006-2007 LucidEra, Inc.
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
package net.sf.farrago.session;

import java.util.*;

import org.eigenbase.relopt.*;


/**
 * FarragoSessionTxnListener defines an interface for listening to events on a
 * {@link FarragoSessionTxnMgr}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FarragoSessionTxnListener
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Notifies listener of a call to FarragoSessionTxnMgr.beginTxn.
     *
     * @param session session initiating transaction
     * @param txnId new transaction ID
     */
    public void transactionBegun(
        FarragoSession session,
        FarragoSessionTxnId txnId);

    /**
     * Notifies listener of the effect of a call to
     * FarragoSessionTxnMgr.accessTable.
     *
     * @param txnId ID of transaction in which access is occurring
     * @param localTableName qualified name of table as it is known in the local
     * catalog
     * @param accessType type of table access
     */
    public void tableAccessed(
        FarragoSessionTxnId txnId,
        List<String> localTableName,
        TableAccessMap.Mode accessType);

    /**
     * Notifies listener of a call to FarragoSessionTxnMgr.endTxn.
     *
     * @param txnId ID of ending transaction
     * @param endType how transaction is ending
     */
    public void transactionEnded(
        FarragoSessionTxnId txnId,
        FarragoSessionTxnEnd endType);
}

// End FarragoSessionTxnListener.java
