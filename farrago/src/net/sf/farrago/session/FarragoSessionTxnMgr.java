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

import org.eigenbase.relopt.*;


/**
 * FarragoSessionTxnMgr defines the interface for transaction management across
 * sessions. It is under development and currently only addresses table access,
 * so it is likely to change drastically. In particular, it will be refined to
 * allow different data wrappers to use different transaction managers; once
 * that happens, most extensions will probably use a common implementation which
 * knows how to coordinate two-phase commits across wrapper-level managers.
 * Until then, transaction management is up to each extension. Another major
 * change required is coordination with Fennel's notion of transactions.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FarragoSessionTxnMgr
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Adds a listener for transaction events.
     *
     * @param listener new listener
     */
    public void addListener(
        FarragoSessionTxnListener listener);

    /**
     * Removes a listener for transaction events.
     *
     * @param listener listener to remove
     */
    public void removeListener(
        FarragoSessionTxnListener listener);

    /**
     * Begins a new transaction.
     *
     * @param session session initiating the transaction
     *
     * @return transaction ID
     */
    public FarragoSessionTxnId beginTxn(
        FarragoSession session);

    /**
     * Notifies transaction manager that a collection of tables is about to be
     * accessed.
     *
     * @param txnId ID of accessing transaction
     * @param tableAccessMap information about planned table accesses
     */
    public void accessTables(
        FarragoSessionTxnId txnId,
        TableAccessMap tableAccessMap);

    /**
     * Notifies transaction manager that a transaction is ending.
     *
     * @param txnId ID of ending transaction
     * @param endType how transaction is ending
     */
    public void endTxn(
        FarragoSessionTxnId txnId,
        FarragoSessionTxnEnd endType);
}

// End FarragoSessionTxnMgr.java
