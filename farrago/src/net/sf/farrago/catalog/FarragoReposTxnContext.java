/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

package net.sf.farrago.catalog;

/**
 * FarragoReposTxnContext manages the state of at most one repository
 * transaction.  A context may be inactive, meaning it has no
 * current transaction.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoReposTxnContext
{
    private FarragoRepos repos;

    private boolean isTxnInProgress;

    /**
     * Creates a new inactive transaction context.
     *
     * @param repos the repos against which transactions are to be performed
     */
    public FarragoReposTxnContext(FarragoRepos repos)
    {
        this.repos = repos;
    }

    /**
     * @return whether a transaction is currently in progress
     */
    public boolean isTxnInProgress()
    {
        return isTxnInProgress;
    }

    /**
     * Begins a new read-only transaction.
     */
    public void beginReadTxn()
    {
        assert(!isTxnInProgress);
        repos.beginReposTxn(false);
        isTxnInProgress = true;
    }

    /**
     * Begins a new read/write transaction.
     */
    public void beginWriteTxn()
    {
        assert(!isTxnInProgress);
        repos.beginReposTxn(true);
        isTxnInProgress = true;
    }

    /**
     * Commits the active transaction, if any.
     */
    public void commit()
    {
        if (!isTxnInProgress) {
            return;
        }
        repos.endReposTxn(false);
        isTxnInProgress = false;
    }

    /**
     * Rolls back the active transaction, if any.
     */
    public void rollback()
    {
        if (!isTxnInProgress) {
            return;
        }
        repos.endReposTxn(true);
        isTxnInProgress = false;
    }
}

// End FarragoReposTxnContext.java
