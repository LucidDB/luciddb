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
package net.sf.farrago.catalog;

/**
 * FarragoReposTxnContext manages the state of at most one repository
 * transaction. A context may be inactive, meaning it has no current
 * transaction.
 *
 *<p>
 *
 * Always use the following exception-safe transaction pattern:
 *
 *<pre><code>
 *   FarragoReposTxnContext txn = repos.newTxnContext();
 *   try {
 *       txn.beginWriteTxn();
 *       ... do stuff which accesses repository ...
 *       txn.commit();
 *   } finally {
 *       // no effect if already committed or beginWriteTxn failed
 *       txn.rollback();
 *   }
 *</code></pre>
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoReposTxnContext
{

    //~ Instance fields --------------------------------------------------------

    private FarragoRepos repos;

    private enum State
    {
        NO_TXN,

        READ_TXN,

        WRITE_TXN
    };

    private State state;
    private int lockLevel;


    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new inactive transaction context.
     *
     * @param repos the repos against which transactions are to be performed
     */
    public FarragoReposTxnContext(FarragoRepos repos)
    {
        this.repos = repos;
        state = State.NO_TXN;
        lockLevel = 0;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * @return whether a transaction is currently in progress
     */
    public boolean isTxnInProgress()
    {
        return state != State.NO_TXN;
    }

    /**
     * @return whether a read-only transaction is currently in progress
     */
    public boolean isReadTxnInProgress()
    {
        return state == State.READ_TXN;
    }
    
    /**
     * Begins a new read-only transaction.
     */
    public void beginReadTxn()
    {
        assert (!isTxnInProgress());
        repos.beginReposTxn(false);
        state = State.READ_TXN;
    }

    /**
     * Begins a new read/write transaction.
     */
    public void beginWriteTxn()
    {
        assert (!isTxnInProgress());

        // NOTE jvs 12-Jan-2007:  don't change state until AFTER successfully
        // beginning a transaction; if beginReposTxn throws an excn,
        // we want to stay in State.NO_TXN
        
        repos.beginReposTxn(true);
        state = State.WRITE_TXN;
    }

    /**
     * Commits the active transaction, if any.
     */
    public void commit()
    {
        if (!isTxnInProgress()) {
            return;
        }
        
        // NOTE jvs 12-Jan-2007:  change state BEFORE attempting
        // to end transaction; if endReposTxn throws an excn,
        // we're in an unknown state, but further calls could just
        // mask the original excn, so pretend we're back to
        // State.NO_TXN regardless.
        
        state = State.NO_TXN;
        repos.endReposTxn(false);
    }

    /**
     * Rolls back the active transaction, if any.
     */
    public void rollback()
    {
        if (!isTxnInProgress()) {
            return;
        }

        // NOTE jvs 12-Jan-2007:  see comment in commit() for ordering rationale
        
        state = State.NO_TXN;
        repos.endReposTxn(true);
    }
    
    /**
     * Acquires a repository lock and begins a matching MDR transaction (shared
     * lock for read, or exclusive lock for write).  Typical usage is start of
     * SQL statement preparation (e.g.  readOnly=true for DML or query, false
     * for DDL).
     * 
     * @param readOnly if true, a shared lock is acquired on the
     * catalog; otherwise, an exclusive lock is acquired
     */
    public void beginLockedTxn(boolean readOnly)
    {
        lockLevel = (readOnly) ? 1 : 2;

        // TODO jvs 24-Jan-2007:  Get rid of downcast here and below by
        // making all creation of FarragoReposTxnContext go through
        // factory method interface on FarragoRepos.
        
        ((FarragoReposImpl) repos).lockRepos(lockLevel);
        
        if (readOnly) {
            beginReadTxn();
        } else {
            beginWriteTxn();
        }
    }
    
    /**
     * Releases lock acquired by beginLockedTxn.  Caller should
     * already have ended transaction with either commit or rollback.
     */
    public void unlockAfterTxn()
    {
        if (lockLevel != 0) {
            ((FarragoReposImpl) repos).unlockRepos(lockLevel);
            lockLevel = 0;
        }       
    }
}

// End FarragoReposTxnContext.java
