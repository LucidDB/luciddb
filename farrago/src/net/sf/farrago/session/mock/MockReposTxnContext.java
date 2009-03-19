/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2007-2009 The Eigenbase Project
// Copyright (C) 2007-2009 SQLstream, Inc.
// Copyright (C) 2007-2009 LucidEra, Inc.
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
package net.sf.farrago.session.mock;

import net.sf.farrago.catalog.*;


/**
 * MockReposTxnContext overrides FarragoReposTxnContext so that no FarragoRepos
 * implementation is needed. Note that this class does not actually create any
 * transaction or lock. See {@link MockSessionStmtValidator}.
 *
 * @author stephan/jack
 * @version $Id$
 * @since Dec 8, 2006
 */
public class MockReposTxnContext
    extends FarragoReposTxnContext
{
    //~ Instance fields --------------------------------------------------------

    private boolean isRead;
    private boolean isWrite;
    private boolean locked;

    //~ Constructors -----------------------------------------------------------

    public MockReposTxnContext()
    {
        super(null);
    }

    //~ Methods ----------------------------------------------------------------

    public void beginReadTxn()
    {
        assert (!isRead && !isWrite);
        isRead = true;
    }

    public void beginWriteTxn()
    {
        assert (!isRead && !isWrite);
        isWrite = true;
    }

    public void commit()
    {
        isRead = isWrite = locked = false;
    }

    public boolean isReadTxnInProgress()
    {
        return isRead;
    }

    public boolean isTxnInProgress()
    {
        return isRead || isWrite || locked;
    }

    public void rollback()
    {
        isRead = isWrite = locked = false;
    }

    public void beginLockedTxn(boolean readOnly)
    {
        assert !isRead && !isWrite && !locked;
        locked = true;
        if (readOnly) {
            isRead = true;
        } else {
            isWrite = true;
        }
    }

    public void unlockAfterTxn()
    {
        isRead = isWrite = locked = false;
    }
}

// End MockReposTxnContext.java
