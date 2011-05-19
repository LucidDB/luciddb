/*
// $Id$
// Applib is a library of SQL-invocable routines for Eigenbase applications.
// Copyright (C) 2006 The Eigenbase Project
// Copyright (C) 2006 SQLstream, Inc.
// Copyright (C) 2006 DynamoBI Corporation
//
// This library is free software; you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation; either version 2.1 of the License, or (at
// your option) any later version.
//
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public
// License along with this library; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
*/
package org.eigenbase.applib.variable;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.instance.*;

import org.eigenbase.applib.resource.*;


/**
 * SQL-invocable procedure to set the value of an application variable.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class SetAppVarUdp
{
    //~ Methods ----------------------------------------------------------------

    public static void execute(String contextId, String varId, String newValue)
    {
        if (varId == null) {
            throw ApplibResource.instance().AppVarIdRequired.ex();
        }
        FarragoRepos repos = null;
        FarragoReposTxnContext txn = null;
        try {
            repos = AppVarUtil.getRepos();
            txn = repos.newTxnContext(true);
            txn.beginWriteTxn();
            CwmExtent context = AppVarUtil.lookupContext(repos, contextId);
            repos.setTagValue(
                context,
                varId,
                (newValue == null) ? AppVarUtil.NULL_APPVAR_VALUE : newValue);
            txn.commit();
        } catch (Throwable ex) {
            throw ApplibResource.instance().AppVarWriteFailed.ex(
                contextId,
                varId,
                ex);
        } finally {
            if (txn != null) {
                txn.rollback();
            }
        }
    }
}

// End SetAppVarUdp.java
