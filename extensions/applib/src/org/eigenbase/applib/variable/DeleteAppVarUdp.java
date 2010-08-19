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
 * SQL-invocable procedure to delete an application variable or context.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class DeleteAppVarUdp
{
    //~ Methods ----------------------------------------------------------------

    public static void execute(String contextId, String varId)
    {
        FarragoRepos repos = null;
        FarragoReposTxnContext txn = null;
        try {
            repos = AppVarUtil.getRepos();
            txn = repos.newTxnContext(true);
            txn.beginWriteTxn();
            CwmExtent context = AppVarUtil.lookupContext(repos, contextId);
            if (varId == null) {
                context.refDelete();
            } else {
                CwmTaggedValue tag =
                    AppVarUtil.lookupVariable(
                        repos,
                        context,
                        varId);
                tag.refDelete();
            }
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

// End DeleteAppVarUdp.java
