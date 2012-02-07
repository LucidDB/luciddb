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
