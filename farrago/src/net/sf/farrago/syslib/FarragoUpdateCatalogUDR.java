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
package net.sf.farrago.syslib;

import java.sql.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.fem.config.*;
import net.sf.farrago.runtime.*;
import net.sf.farrago.session.*;


/**
 * FarragoUpdateCatalogUDR implements system procedures for updating Farrago
 * catalogs (intended for use as part of installation).
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class FarragoUpdateCatalogUDR
    extends FarragoAbstractCatalogInit
{
    //~ Constructors -----------------------------------------------------------

    private FarragoUpdateCatalogUDR(FarragoRepos repos)
    {
        super(repos);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Updates Farrago config objects in cases where they have been set to dummy
     * values.
     */
    public static void updateConfiguration()
    {
        tracer.info("Updating Farrago system parameters");

        //FarragoSession session = FarragoUdrRuntime.getSession();
        //FarragoRepos repos = session.getRepos();
        FarragoRepos repos = FarragoUdrRuntime.getRepos();

        FarragoUpdateCatalogUDR init = null;
        FarragoReposTxnContext txn = repos.newTxnContext(true);
        boolean rollback = true;
        try {
            try {
                txn.beginWriteTxn();
                init = new FarragoUpdateCatalogUDR(repos);
                init.updateSystemParameters();
                rollback = false;
            } finally {
                if (init != null) {
                    // Guarantee that publishObjects is called
                    init.publishObjects(rollback);
                }
            }
        } finally {
            // Guarantee that the txn is cleaned up
            if (rollback) {
                txn.rollback();
            } else {
                txn.commit();
            }
        }
        tracer.info("Update of Farrago system parameters complete");
    }

    /**
     * Updates catalogs with system objects, adding them if they don't exist
     * yet.
     */
    public static void updateSystemObjects()
    {
        tracer.info("Updating system-owned catalog objects");
        FarragoRepos repos = FarragoUdrRuntime.getRepos();

        FarragoUpdateCatalogUDR init = null;
        FarragoReposTxnContext txn = repos.newTxnContext(true);
        boolean rollback = true;
        try {
            try {
                txn.beginWriteTxn();
                init = new FarragoUpdateCatalogUDR(repos);
                init.updateSystemTypes();
                rollback = true;
            } finally {
                if (init != null) {
                    // Guarantee that publishObjects is called
                    init.publishObjects(rollback);
                }
            }
        } finally {
            // Guarantee that the txn is cleaned up
            if (rollback) {
                txn.rollback();
            } else {
                txn.commit();
            }
        }
        tracer.info("Update of system-owned catalog objects committed");
    }
}

// End FarragoUpdateCatalogUDR.java
