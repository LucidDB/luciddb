/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 SQLstream, Inc.
// Copyright (C) 2005-2007 LucidEra, Inc.
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

import net.sf.farrago.fem.security.*;
import net.sf.farrago.fem.sql2003.*;


/**
 * FarragoCatalogInit contains one-time persistent initialization procedures for
 * the Farrago catalog.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoCatalogInit
    extends FarragoAbstractCatalogInit
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * Reserved name for the system boot catalog.
     */
    public static final String SYSBOOT_CATALOG_NAME = "SYS_BOOT";

    /**
     * Reserved name for the local catalog.
     */
    public static final String LOCALDB_CATALOG_NAME = "LOCALDB";

    /**
     * Reserved name for the public role.
     */
    public static final String PUBLIC_ROLE_NAME = "PUBLIC";

    /**
     * Reserved name for the system admin authorization user. Note that this is
     * intentionally lower-case to match the SQL Server convention.
     */
    public static final String SA_USER_NAME = "sa";

    /**
     * Default jdbc connection timeout in milliseconds
     */
    public static final long DEFAULT_CONNECTION_TIMEOUT_MILLIS = 86400000;

    /**
     * Default freshmen page queue percentage
     */
    public static final int DEFAULT_FRESHMEN_PAGE_QUEUE_PERCENTAGE = 25;

    /**
     * Default page history queue percentage
     */
    public static final int DEFAULT_PAGE_HISTORY_QUEUE_PERCENTAGE = 100;

    /**
     * Default maximum number of pages to prefetch
     */
    public static final int DEFAULT_PREFETCH_PAGES_MAX = 12;

    /**
     * Default prefetch throttle rate
     */
    public static final int DEFAULT_PREFETCH_THROTTLE_RATE = 10;

    //~ Constructors -----------------------------------------------------------

    protected FarragoCatalogInit(FarragoRepos repos)
    {
        super(repos);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Creates objects owned by the system. This is only done once during
     * database creation.
     *
     * @param repos the repository in which to initialize the catalog
     */
    public static void createSystemObjects(FarragoRepos repos)
    {
        tracer.info("Creating system-owned catalog objects");
        FarragoCatalogInit init = null;
        FarragoReposTxnContext txn = repos.newTxnContext();
        boolean rollback = true;
        try {
            try {
                txn.beginWriteTxn();
                init = new FarragoCatalogInit(repos);
                init.initCatalog();
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

        tracer.info("Creation of system-owned catalog objects committed");
    }

    private void initCatalog()
    {
        createSystemCatalogs();
        createSystemAuth();

        // NOTE jvs 3-Jan-2007:  system types are created by the UDR
        // sys_boot.sys_boot.update_system_objects(), but we do it
        // here redundantly to support Farrago extension projects
        // which rely on FarragoCatalogInit to do all the work.
        updateSystemTypes();
    }

    private void createSystemCatalogs()
    {
        FemLocalCatalog catalog;

        catalog = repos.newFemLocalCatalog();
        catalog.setName(SYSBOOT_CATALOG_NAME);
        FarragoCatalogUtil.initializeCatalog(repos, catalog);

        catalog = repos.newFemLocalCatalog();
        catalog.setName(LOCALDB_CATALOG_NAME);
        FarragoCatalogUtil.initializeCatalog(repos, catalog);
    }

    private void createSystemAuth()
    {
        // Create the System Internal User
        FemUser systemUser = repos.newFemUser();
        systemUser.setName(SYSTEM_USER_NAME);

        // Create the System admin user, this is the only system created
        // authenticatable user (via login)
        FemUser saUser = repos.newFemUser();
        saUser.setName(SA_USER_NAME);

        // Create the built-in role PUBLIC
        FemRole publicRole = repos.newFemRole();
        publicRole.setName(PUBLIC_ROLE_NAME);
    }
}

// End FarragoCatalogInit.java
