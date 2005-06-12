/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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

import java.sql.*;
import java.util.*;
import java.util.logging.*;

import net.sf.farrago.*;
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.datatypes.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.fem.security.*;
import net.sf.farrago.fem.config.*;
import net.sf.farrago.trace.*;
import net.sf.farrago.util.*;

/**
 * FarragoCatalogInit contains one-time persistent initialization procedures
 * for the Farrago catalog.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class FarragoCatalogInit
{
    //~ Static fields/initializers --------------------------------------------

    private static final Logger tracer = FarragoTrace.getReposTracer();
    
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
    public static final String SECURITY_PUBLIC_ROLE_NAME = "PUBLIC";

    /**
     * Reserved name for the system internal authorization user.
     */
    public static final String SECURITY_SYSUSER_NAME = "_SYSTEM";
    
    /**
     * Creates objects owned by the system.  This is only done once during
     * database creation.
     *
     * @param repos the repository in which to initialize the catalog
     */
    public static void createSystemObjects(FarragoRepos repos)
    {
        tracer.info("Creating system-owned catalog objects");
        boolean rollback = true;
        try {
            repos.beginReposTxn(true);
            initCatalog(repos);
            rollback = false;
        } finally {
            repos.endReposTxn(rollback);
        }
        tracer.info("Creation of system-owned catalog objects committed");
    }
    
    private static void initCatalog(FarragoRepos repos)
    {
        createSystemCatalogs(repos);
        createSystemTypes(repos);
        createSystemAuth(repos);
    }

    private static void createSystemCatalogs(FarragoRepos repos)
    {
        FemLocalCatalog catalog;

        catalog = repos.newFemLocalCatalog();
        catalog.setName(SYSBOOT_CATALOG_NAME);
        FarragoCatalogUtil.initializeCatalog(repos, catalog);

        catalog = repos.newFemLocalCatalog();
        catalog.setName(LOCALDB_CATALOG_NAME);
        FarragoCatalogUtil.initializeCatalog(repos, catalog);
    }

    private static void createSystemAuth(FarragoRepos repos)
    {
        // Create the System Internal User 
        FemUser systemUser = repos.newFemUser();
        systemUser.setName(SECURITY_SYSUSER_NAME);
        
        // Create the built-in role PUBLIC
        FemRole publicRole = repos.newFemRole();
        publicRole.setName(SECURITY_PUBLIC_ROLE_NAME);

        // Create a creation grant for sys user and public role
        Timestamp ts = new Timestamp(System.currentTimeMillis());
        
        FemCreationGrant grantUser = repos.newFemCreationGrant();
        grantUser.setAction(PrivilegedActionEnum.CREATION.toString());
        grantUser.setWithGrantOption(false);
        grantUser.setCreationDate(ts.toString());
        grantUser.setModificationDate(grantUser.getCreationDate());
        // TODO: can we set to something?
        grantUser.setGrantor(systemUser);
        grantUser.setGrantee(systemUser);
        grantUser.setElement(systemUser);
        
        FemCreationGrant grantPublicRole = repos.newFemCreationGrant();
        grantPublicRole.setAction(PrivilegedActionEnum.CREATION.toString());
        grantPublicRole.setWithGrantOption(false);
        grantPublicRole.setCreationDate(ts.toString());
        grantPublicRole.setModificationDate(grantPublicRole.getCreationDate());
        // TODO: can we set to something?
        grantPublicRole.setGrantor(systemUser);
        grantPublicRole.setGrantee(systemUser);
        grantPublicRole.setElement(publicRole);
    }

    private static void defineTypeAlias(
        FarragoRepos repos,
        String aliasName,
        CwmSqlsimpleType type)
    {
        CwmTypeAlias typeAlias = repos.newCwmTypeAlias();
        typeAlias.setName(aliasName);
        typeAlias.setType(type);
    }
    
    private static void createSystemTypes(FarragoRepos repos)
    {
        CwmSqlsimpleType simpleType;

        // This is where all the builtin types are defined.  To add a new
        // builtin type, you have to:
        // (1) add a definition here
        // (2) add mappings in FarragoTypeFactoryImpl and maybe
        // SqlTypeName/SqlTypeFamily
        // (3) add Fennel mappings in
        // FennelRelUtil.convertSqlTypeNumberToFennelTypeOrdinal
        // (4) since I've already done all the easy cases, you'll probably
        // need lots of extra fancy semantics elsewhere
        simpleType = repos.newCwmSqlsimpleType();
        simpleType.setName("BOOLEAN");
        simpleType.setTypeNumber(new Integer(Types.BOOLEAN));

        simpleType = repos.newCwmSqlsimpleType();
        simpleType.setName("TINYINT");
        simpleType.setTypeNumber(new Integer(Types.TINYINT));
        simpleType.setNumericPrecision(new Integer(8));
        simpleType.setNumericPrecisionRadix(new Integer(2));
        simpleType.setNumericScale(new Integer(0));

        simpleType = repos.newCwmSqlsimpleType();
        simpleType.setName("SMALLINT");
        simpleType.setTypeNumber(new Integer(Types.SMALLINT));
        simpleType.setNumericPrecision(new Integer(16));
        simpleType.setNumericPrecisionRadix(new Integer(2));
        simpleType.setNumericScale(new Integer(0));

        simpleType = repos.newCwmSqlsimpleType();
        simpleType.setName("INTEGER");
        simpleType.setTypeNumber(new Integer(Types.INTEGER));
        simpleType.setNumericPrecision(new Integer(32));
        simpleType.setNumericPrecisionRadix(new Integer(2));
        simpleType.setNumericScale(new Integer(0));
        defineTypeAlias(repos, "INT", simpleType);

        simpleType = repos.newCwmSqlsimpleType();
        simpleType.setName("BIGINT");
        simpleType.setTypeNumber(new Integer(Types.BIGINT));
        simpleType.setNumericPrecision(new Integer(64));
        simpleType.setNumericPrecisionRadix(new Integer(2));
        simpleType.setNumericScale(new Integer(0));

        simpleType = repos.newCwmSqlsimpleType();
        simpleType.setName("REAL");
        simpleType.setTypeNumber(new Integer(Types.REAL));
        simpleType.setNumericPrecision(new Integer(23));
        simpleType.setNumericPrecisionRadix(new Integer(2));

        simpleType = repos.newCwmSqlsimpleType();
        simpleType.setName("DOUBLE");
        simpleType.setTypeNumber(new Integer(Types.DOUBLE));
        simpleType.setNumericPrecision(new Integer(52));
        simpleType.setNumericPrecisionRadix(new Integer(2));
        defineTypeAlias(repos, "DOUBLE PRECISION", simpleType);
        defineTypeAlias(repos, "FLOAT", simpleType);

        simpleType = repos.newCwmSqlsimpleType();
        simpleType.setName("VARCHAR");
        simpleType.setTypeNumber(new Integer(Types.VARCHAR));

        // NOTE: this is an upper bound based on usage of 2-byte length
        // indicators in stored tuples; there are further limits based on page
        // size (imposed during table creation)
        simpleType.setCharacterMaximumLength(new Integer(65535));
        defineTypeAlias(repos, "CHARACTER VARYING", simpleType);

        simpleType = repos.newCwmSqlsimpleType();
        simpleType.setName("VARBINARY");
        simpleType.setTypeNumber(new Integer(Types.VARBINARY));
        simpleType.setCharacterMaximumLength(new Integer(65535));

        simpleType = repos.newCwmSqlsimpleType();
        simpleType.setName("CHAR");
        simpleType.setTypeNumber(new Integer(Types.CHAR));
        simpleType.setCharacterMaximumLength(new Integer(65535));
        defineTypeAlias(repos, "CHARACTER", simpleType);

        simpleType = repos.newCwmSqlsimpleType();
        simpleType.setName("BINARY");
        simpleType.setTypeNumber(new Integer(Types.BINARY));
        simpleType.setCharacterMaximumLength(new Integer(65535));

        simpleType = repos.newCwmSqlsimpleType();
        simpleType.setName("DATE");
        simpleType.setTypeNumber(new Integer(Types.DATE));
        simpleType.setDateTimePrecision(new Integer(0));

        // TODO jvs 26-July-2004: Support fractional precision for TIME and
        // TIMESTAMP.  Currently, most of the support is there for up to
        // milliseconds, but JDBC getString conversion is missing (see comments
        // in SqlDateTimeWithoutTZ).  SQL99 says default precision for
        // TIMESTAMP is microseconds, so some more work is required to
        // support that.  Default precision for TIME is seconds,
        // which is already the case.
        simpleType = repos.newCwmSqlsimpleType();
        simpleType.setName("TIME");
        simpleType.setTypeNumber(new Integer(Types.TIME));
        simpleType.setDateTimePrecision(new Integer(0));

        simpleType = repos.newCwmSqlsimpleType();
        simpleType.setName("TIMESTAMP");
        simpleType.setTypeNumber(new Integer(Types.TIMESTAMP));
        simpleType.setDateTimePrecision(new Integer(0));

        simpleType = repos.newCwmSqlsimpleType();
        simpleType.setName("DECIMAL");
        simpleType.setTypeNumber(new Integer(Types.DECIMAL));
        simpleType.setNumericPrecision(new Integer(39));
        simpleType.setNumericPrecisionRadix(new Integer(10));
        defineTypeAlias(repos, "DEC", simpleType);

        FemSqlcollectionType collectType;
        collectType = repos.newFemSqlmultisetType();
        collectType.setName("MULTISET");
        // a multiset has the same type# as an array for now
        collectType.setTypeNumber(new Integer(Types.ARRAY));
    }
}

// End FarragoCatalogInit.java
