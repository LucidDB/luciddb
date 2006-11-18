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

import javax.jmi.reflect.*;

import net.sf.farrago.*;
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.datatypes.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.config.*;
import net.sf.farrago.fem.security.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.trace.*;
import net.sf.farrago.util.*;
import net.sf.farrago.resource.*;

import org.eigenbase.jmi.*;
import org.eigenbase.sql.type.*;

import org.netbeans.api.mdr.events.*;


/**
 * FarragoCatalogInit contains one-time persistent initialization procedures for
 * the Farrago catalog.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoCatalogInit
    implements MDRPreChangeListener
{

    //~ Static fields/initializers ---------------------------------------------

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
    public static final String PUBLIC_ROLE_NAME = "PUBLIC";

    /**
     * Reserved name for the system internal authorization user.
     */
    public static final String SYSTEM_USER_NAME = "_SYSTEM";

    /**
     * Reserved name for the system admin authorization user. Note that this is
     * intentionally lower-case to match the SQL Server convention.
     */
    public static final String SA_USER_NAME = "sa";

    //~ Instance fields --------------------------------------------------------

    private final FarragoRepos repos;

    private final Set objs;

    private final String timestamp;

    //~ Constructors -----------------------------------------------------------

    private FarragoCatalogInit(FarragoRepos repos)
    {
        this.repos = repos;
        objs = new HashSet();

        // listen to MDR events during initialization so that we can
        // consistently fill in generic information on all objects
        repos.getMdrRepos().addListener(
            this,
            AttributeEvent.EVENTMASK_ATTRIBUTE);

        timestamp = FarragoCatalogUtil.createTimestamp();
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
        boolean rollback = false;
        FarragoCatalogInit init = null;
        try {
            repos.beginReposTxn(true);
            rollback = true;
            init = new FarragoCatalogInit(repos);
            init.initCatalog();
            rollback = false;
            repos.endReposTxn(false);
        } finally {
            if (init != null) {
                init.publishObjects(rollback);
            }
            if (rollback) {
                repos.endReposTxn(true);
            }
        }
        tracer.info("Creation of system-owned catalog objects committed");
    }

    // implement MDRChangeListener
    public void change(MDRChangeEvent event)
    {
        // don't care
    }

    // implement MDRPreChangeListener
    public void changeCancelled(MDRChangeEvent event)
    {
        // don't care
    }

    // implement MDRPreChangeListener
    public void plannedChange(MDRChangeEvent event)
    {
        if (event instanceof AttributeEvent) {
            objs.add(event.getSource());
        }
    }

    private void publishObjects(boolean rollback)
    {
        repos.getMdrRepos().removeListener(this);
        if (rollback) {
            return;
        }
        Iterator iter = objs.iterator();
        while (iter.hasNext()) {
            Object obj = iter.next();
            if (obj instanceof CwmModelElement) {
                CwmModelElement modelElement = (CwmModelElement) obj;

                // Set visibility so that DdlValidator doesn't try
                // to revalidate this object.
                modelElement.setVisibility(VisibilityKindEnum.VK_PUBLIC);

                // Define this as a system-owned object.
                FemGrant grant =
                    FarragoCatalogUtil.newCreationGrant(
                        repos,
                        SYSTEM_USER_NAME,
                        SYSTEM_USER_NAME,
                        modelElement);
                JmiObjUtil.setMandatoryPrimitiveDefaults(grant);
            }
            if (obj instanceof FemAnnotatedElement) {
                FemAnnotatedElement annotatedElement =
                    (FemAnnotatedElement) obj;
                if (annotatedElement.getDescription() == null) {
                    annotatedElement.setDescription(
                        FarragoResource.instance().
                        CatalogBootstrapObjDescription.str());
                }
                FarragoCatalogUtil.updateAnnotatedElement(
                    annotatedElement,
                    timestamp,
                    true);
            }
            JmiObjUtil.setMandatoryPrimitiveDefaults((RefObject) obj);
        }
    }

    private void initCatalog()
    {
        createSystemCatalogs();
        createSystemTypes();
        createSystemAuth();
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

    private void defineTypeAlias(
        String aliasName,
        CwmSqldataType type)
    {
        CwmTypeAlias typeAlias = repos.newCwmTypeAlias();
        typeAlias.setName(aliasName);
        typeAlias.setType(type);
    }

    private void createSystemTypes()
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
        simpleType.setTypeNumber(Types.BOOLEAN);

        simpleType = repos.newCwmSqlsimpleType();
        simpleType.setName("TINYINT");
        simpleType.setTypeNumber(Types.TINYINT);
        simpleType.setNumericPrecision(8);
        simpleType.setNumericPrecisionRadix(2);
        simpleType.setNumericScale(0);

        simpleType = repos.newCwmSqlsimpleType();
        simpleType.setName("SMALLINT");
        simpleType.setTypeNumber(Types.SMALLINT);
        simpleType.setNumericPrecision(16);
        simpleType.setNumericPrecisionRadix(2);
        simpleType.setNumericScale(0);

        simpleType = repos.newCwmSqlsimpleType();
        simpleType.setName("INTEGER");
        simpleType.setTypeNumber(Types.INTEGER);
        simpleType.setNumericPrecision(32);
        simpleType.setNumericPrecisionRadix(2);
        simpleType.setNumericScale(0);
        defineTypeAlias("INT", simpleType);

        simpleType = repos.newCwmSqlsimpleType();
        simpleType.setName("BIGINT");
        simpleType.setTypeNumber(Types.BIGINT);
        simpleType.setNumericPrecision(64);
        simpleType.setNumericPrecisionRadix(2);
        simpleType.setNumericScale(0);

        simpleType = repos.newCwmSqlsimpleType();
        simpleType.setName("REAL");
        simpleType.setTypeNumber(Types.REAL);
        simpleType.setNumericPrecision(23);
        simpleType.setNumericPrecisionRadix(2);

        simpleType = repos.newCwmSqlsimpleType();
        simpleType.setName("DOUBLE");
        simpleType.setTypeNumber(Types.DOUBLE);
        simpleType.setNumericPrecision(52);
        simpleType.setNumericPrecisionRadix(2);
        defineTypeAlias("DOUBLE PRECISION", simpleType);
        defineTypeAlias("FLOAT", simpleType);

        simpleType = repos.newCwmSqlsimpleType();
        simpleType.setName("VARCHAR");
        simpleType.setTypeNumber(Types.VARCHAR);

        // NOTE: this is an upper bound based on usage of 2-byte length
        // indicators in stored tuples; there are further limits based on page
        // size (imposed during table creation)
        simpleType.setCharacterMaximumLength(65535);
        defineTypeAlias("CHARACTER VARYING", simpleType);

        simpleType = repos.newCwmSqlsimpleType();
        simpleType.setName("VARBINARY");
        simpleType.setTypeNumber(Types.VARBINARY);
        simpleType.setCharacterMaximumLength(65535);

        simpleType = repos.newCwmSqlsimpleType();
        simpleType.setName("CHAR");
        simpleType.setTypeNumber(Types.CHAR);
        simpleType.setCharacterMaximumLength(65535);
        defineTypeAlias("CHARACTER", simpleType);

        simpleType = repos.newCwmSqlsimpleType();
        simpleType.setName("BINARY");
        simpleType.setTypeNumber(Types.BINARY);
        simpleType.setCharacterMaximumLength(65535);

        simpleType = repos.newCwmSqlsimpleType();
        simpleType.setName("DATE");
        simpleType.setTypeNumber(Types.DATE);
        simpleType.setDateTimePrecision(0);

        // TODO jvs 26-July-2004: Support fractional precision for TIME and
        // TIMESTAMP.  Currently, most of the support is there for up to
        // milliseconds, but JDBC getString conversion is missing (see comments
        // in SqlDateTimeWithoutTZ).  SQL99 says default precision for TIMESTAMP
        // is microseconds, so some more work is required to support that.
        // Default precision for TIME is seconds, which is already the case.
        simpleType = repos.newCwmSqlsimpleType();
        simpleType.setName("TIME");
        simpleType.setTypeNumber(Types.TIME);
        simpleType.setDateTimePrecision(0);

        simpleType = repos.newCwmSqlsimpleType();
        simpleType.setName("TIMESTAMP");
        simpleType.setTypeNumber(Types.TIMESTAMP);
        simpleType.setDateTimePrecision(0);

        simpleType = repos.newCwmSqlsimpleType();
        simpleType.setName("DECIMAL");
        simpleType.setTypeNumber(Types.DECIMAL);
        simpleType.setNumericPrecision(SqlTypeName.MAX_NUMERIC_PRECISION);
        simpleType.setNumericPrecisionRadix(10);
        simpleType.setNumericScale(SqlTypeName.MAX_NUMERIC_SCALE);
        defineTypeAlias("DEC", simpleType);

        // placeholder for CURSOR parameters to UDX's; by adding
        // an "eponymous alias", we'll make it visible from type lookup;
        // we don't use a simple type here because we don't want it to
        // show up as part of standard type info
        CwmSqlstructuredType cursorType = repos.newCwmSqlstructuredType();
        cursorType.setName("CURSOR");
        cursorType.setTypeNumber(SqlTypeName.Cursor.getJdbcOrdinal());
        defineTypeAlias("CURSOR", cursorType);

        // REVIEW jvs 11-Aug-2005:  This isn't a real type descriptor, since
        // collection types are constructed anonymously rather than
        // defined as named instances.  Do we need it?
        FemSqlcollectionType collectType;
        collectType = repos.newFemSqlmultisetType();
        collectType.setName("MULTISET");

        // a multiset has the same type# as an array for now
        collectType.setTypeNumber(Types.ARRAY);
    }
}

// End FarragoCatalogInit.java
