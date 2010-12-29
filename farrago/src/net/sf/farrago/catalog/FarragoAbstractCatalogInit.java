/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
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

import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.datatypes.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.config.*;
import net.sf.farrago.fem.security.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.trace.*;

import org.eigenbase.jmi.*;
import org.eigenbase.sql.type.*;

import org.netbeans.api.mdr.events.*;


/**
 * FarragoAbstractCatalogInit provides an abstract base class for classes that
 * initialize the Farrago catalog.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public abstract class FarragoAbstractCatalogInit
    implements MDRPreChangeListener
{
    //~ Static fields/initializers ---------------------------------------------

    //  ~ Static fields/initializers -------------------------------------------

    protected static final Logger tracer = FarragoTrace.getReposTracer();

    /**
     * Reserved name for the system internal authorization user.
     */
    public static final String SYSTEM_USER_NAME = "_SYSTEM";

    //~ Instance fields --------------------------------------------------------

    protected final FarragoRepos repos;

    private final Set<Object> objs;

    private final String timestamp;

    //~ Constructors -----------------------------------------------------------

    protected FarragoAbstractCatalogInit(FarragoRepos repos)
    {
        this.repos = repos;
        objs = new HashSet<Object>();

        // listen to MDR events during initialization so that we can
        // consistently fill in generic information on all objects
        repos.getMdrRepos().addListener(
            this,
            AttributeEvent.EVENTMASK_ATTRIBUTE);

        timestamp = FarragoCatalogUtil.createTimestamp();
    }

    //~ Methods ----------------------------------------------------------------

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

    public void publishObjects(boolean rollback)
    {
        repos.getMdrRepos().removeListener(this);
        if (rollback) {
            return;
        }
        for (Object obj : objs) {
            if (obj instanceof CwmModelElement) {
                CwmModelElement modelElement = (CwmModelElement) obj;

                // Set visibility so that DdlValidator doesn't try
                // to revalidate this object.
                modelElement.setVisibility(VisibilityKindEnum.VK_PUBLIC);

                if (!FarragoCatalogUtil.needsCreationGrant(modelElement)) {
                    continue;
                }
                
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
                        FarragoResource.instance()
                        .CatalogBootstrapObjDescription.str());
                }
                FarragoCatalogUtil.updateAnnotatedElement(
                    annotatedElement,
                    timestamp,
                    true);
            }
            JmiObjUtil.setMandatoryPrimitiveDefaults((RefObject) obj);
        }
    }

    protected void defineTypeAlias(String aliasName, CwmSqldataType type)
    {
        CwmTypeAlias typeAlias = repos.newCwmTypeAlias();
        typeAlias.setName(aliasName);
        typeAlias.setType(type);
    }

    protected void updateSystemParameters()
    {
        // If migrated from a catalog version where these parameters
        // don't exist, they will be set to null; so set them to default
        // values.
        //
        // NOTE zfong 5/22/08 - Make sure to also update
        // {@link FarragoTestCase#saveParameters(FarragoRepos) to avoid
        // resetting these parameters to their original null values.
        FemFarragoConfig config = repos.getCurrentConfig();
        if (config.getConnectionTimeoutMillis() == null) {
            config.setConnectionTimeoutMillis(
                new Long(FarragoCatalogInit.DEFAULT_CONNECTION_TIMEOUT_MILLIS));
        }

        if (repos.isFennelEnabled()) {
            FemFennelConfig fennelConfig = config.getFennelConfig();
            if (fennelConfig.getFreshmenPageQueuePercentage() == null) {
                fennelConfig.setFreshmenPageQueuePercentage(
                    new Integer(
                        FarragoCatalogInit
                        .DEFAULT_FRESHMEN_PAGE_QUEUE_PERCENTAGE));
            }
            if (fennelConfig.getPageHistoryQueuePercentage() == null) {
                fennelConfig.setPageHistoryQueuePercentage(
                    new Integer(
                        FarragoCatalogInit.DEFAULT_PAGE_HISTORY_QUEUE_PERCENTAGE));
            }
            if (fennelConfig.getPrefetchPagesMax() == null) {
                fennelConfig.setPrefetchPagesMax(
                    new Integer(FarragoCatalogInit.DEFAULT_PREFETCH_PAGES_MAX));
            }
            if (fennelConfig.getPrefetchThrottleRate() == null) {
                fennelConfig.setPrefetchThrottleRate(
                    new Integer(
                        FarragoCatalogInit.DEFAULT_PREFETCH_THROTTLE_RATE));
            }
            if (fennelConfig.getDeviceSchedulerType() == null) {
                fennelConfig.setDeviceSchedulerType(
                    DeviceSchedulerTypeEnum.DEFAULT);
            }
            if (fennelConfig.getProcessorCacheBytes() == null) {
                fennelConfig.setProcessorCacheBytes(-1);
            }
        }
    }

    protected void updateSystemTypes()
    {
        Collection<CwmSqlsimpleType> types =
            repos.allOfClass(CwmSqlsimpleType.class);
        CwmSqlsimpleType simpleType;

        simpleType = FarragoCatalogUtil.getModelElementByName(types, "BOOLEAN");
        if (simpleType == null) {
            simpleType = repos.newCwmSqlsimpleType();
            simpleType.setName("BOOLEAN");
            simpleType.setTypeNumber(Types.BOOLEAN);
        }

        simpleType = FarragoCatalogUtil.getModelElementByName(types, "TINYINT");
        if (simpleType == null) {
            simpleType = repos.newCwmSqlsimpleType();
            simpleType.setName("TINYINT");
            simpleType.setTypeNumber(Types.TINYINT);
            simpleType.setNumericPrecision(8);
            simpleType.setNumericPrecisionRadix(2);
            simpleType.setNumericScale(0);
        }

        simpleType =
            FarragoCatalogUtil.getModelElementByName(types, "SMALLINT");
        if (simpleType == null) {
            simpleType = repos.newCwmSqlsimpleType();
            simpleType.setName("SMALLINT");
            simpleType.setTypeNumber(Types.SMALLINT);
            simpleType.setNumericPrecision(16);
            simpleType.setNumericPrecisionRadix(2);
            simpleType.setNumericScale(0);
        }

        simpleType = FarragoCatalogUtil.getModelElementByName(types, "INTEGER");
        if (simpleType == null) {
            simpleType = repos.newCwmSqlsimpleType();
            simpleType.setName("INTEGER");
            simpleType.setTypeNumber(Types.INTEGER);
            simpleType.setNumericPrecision(32);
            simpleType.setNumericPrecisionRadix(2);
            simpleType.setNumericScale(0);
            defineTypeAlias("INT", simpleType);
        }

        simpleType = FarragoCatalogUtil.getModelElementByName(types, "BIGINT");
        if (simpleType == null) {
            simpleType = repos.newCwmSqlsimpleType();
            simpleType.setName("BIGINT");
            simpleType.setTypeNumber(Types.BIGINT);
            simpleType.setNumericPrecision(64);
            simpleType.setNumericPrecisionRadix(2);
            simpleType.setNumericScale(0);
        }

        simpleType = FarragoCatalogUtil.getModelElementByName(types, "REAL");
        if (simpleType == null) {
            simpleType = repos.newCwmSqlsimpleType();
            simpleType.setName("REAL");
            simpleType.setTypeNumber(Types.REAL);
            simpleType.setNumericPrecision(23);
            simpleType.setNumericPrecisionRadix(2);
        }

        simpleType = FarragoCatalogUtil.getModelElementByName(types, "DOUBLE");
        if (simpleType == null) {
            simpleType = repos.newCwmSqlsimpleType();
            simpleType.setName("DOUBLE");
            simpleType.setTypeNumber(Types.DOUBLE);
            simpleType.setNumericPrecision(52);
            simpleType.setNumericPrecisionRadix(2);
            defineTypeAlias("DOUBLE PRECISION", simpleType);
            defineTypeAlias("FLOAT", simpleType);
        }

        simpleType = FarragoCatalogUtil.getModelElementByName(types, "VARCHAR");
        if (simpleType == null) {
            simpleType = repos.newCwmSqlsimpleType();
            simpleType.setName("VARCHAR");
            simpleType.setTypeNumber(Types.VARCHAR);

            // NOTE: this is an upper bound based on usage of 2-byte length
            // indicators in stored tuples; there are further limits based on
            // page size (imposed during table creation)
            simpleType.setCharacterMaximumLength(65535);
            defineTypeAlias("CHARACTER VARYING", simpleType);
        }

        simpleType =
            FarragoCatalogUtil.getModelElementByName(types, "VARBINARY");
        if (simpleType == null) {
            simpleType = repos.newCwmSqlsimpleType();
            simpleType.setName("VARBINARY");
            simpleType.setTypeNumber(Types.VARBINARY);
            simpleType.setCharacterMaximumLength(65535);
        }

        simpleType = FarragoCatalogUtil.getModelElementByName(types, "CHAR");
        if (simpleType == null) {
            simpleType = repos.newCwmSqlsimpleType();
            simpleType.setName("CHAR");
            simpleType.setTypeNumber(Types.CHAR);
            simpleType.setCharacterMaximumLength(65535);
            defineTypeAlias("CHARACTER", simpleType);
        }

        simpleType = FarragoCatalogUtil.getModelElementByName(types, "BINARY");
        if (simpleType == null) {
            simpleType = repos.newCwmSqlsimpleType();
            simpleType.setName("BINARY");
            simpleType.setTypeNumber(Types.BINARY);
            simpleType.setCharacterMaximumLength(65535);
        }

        simpleType = FarragoCatalogUtil.getModelElementByName(types, "DATE");
        if (simpleType == null) {
            simpleType = repos.newCwmSqlsimpleType();
            simpleType.setName("DATE");
            simpleType.setTypeNumber(Types.DATE);
            simpleType.setDateTimePrecision(0);
        }

        simpleType = FarragoCatalogUtil.getModelElementByName(types, "TIME");
        if (simpleType == null) {
            // TODO jvs 26-July-2004: Support fractional precision for TIME and
            // TIMESTAMP.  Currently, most of the support is there for up to
            // milliseconds, but JDBC getString conversion is missing (see
            // comments in SqlDateTimeWithoutTZ).  SQL99 says default precision
            // for TIMESTAMP is microseconds, so some more work is required to
            // support that.  Default precision for TIME is seconds, which is
            // already the case.
            simpleType = repos.newCwmSqlsimpleType();
            simpleType.setName("TIME");
            simpleType.setTypeNumber(Types.TIME);
            simpleType.setDateTimePrecision(0);
        }

        simpleType =
            FarragoCatalogUtil.getModelElementByName(types, "TIMESTAMP");
        if (simpleType == null) {
            simpleType = repos.newCwmSqlsimpleType();
            simpleType.setName("TIMESTAMP");
            simpleType.setTypeNumber(Types.TIMESTAMP);
            simpleType.setDateTimePrecision(0);
        }

        simpleType = FarragoCatalogUtil.getModelElementByName(types, "DECIMAL");
        if (simpleType == null) {
            simpleType = repos.newCwmSqlsimpleType();
            simpleType.setName("DECIMAL");
            simpleType.setTypeNumber(Types.DECIMAL);
            simpleType.setNumericPrecision(SqlTypeName.MAX_NUMERIC_PRECISION);
            simpleType.setNumericPrecisionRadix(10);
            simpleType.setNumericScale(SqlTypeName.MAX_NUMERIC_SCALE);
            defineTypeAlias("DEC", simpleType);
        }

        Collection<CwmSqlstructuredType> structTypes =
            repos.allOfClass(CwmSqlstructuredType.class);
        CwmSqlstructuredType structType;

        structType =
            FarragoCatalogUtil.getModelElementByName(structTypes, "CURSOR");
        if (structType == null) {
            // placeholder for CURSOR parameters to UDX's; by adding
            // an "eponymous alias", we'll make it visible from type lookup;
            // we don't use a simple type here because we don't want it to
            // show up as part of standard type info
            CwmSqlstructuredType cursorType = repos.newCwmSqlstructuredType();
            cursorType.setName("CURSOR");
            cursorType.setTypeNumber(SqlTypeName.CURSOR.getJdbcOrdinal());
            defineTypeAlias("CURSOR", cursorType);
        }

        structType =
            FarragoCatalogUtil.getModelElementByName(
                structTypes,
                "COLUMN_LIST");
        if (structType == null) {
            // NOTE jvs 27-Dec-2006: like CURSOR, need an eponymous alias for
            // COLUMN_LIST; this is annoying, because it's entirely an internal
            // type name (users are never supposed to reference it
            // explicitly--they use SELECT FROM syntax instead); but without
            // it, type resolution fails internally; there's probably
            // some way to fix this
            CwmSqlstructuredType columnListType =
                repos.newCwmSqlstructuredType();
            columnListType.setName("COLUMN_LIST");
            columnListType.setTypeNumber(
                SqlTypeName.COLUMN_LIST.getJdbcOrdinal());
            defineTypeAlias("COLUMN_LIST", columnListType);
        }

        Collection<FemSqlmultisetType> multisetTypes =
            repos.allOfClass(FemSqlmultisetType.class);
        FemSqlmultisetType multisetType;

        multisetType =
            FarragoCatalogUtil.getModelElementByName(multisetTypes, "MULTISET");
        if (multisetType == null) {
            // REVIEW jvs 11-Aug-2005:  This isn't a real type descriptor, since
            // collection types are constructed anonymously rather than defined
            // as named instances.  Do we need it?
            FemSqlcollectionType collectType;
            collectType = repos.newFemSqlmultisetType();
            collectType.setName("MULTISET");

            // a multiset has the same type# as an array for now
            collectType.setTypeNumber(Types.ARRAY);
        }
    }
}

// End FarragoAbstractCatalogInit.java
