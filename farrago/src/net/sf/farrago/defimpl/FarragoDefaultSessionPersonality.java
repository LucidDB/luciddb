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
package net.sf.farrago.defimpl;

import com.disruptivetech.farrago.fennel.*;
import com.lucidera.farrago.fennel.*;
import com.lucidera.lurql.*;

import org.eigenbase.util.*;
import org.eigenbase.jmi.*;
import org.eigenbase.oj.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.sql.fun.*;
import org.eigenbase.resgen.*;

import net.sf.farrago.session.*;
import net.sf.farrago.db.*;
import net.sf.farrago.query.*;
import net.sf.farrago.ddl.*;
import net.sf.farrago.parser.*;
import net.sf.farrago.runtime.*;
import net.sf.farrago.catalog.*;

import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.datatypes.*;
import net.sf.farrago.cwm.keysindexes.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.cwm.relational.enumerations.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.fem.security.*;

import java.util.*;

import javax.jmi.reflect.*;

/**
 * FarragoDefaultSessionPersonality is a default implementation of the
 * {@link FarragoSessionPersonality} interface.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoDefaultSessionPersonality
    implements FarragoSessionPersonality
{
    private final FarragoDatabase database;
    
    protected FarragoDefaultSessionPersonality(FarragoDbSession session)
    {
        database = session.getDatabase();
    }

    // implement FarragoSessionPersonality
    public FarragoSessionPlanner newPlanner(
        FarragoSessionPreparingStmt stmt,
        boolean init)
    {
        FarragoDefaultPlanner planner = new FarragoDefaultPlanner(stmt);
        if (init) {
            planner.init();
        }
        return planner;
    }

    // implement FarragoSessionPersonality
    public void definePlannerListeners(FarragoSessionPlanner planner)
    {
    }

    // implement FarragoStreamFactoryProvider
    public void registerStreamFactories(long hStreamGraph)
    {
        DisruptiveTechJni.registerStreamFactory(hStreamGraph);
        LucidEraJni.registerStreamFactory(hStreamGraph);
    }
    
    // implement FarragoSessionPersonality
    public String getDefaultLocalDataServerName(
        FarragoSessionStmtValidator stmtValidator)
    {
        if (stmtValidator.getSession().getRepos().isFennelEnabled()) {
            return "SYS_FTRS_DATA_SERVER";
        } else {
            return "SYS_MOCK_DATA_SERVER";
        }
    }
    
    // implement FarragoSessionPersonality
    public SqlOperatorTable getSqlOperatorTable(
        FarragoSessionPreparingStmt preparingStmt)
    {
        return SqlStdOperatorTable.instance();
    }

    // implement FarragoSessionPersonality
    public OJRexImplementorTable getOJRexImplementorTable(
        FarragoSessionPreparingStmt preparingStmt)
    {
        return database.getOJRexImplementorTable();
    }
    
    // implement FarragoSessionPersonality
    public FarragoSessionParser newParser(FarragoSession session)
    {
        return new FarragoParser();
    }
    
    // implement FarragoSessionPersonality
    public FarragoSessionPreparingStmt newPreparingStmt(
        FarragoSessionStmtValidator stmtValidator)
    {
        return new FarragoPreparingStmt(stmtValidator);
    }
    
    // implement FarragoSessionPersonality
    public FarragoSessionDdlValidator newDdlValidator(
        FarragoSessionStmtValidator stmtValidator)
    {
        return new DdlValidator(stmtValidator);
    }
    
    // implement FarragoSessionPersonality
    public void defineDdlHandlers(
        FarragoSessionDdlValidator ddlValidator,
        List handlerList)
    {
        // NOTE jvs 21-Jan-2005:  handlerList order matters here.
        // DdlRelationalHandler includes some catch-all methods for
        // superinterfaces which we only want to invoke when one of
        // the more specific handlers doesn't satisfied the request.
        DdlMedHandler medHandler = new DdlMedHandler(ddlValidator);
        DdlSecurityHandler securityHandler =
            new DdlSecurityHandler(ddlValidator);
        handlerList.add(medHandler);
        handlerList.add(new DdlRoutineHandler(ddlValidator));
        handlerList.add(new DdlRelationalHandler(medHandler));
        handlerList.add(securityHandler);

        // Define drop rules
        FarragoRepos repos =
            ddlValidator.getStmtValidator().getSession().getRepos();

        // When a table is dropped, all indexes on the table should also be
        // implicitly dropped.
        ddlValidator.defineDropRule(
            repos.getKeysIndexesPackage().getIndexSpansClass(),
            new FarragoSessionDdlDropRule("spannedClass", null,
                ReferentialRuleTypeEnum.IMPORTED_KEY_CASCADE));

        // Dependencies can never be dropped without CASCADE, but with
        // CASCADE, they go away.
        ddlValidator.defineDropRule(
            repos.getCorePackage().getDependencySupplier(),
            new FarragoSessionDdlDropRule("supplier", null,
                ReferentialRuleTypeEnum.IMPORTED_KEY_RESTRICT));

        // When a dependency gets dropped, take its owner (the client)
        // down with it.
        ddlValidator.defineDropRule(
            repos.getCorePackage().getElementOwnership(),
            new FarragoSessionDdlDropRule("ownedElement", CwmDependency.class,
                ReferentialRuleTypeEnum.IMPORTED_KEY_CASCADE));

        // Without CASCADE, a schema can only be dropped when it is empty.
        // This is not true for other namespaces (e.g. a table's constraints
        // are dropped implicitly), so we specify the superInterface filter.
        ddlValidator.defineDropRule(
            repos.getCorePackage().getElementOwnership(),
            new FarragoSessionDdlDropRule("namespace", CwmSchema.class,
                ReferentialRuleTypeEnum.IMPORTED_KEY_RESTRICT));
        
        // When a UDT is dropped, all routines which realize methods should
        // also be implicitly dropped.
        ddlValidator.defineDropRule(
            repos.getBehavioralPackage().getOperationMethod(),
            new FarragoSessionDdlDropRule("specification", null,
                ReferentialRuleTypeEnum.IMPORTED_KEY_CASCADE));

        // Grants should be dropped together with any of the grantor, grantee,
        // or granted element
        ddlValidator.defineDropRule(
            repos.getSecurityPackage().getPrivilegeIsGrantedToGrantee(),
            new FarragoSessionDdlDropRule(
                "Grantee",
                null,
                ReferentialRuleTypeEnum.IMPORTED_KEY_CASCADE));
        ddlValidator.defineDropRule(
            repos.getSecurityPackage().getPrivilegeIsGrantedByGrantor(),
            new FarragoSessionDdlDropRule(
                "Grantor",
                null,
                ReferentialRuleTypeEnum.IMPORTED_KEY_CASCADE));
        ddlValidator.defineDropRule(
            repos.getSecurityPackage().getPrivilegeIsGrantedOnElement(),
            new FarragoSessionDdlDropRule(
                "Element",
                null,
                ReferentialRuleTypeEnum.IMPORTED_KEY_CASCADE));
    }
    
    // implement FarragoSessionPersonality
    public void definePrivileges(
        FarragoSessionPrivilegeMap map)
    {
        FarragoRepos repos = database.getSystemRepos();

        PrivilegedAction [] tableActions = new PrivilegedAction [] 
            {
                PrivilegedActionEnum.SELECT,
                PrivilegedActionEnum.INSERT,
                PrivilegedActionEnum.DELETE,
                PrivilegedActionEnum.UPDATE,
            };
        defineTypePrivileges(
            map,
            repos.getRelationalPackage().getCwmNamedColumnSet(),
            tableActions);
        
        PrivilegedAction [] routineActions = new PrivilegedAction [] 
            {
                PrivilegedActionEnum.EXECUTE
            };
        defineTypePrivileges(
            map,
            repos.getSql2003Package().getFemRoutine(), 
            routineActions);
    }

    private void defineTypePrivileges(
        FarragoSessionPrivilegeMap map,
        RefClass refClass,
        PrivilegedAction [] actions)
    {
        for (int i = 0; i < actions.length; ++i) {
            map.mapPrivilegeForType(
                refClass,
                actions[i].toString(),
                true,
                true);
        }
    }
    
    // implement FarragoSessionPersonality
    public Class getRuntimeContextClass(
        FarragoSessionPreparingStmt preparingStmt)
    {
        return FarragoRuntimeContext.class;
    }

    // implement FarragoSessionPersonality
    public FarragoSessionRuntimeContext newRuntimeContext(
        FarragoSessionRuntimeParams params)
    {
        return new FarragoRuntimeContext(params);
    }
    
    // implement FarragoSessionPersonality
    public void validate(
        FarragoSessionStmtValidator stmtValidator,
        SqlNode sqlNode)
    {
    }
    
    // implement FarragoSessionPersonality
    public JmiQueryProcessor newJmiQueryProcessor(String language)
    {
        if (!language.equals("LURQL")) {
            return null;
        }
        return new LurqlQueryProcessor(
            database.getSystemRepos().getMdrRepos());
    }

    public boolean isSupportedType(SqlTypeName type)
    {
        if (type == null) {
            // Not a SQL type -- may be a structured type, such as MULTISET.
            return true;
        }
        switch (type.getOrdinal()) {
        case SqlTypeName.Boolean_ordinal:
        case SqlTypeName.Tinyint_ordinal:
        case SqlTypeName.Smallint_ordinal:
        case SqlTypeName.Integer_ordinal:
        case SqlTypeName.Date_ordinal:
        case SqlTypeName.Time_ordinal:
        case SqlTypeName.Timestamp_ordinal:
        case SqlTypeName.Bigint_ordinal:
        case SqlTypeName.Varchar_ordinal:
        case SqlTypeName.Varbinary_ordinal:
        case SqlTypeName.Multiset_ordinal:
        case SqlTypeName.Char_ordinal:
        case SqlTypeName.Binary_ordinal:
        case SqlTypeName.Real_ordinal:
        case SqlTypeName.Float_ordinal:
        case SqlTypeName.Double_ordinal:
        case SqlTypeName.Row_ordinal:
        case SqlTypeName.Decimal_ordinal:
            return true;
        case SqlTypeName.Distinct_ordinal:
        default:
            return false;
        }
    }

    // implement FarragoSessionPersonality
    public boolean supportsFeature(ResourceDefinition feature)
    {
        // By default, support everything
        return true;
    }
}

// End FarragoDefaultSessionPersonality.java
