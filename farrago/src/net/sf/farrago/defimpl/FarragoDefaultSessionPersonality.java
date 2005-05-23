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

import org.eigenbase.jmi.*;
import org.eigenbase.oj.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;

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

import java.util.*;

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
        handlerList.add(medHandler);
        handlerList.add(new DdlRoutineHandler(ddlValidator));
        handlerList.add(new DdlRelationalHandler(medHandler));

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
        // CASCADE, they go away (a special case later on takes care of
        // cascading to the dependent object as well).
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
    public JmiQueryProcessor getJmiQueryProcessor()
    {
        // TODO:  share a common instance, query plan caching, all that
        return new LurqlQueryProcessor(
            database.getSystemRepos().getMdrRepos());
    }
}

// End FarragoDefaultSessionPersonality.java
