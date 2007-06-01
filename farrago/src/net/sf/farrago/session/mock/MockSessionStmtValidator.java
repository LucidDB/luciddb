/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2007-2007 The Eigenbase Project
// Copyright (C) 2007-2007 Disruptive Tech
// Copyright (C) 2007-2007 LucidEra, Inc.
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
package net.sf.farrago.session.mock;

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.cwm.relational.enumerations.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.fennel.*;
import net.sf.farrago.namespace.util.*;
import net.sf.farrago.session.*;
import net.sf.farrago.type.*;
import net.sf.farrago.util.*;

import org.eigenbase.resgen.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.trace.*;
import org.eigenbase.util.*;


/**
 * MockSessionStmtValidator provides a bare-bones implementation of
 * FarragoSessionStmtValidator. It primarily exists to support the retrieval of
 * a MockReposTxnContext in the SQL parser.
 *
 * @author stephan/jack
 * @version $Id$
 * @since Dec 8, 2006
 */
public class MockSessionStmtValidator
    implements FarragoSessionStmtValidator
{
    //~ Instance fields --------------------------------------------------------

    FarragoSessionParser parser;
    FarragoReposTxnContext reposTxnContext;

    //~ Methods ----------------------------------------------------------------

    public void setParser(FarragoSessionParser parser)
    {
        this.parser = parser;
    }

    public FarragoSessionParser getParser()
    {
        return parser;
    }

    public void setReposTxnContext(FarragoReposTxnContext reposTxnContext)
    {
        this.reposTxnContext = reposTxnContext;
    }

    public FarragoReposTxnContext getReposTxnContext()
    {
        return reposTxnContext;
    }

    public FarragoSession getSession()
    {
        throw new UnsupportedOperationException();
    }

    public FarragoRepos getRepos()
    {
        throw new UnsupportedOperationException();
    }

    public FennelDbHandle getFennelDbHandle()
    {
        throw new UnsupportedOperationException();
    }

    public FarragoTypeFactory getTypeFactory()
    {
        throw new UnsupportedOperationException();
    }

    public FarragoSessionVariables getSessionVariables()
    {
        throw new UnsupportedOperationException();
    }

    public FarragoObjectCache getCodeCache()
    {
        throw new UnsupportedOperationException();
    }

    public FarragoDataWrapperCache getDataWrapperCache()
    {
        throw new UnsupportedOperationException();
    }

    public FarragoSessionIndexMap getIndexMap()
    {
        throw new UnsupportedOperationException();
    }

    public FarragoObjectCache getSharedDataWrapperCache()
    {
        throw new UnsupportedOperationException();
    }

    public FarragoSessionPrivilegeChecker getPrivilegeChecker()
    {
        throw new UnsupportedOperationException();
    }

    public FarragoDdlLockManager getDdlLockManager()
    {
        throw new UnsupportedOperationException();
    }

    public void requestPrivilege(CwmModelElement obj, String action)
    {
    }

    public CwmColumn findColumn(
        CwmNamedColumnSet namedColumnSet,
        String columnName)
    {
        return null;
    }

    public CwmCatalog findCatalog(String catalogName)
    {
        return null;
    }

    public CwmCatalog getDefaultCatalog()
    {
        return null;
    }

    public FemLocalSchema findSchema(SqlIdentifier schemaName)
    {
        return null;
    }

    public FemDataWrapper findDataWrapper(
        SqlIdentifier wrapperName,
        boolean isForeign)
    {
        return null;
    }

    public FemDataServer findDataServer(SqlIdentifier serverName)
    {
        return null;
    }

    public FemDataServer getDefaultLocalDataServer()
    {
        return null;
    }

    public <T extends CwmModelElement> T findSchemaObject(
        SqlIdentifier qualifiedName,
        Class<T> clazz)
    {
        return null;
    }

    public <T extends CwmModelElement> T findUnqualifiedObject(
        SqlIdentifier unqualifiedName,
        Class<T> clazz)
    {
        return null;
    }

    public List<FemRoutine> findRoutineOverloads(
        SqlIdentifier invocationName,
        ProcedureType routineType)
    {
        return null;
    }

    public CwmSqldataType findSqldataType(SqlIdentifier typeName)
    {
        return null;
    }

    public FemJar findJarFromLiteralName(String jarName)
    {
        return null;
    }

    public <T extends CwmModelElement> FarragoSessionResolvedObject<T>
    resolveSchemaObjectName(String [] names, Class<T> clazz)
    {
        return null;
    }

    public SqlMoniker [] getAllSchemaObjectNames(String [] names)
    {
        return null;
    }

    public void setParserPosition(SqlParserPos pos)
    {
    }

    public void validateFeature(
        ResourceDefinition feature,
        SqlParserPos context)
    {
    }

    public void setTimingTracer(EigenbaseTimingTracer timingTracer)
    {
    }

    public EigenbaseTimingTracer getTimingTracer()
    {
        return null;
    }

    public CwmNamedColumnSet getSampleDataset(
        CwmNamedColumnSet columnSet,
        String datasetName)
    {
        return null;
    }

    public void validateDataType(SqlDataTypeSpec dataType)
        throws SqlValidatorException
    {
    }

    public void addAllocation(ClosableAllocation allocation)
    {
    }

    public void closeAllocation()
    {
    }

    public void setWarningQueue(FarragoWarningQueue queue)
    {
    }

    public FarragoWarningQueue getWarningQueue()
    {
        return null;
    }
}

// End MockSessionStmtValidator.java
