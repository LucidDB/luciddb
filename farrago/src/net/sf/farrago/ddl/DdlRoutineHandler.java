/*
// $Id$
// Farrago is a relational database management system.
// Copyright (C) 2004-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/
package net.sf.farrago.ddl;

import org.eigenbase.util.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.query.*;
import net.sf.farrago.session.*;
import net.sf.farrago.type.*;
import net.sf.farrago.util.*;
import net.sf.farrago.plugin.*;

import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.cwm.relational.enumerations.*;
import net.sf.farrago.cwm.behavioral.*;
import net.sf.farrago.fem.sql2003.*;

import java.io.*;
import java.net.*;
import java.sql.*;
import java.util.*;

/**
 * DdlRelationalHandler defines DDL handler methods for user-defined
 * routines and related objects such as types and jars.  TODO:
 * rename this class to DdlUserDefHandler
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class DdlRoutineHandler extends DdlHandler
{
    public DdlRoutineHandler(DdlValidator validator)
    {
        super(validator);
    }

    // implement FarragoSessionDdlHandler
    public void validateDefinition(FemRoutine routine)
    {
        Iterator iter = routine.getParameter().iterator();
        int iOrdinal = 0;
        FemRoutineParameter returnParam = null;
        while (iter.hasNext()) {
            FemRoutineParameter param = (FemRoutineParameter) iter.next();
            if (param.getKind() == ParameterDirectionKindEnum.PDK_RETURN) {
                returnParam = param;
            } else {
                if (routine.getType().equals(ProcedureTypeEnum.FUNCTION)) {
                    if (param.getKind() != ParameterDirectionKindEnum.PDK_IN) {
                        throw validator.newPositionalError(
                            param,
                            validator.res.newValidatorFunctionOutputParam(
                                repos.getLocalizedObjectName(routine)));
                    }
                }
                param.setOrdinal(iOrdinal);
                ++iOrdinal;
            }
            validateRoutineParam(param);
        }
        if (routine.getDataAccess() == null) {
            throw validator.newPositionalError(
                routine,
                validator.res.newValidatorRoutineDataAccessUnspecified(
                    repos.getLocalizedObjectName(routine)));
        }

        if (routine.getBody() != null) {
            validateSqlRoutine(routine, returnParam);
        } else {
            validateJavaRoutine(routine, returnParam);
        }

        // make sure routine signature doesn't conflict with other routines
        FarragoUserDefinedRoutineLookup lookup =
            new FarragoUserDefinedRoutineLookup(
                validator.getStmtValidator(),
                null,
                routine);
        FarragoUserDefinedRoutine prototype = lookup.convertRoutine(routine);
        SqlIdentifier invocationName = prototype.getSqlIdentifier();
        invocationName.names[invocationName.names.length - 1] =
            routine.getInvocationName();
        List list = SqlUtil.lookupSubjectRoutines(
            lookup,
            invocationName,
            prototype.getParamTypes(),
            routine.getType().equals(ProcedureTypeEnum.PROCEDURE));

        // should find at least this routine!
        assert(!list.isEmpty());

        if (list.size() > 1) {
            throw validator.newPositionalError(
                routine,
                validator.res.newValidatorRoutineConflict(
                    repos.getLocalizedObjectName(routine)));
        }
    }

    private void validateSqlRoutine(
        FemRoutine routine,
        FemRoutineParameter returnParam)
    {
        if (routine.getLanguage() == null) {
            routine.setLanguage("SQL");
        }
        // TODO jvs 11-Jan-2005:  enum for supported languages
        if (!routine.getLanguage().equals("SQL")) {
            throw validator.newPositionalError(
                routine,
                validator.res.newValidatorRoutineBodySqlOnly(
                    repos.getLocalizedObjectName(routine)));
        }
        if (routine.getDataAccess() == RoutineDataAccessEnum.RDA_NO_SQL) {
            throw validator.newPositionalError(
                routine,
                validator.res.newValidatorRoutineNoSql(
                    repos.getLocalizedObjectName(routine)));
        }
        if (routine.getParameterStyle() == null) {
            routine.setParameterStyle(RoutineParameterStyleEnum.RPS_SQL);
        }
        if (routine.getParameterStyle() !=
            RoutineParameterStyleEnum.RPS_SQL)
        {
            throw validator.newPositionalError(
                routine,
                validator.res.newValidatorRoutineSqlParamStyleOnly(
                    repos.getLocalizedObjectName(routine)));
        }
        FarragoSession session = validator.newReentrantSession();
        try {
            validateRoutineBody(session, routine, returnParam);
        } catch (FarragoUnvalidatedDependencyException ex) {
            // pass this one through
            throw ex;
        } catch (Throwable ex) {
            throw validator.res.newValidatorInvalidObjectDefinition(
                repos.getLocalizedObjectName(routine), 
                ex);
        } finally {
            validator.releaseReentrantSession(session);
        }
    }

    private void validateJavaRoutine(
        FemRoutine routine, 
        FemRoutineParameter returnParam)
    {
        CwmProcedureExpression dummyBody =
            repos.newCwmProcedureExpression();
        dummyBody.setLanguage("JAVA");
        dummyBody.setBody(";");
        routine.setBody(dummyBody);
        
        if (routine.getLanguage() == null) {
            routine.setLanguage("JAVA");
        }
        if (!routine.getLanguage().equals("JAVA")) {
            throw validator.newPositionalError(
                routine,
                validator.res.newValidatorRoutineExternalJavaOnly(
                    repos.getLocalizedObjectName(routine)));
        }
        if (routine.getParameterStyle() == null) {
            routine.setParameterStyle(RoutineParameterStyleEnum.RPS_JAVA);
        }
        if (routine.getParameterStyle() != RoutineParameterStyleEnum.RPS_JAVA) {
            throw validator.newPositionalError(
                routine,
                validator.res.newValidatorRoutineJavaParamStyleOnly(
                    repos.getLocalizedObjectName(routine)));
        }

        FarragoUserDefinedRoutineLookup lookup =
            new FarragoUserDefinedRoutineLookup(
                validator.getStmtValidator(), null, null);
        FarragoUserDefinedRoutine sqlRoutine = lookup.convertRoutine(routine);
        try {
            sqlRoutine.getJavaMethod();
        } catch (SqlValidatorException ex) {
            throw validator.newPositionalError(routine,ex);
        }
        if (sqlRoutine.getJar() != null) {
            validator.createDependency(
                routine,
                Collections.singleton(sqlRoutine.getJar()),
                "RoutineUsesJar");
        }
    }

    private void validateRoutineBody(
        FarragoSession session, 
        final FemRoutine routine,
        FemRoutineParameter returnParam)
        throws Throwable
    {
        final FarragoTypeFactory typeFactory = validator.getTypeFactory();
        final List params = routine.getParameter();

        RelDataType paramRowType = typeFactory.createStructType(
            new RelDataTypeFactory.FieldInfo() 
            {
                public int getFieldCount()
                {
                    return FarragoCatalogUtil.getRoutineParamCount(routine);
                }
                
                public String getFieldName(int index)
                {
                    FemRoutineParameter param =
                        (FemRoutineParameter) params.get(index);
                    return param.getName();
                }

                public RelDataType getFieldType(int index)
                {
                    FemRoutineParameter param =
                        (FemRoutineParameter) params.get(index);
                    return typeFactory.createCwmElementType(param);
                }
            });

        tracer.fine(routine.getBody().getBody());
        
        FarragoSessionAnalyzedSql analyzedSql;
        try {
            analyzedSql = session.analyzeSql(
                FarragoUserDefinedRoutine.removeReturnPrefix(
                    routine.getBody().getBody()),
                paramRowType);
        } catch (Throwable ex) {
            throw adjustExceptionParserPosition(routine, ex);
        }
        
        if (analyzedSql.hasDynamicParams) {
            // TODO jvs 29-Dec-2004:  add a test for this; currently
            // hits an earlier assertion in SqlValidator
            throw validator.res.newValidatorInvalidRoutineDynamicParam();
        }

        // TODO jvs 28-Dec-2004:  CAST FROM
        
        RelDataType declaredReturnType =
            typeFactory.createCwmElementType(returnParam);
        RelDataType actualReturnType = analyzedSql.resultType;
        if (!SqlTypeUtil.canAssignFrom(declaredReturnType, actualReturnType)) {
            throw validator.res.newValidatorFunctionReturnType(
                actualReturnType.toString(),
                repos.getLocalizedObjectName(routine),
                declaredReturnType.toString());
        }

        validator.createDependency(
            routine, analyzedSql.dependencies, "RoutineUsage");
        
        routine.getBody().setBody(
            FarragoUserDefinedRoutine.addReturnPrefix(
                analyzedSql.canonicalString));
    }

    public void validateRoutineParam(FemRoutineParameter param)
    {
        validateTypedElement(param);
    }
    
    // implement FarragoSessionDdlHandler
    public void validateDefinition(FemJar jar)
    {
        // TODO jvs 19-Jan-2005: implement deployment descriptors, and
        // (optionally?) copy jar to an area managed by Farrago
        URL url;
        try {
            url = new URL(jar.getUrl());
        } catch (MalformedURLException ex) {
            throw validator.res.newPluginMalformedJarUrl(
                repos.getLocalizedObjectName(jar.getUrl()),
                repos.getLocalizedObjectName(jar),
                ex);
        }
    }

    // implement FarragoSessionDdlHandler
    public void validateDefinition(FemSqlobjectType typeDef)
    {
        validateAttributeSet(typeDef);
    }
}

// End DdlRoutineHandler.java
