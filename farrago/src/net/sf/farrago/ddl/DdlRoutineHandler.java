/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2004-2005 John V. Sichi
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
package net.sf.farrago.ddl;

import org.eigenbase.util.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.validate.SqlValidatorException;
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
 * DdlRoutineHandler defines DDL handler methods for user-defined
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
                    if ((param.getKind() != null) && (param.getType() == null))
                    {
                        throw validator.newPositionalError(
                            param,
                            validator.res.newValidatorFunctionOutputParam(
                                repos.getLocalizedObjectName(routine)));
                    }
                    param.setKind(ParameterDirectionKindEnum.PDK_IN);
                } else {
                    if (param.getKind() == null) {
                        param.setKind(ParameterDirectionKindEnum.PDK_IN);
                    }
                    if (param.getKind() != ParameterDirectionKindEnum.PDK_IN) {
                        // TODO jvs 25-Feb-2005:  implement OUT and INOUT
                        // params to procedures
                        throw Util.needToImplement(param.getKind());
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

        if (routine.getLanguage() == null) {
            routine.setLanguage(
                ExtensionLanguageEnum.SQL.toString());
        }
        if (routine.getLanguage().equals(
                ExtensionLanguageEnum.SQL.toString()))
        {
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

        if (routine.getSpecification() != null) {
            // This is a method.  For now, it can only be a constructor method.
            CwmClassifier classifier = routine.getSpecification().getOwner();
            if (!routine.getInvocationName().equals(classifier.getName())) {
                throw validator.newPositionalError(
                    routine,
                    validator.res.newValidatorConstructorName(
                        repos.getLocalizedObjectName(routine),
                        repos.getLocalizedObjectName(classifier)));
            }
            if (!routine.getLanguage().equals(
                    ExtensionLanguageEnum.SQL.toString()))
            {
                throw Util.needToImplement(
                    "constructor methods with language "
                    + routine.getLanguage());
            }
            if (returnParam.getType() != classifier) {
                throw validator.newPositionalError(
                    routine,
                    validator.res.newValidatorConstructorType(
                        repos.getLocalizedObjectName(routine)));
            }
        }
    }

    // implement FarragoSessionDdlHandler
    public void validateModification(FemRoutine routine)
    {
        validateDefinition(routine);
    }

    private void validateSqlRoutine(
        FemRoutine routine,
        FemRoutineParameter returnParam)
    {
        if (routine.getDataAccess() == RoutineDataAccessEnum.RDA_NO_SQL) {
            throw validator.newPositionalError(
                routine,
                validator.res.newValidatorRoutineNoSql(
                    repos.getLocalizedObjectName(routine)));
        }
        if (routine.getParameterStyle() != null) {
            throw validator.newPositionalError(
                routine,
                validator.res.newValidatorRoutineNoParamStyle(
                    repos.getLocalizedObjectName(routine)));
        }
        if (routine.getBody() == null) {
            if (routine.getExternalName() == null) {
                // must be a method declaration and we haven't seen
                // the definition yet
                CwmProcedureExpression dummyBody =
                    repos.newCwmProcedureExpression();
                dummyBody.setLanguage(
                    ExtensionLanguageEnum.SQL.toString());
                dummyBody.setBody(";");
                routine.setBody(dummyBody);
                return;
            } else {
                throw validator.newPositionalError(
                    routine,
                    validator.res.newValidatorRoutineBodyMissing(
                        repos.getLocalizedObjectName(routine)));
            }
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
        if (routine.getBody() != null) {
            if ((routine.getBody().getLanguage() != null)
                && !routine.getBody().getLanguage().equals(
                    ExtensionLanguageEnum.JAVA.toString()))
            {
                throw validator.newPositionalError(
                    routine,
                    validator.res.newValidatorRoutineExternalNoBody(
                        repos.getLocalizedObjectName(routine)));
            }
        }

        CwmProcedureExpression dummyBody =
            repos.newCwmProcedureExpression();
        dummyBody.setLanguage(ExtensionLanguageEnum.JAVA.toString());
        dummyBody.setBody(";");
        routine.setBody(dummyBody);

        if (!routine.getLanguage().equals(
                ExtensionLanguageEnum.JAVA.toString()))
        {
            throw validator.newPositionalError(
                routine,
                validator.res.newValidatorRoutineExternalJavaOnly(
                    repos.getLocalizedObjectName(routine)));
        }
        if (routine.getParameterStyle() == null) {
            routine.setParameterStyle(
                RoutineParameterStyleEnum.RPS_JAVA.toString());
        }
        if (!(routine.getParameterStyle().equals(
                  RoutineParameterStyleEnum.RPS_JAVA.toString()))) {
            throw validator.newPositionalError(
                routine,
                validator.res.newValidatorRoutineJavaParamStyleOnly(
                    repos.getLocalizedObjectName(routine)));
        }

        if (routine.getExternalName() == null) {
            // must be a method declaration and we haven't seen
            // the definition yet
            return;
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

        if (FarragoUserDefinedRoutine.hasReturnPrefix(
                routine.getBody().getBody()))
        {
            validateReturnBody(
                routine, session, paramRowType, returnParam);
        } else {
            validateConstructorBody(
                routine, session, paramRowType,
                (FemSqlobjectType) returnParam.getType());
        }
    }

    private void validateReturnBody(
        FemRoutine routine,
        FarragoSession session,
        RelDataType paramRowType,
        FemRoutineParameter returnParam)
        throws Throwable
    {
        FarragoTypeFactory typeFactory = validator.getTypeFactory();
        FarragoSessionAnalyzedSql analyzedSql;
        try {
            analyzedSql = session.analyzeSql(
                FarragoUserDefinedRoutine.removeReturnPrefix(
                    routine.getBody().getBody()),
                typeFactory,
                paramRowType);
        } catch (Throwable ex) {
            throw adjustExceptionParserPosition(routine, ex);
        }

        validator.createDependency(
            routine, analyzedSql.dependencies, "RoutineUsage");

        routine.getBody().setBody(
            FarragoUserDefinedRoutine.addReturnPrefix(
                analyzedSql.canonicalString));

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
    }

    private void validateConstructorBody(
        FemRoutine routine,
        FarragoSession session,
        RelDataType paramRowType,
        FemSqlobjectType objectType)
    {
        FarragoTypeFactory typeFactory = validator.getTypeFactory();
        SqlDialect sqlDialect = new SqlDialect(session.getDatabaseMetaData());
        FarragoSessionParser parser = session.newParser();
        SqlNodeList nodeList = (SqlNodeList)
            parser.parseSqlText(
                validator,
                routine.getBody().getBody(),
                true);
        Iterator iter = nodeList.getList().iterator();
        StringBuffer newBody = new StringBuffer();
        newBody.append("BEGIN ");
        Set dependencies = new HashSet();
        while (iter.hasNext()) {
            SqlCall call = (SqlCall) iter.next();
            SqlIdentifier lhs = (SqlIdentifier) call.getOperands()[0];
            SqlNode expr = call.getOperands()[1];
            FemSqltypeAttribute attribute = (FemSqltypeAttribute)
                FarragoCatalogUtil.getModelElementByName(
                    objectType.getFeature(),
                    lhs.getSimple());
            if (attribute == null) {
                throw validator.res.newValidatorConstructorAssignmentUnknown(
                    repos.getLocalizedObjectName(lhs.getSimple()));
            }
            FarragoSessionAnalyzedSql analyzedSql;
            // TODO jvs 26-Feb-2005:  need to figure out how to
            // adjust parser pos in error msgs
            analyzedSql = session.analyzeSql(
                expr.toSqlString(sqlDialect),
                typeFactory,
                paramRowType);
            if (analyzedSql.hasDynamicParams) {
                throw validator.res.newValidatorInvalidRoutineDynamicParam();
            }
            RelDataType lhsType =
                typeFactory.createCwmElementType(attribute);
            RelDataType rhsType = analyzedSql.resultType;
            if (!SqlTypeUtil.canAssignFrom(lhsType, rhsType)) {
                throw validator.res.newValidatorConstructorAssignmentType(
                    rhsType.toString(),
                    lhsType.toString(),
                    repos.getLocalizedObjectName(attribute));
            }
            newBody.append("SET SELF.");
            newBody.append(lhs.toSqlString(sqlDialect));
            newBody.append(" = ");
            newBody.append(analyzedSql.canonicalString);
            newBody.append("; ");
            dependencies.addAll(analyzedSql.dependencies);
        }
        newBody.append("RETURN SELF; END");
        routine.getBody().setBody(newBody.toString());
        validator.createDependency(
            routine, dependencies, "RoutineUsage");
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
        validateUserDefinedType(typeDef);
    }

    // implement FarragoSessionDdlHandler
    public void validateDefinition(FemSqldistinguishedType typeDef)
    {
        validateUserDefinedType(typeDef);
        validateTypedElement(typeDef);
        if (!(typeDef.getType() instanceof CwmSqlsimpleType)) {
            throw validator.newPositionalError(
                typeDef,
                validator.res.newValidatorDistinctTypePredefined(
                    repos.getLocalizedObjectName(typeDef)));
        }
        CwmSqlsimpleType predefinedType = (CwmSqlsimpleType) typeDef.getType();
        typeDef.setSqlSimpleType(predefinedType);
    }

    // implement FarragoSessionDdlHandler
    public void validateDefinition(FemUserDefinedOrdering orderingDef)
    {
        if (orderingDef.getName() == null) {
            orderingDef.setName(orderingDef.getType().getName());
        }
        if (orderingDef.getType().getOrdering().size() > 1) {
            throw validator.newPositionalError(
                orderingDef,
                validator.res.newValidatorMultipleOrderings(
                    repos.getLocalizedObjectName(orderingDef.getType())));
        }
    }
    
    private void validateUserDefinedType(FemUserDefinedType typeDef)
    {
        if (typeDef.isFinal() && typeDef.isAbstract()) {
            throw validator.newPositionalError(
                typeDef,
                validator.res.newValidatorFinalAbstractType(
                    repos.getLocalizedObjectName(typeDef)));
        }

        // NOTE jvs 13-Feb-2005: Once we support inheritance, we will allow
        // abstract and non-final for FemSqlobjectTypes.

        if (!typeDef.isFinal()) {
            throw validator.newPositionalError(
                typeDef,
                validator.res.newValidatorNonFinalType(
                    repos.getLocalizedObjectName(typeDef)));
        }

        if (typeDef.isAbstract()) {
            throw validator.newPositionalError(
                typeDef,
                validator.res.newValidatorNonInstantiableType(
                    repos.getLocalizedObjectName(typeDef)));
        }
    }
}

// End DdlRoutineHandler.java
