/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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
package net.sf.farrago.query;

import java.lang.reflect.*;

import java.sql.*;

import java.util.*;
import java.util.List;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.behavioral.*;
import net.sf.farrago.cwm.relational.enumerations.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.ojrex.*;
import net.sf.farrago.plugin.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.session.*;

import openjava.ptree.*;

import org.eigenbase.oj.rex.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.util.*;


/**
 * FarragoUserDefinedRoutine extends {@link SqlFunction} with a repository
 * reference to a specific user-defined routine.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoUserDefinedRoutine
    extends SqlFunction
    implements OJRexImplementor
{
    //~ Static fields/initializers ---------------------------------------------

    private static final String RETURN_PREFIX = "RETURN ";

    //~ Instance fields --------------------------------------------------------

    private final FemRoutine routine;

    private final FarragoSessionStmtValidator stmtValidator;

    private final FarragoPreparingStmt preparingStmt;

    private FemJar femJar;

    // NOTE jvs 6-Aug-2006:  This is non-final because it may be expanded
    // by UDX type derivation.
    private RelDataType returnType;

    //~ Constructors -----------------------------------------------------------

    public FarragoUserDefinedRoutine(
        FarragoSessionStmtValidator stmtValidator,
        FarragoPreparingStmt preparingStmt,
        FemRoutine routine,
        RelDataType returnType,
        RelDataType [] paramTypes)
    {
        super(
            FarragoCatalogUtil.getQualifiedName(routine),
            FarragoCatalogUtil.isTableFunction(routine)
            ? new TableFunctionReturnTypeInference(
                returnType,
                getRoutineParamNames(routine))
            : new ExplicitReturnTypeInference(returnType),
            new ExplicitOperandTypeInference(paramTypes),
            new AssignableOperandTypeChecker(paramTypes),
            paramTypes,
            (routine.getType() == ProcedureTypeEnum.PROCEDURE)
            ? SqlFunctionCategory.UserDefinedProcedure
            : (FarragoCatalogUtil.isRoutineConstructor(routine)
                ? SqlFunctionCategory.UserDefinedConstructor
                : SqlFunctionCategory.UserDefinedSpecificFunction));
        this.stmtValidator = stmtValidator;
        this.preparingStmt = preparingStmt;
        this.routine = routine;
        this.returnType = returnType;

        if ((preparingStmt != null)
            && (routine != null)
            && isDynamicFunction())
        {
            this.preparingStmt.disableStatementCaching();
        }
    }

    //~ Methods ----------------------------------------------------------------

    private static List<String> getRoutineParamNames(FemRoutine femRoutine)
    {
        List<String> list = new ArrayList<String>();
        for (
            FemRoutineParameter param
            : Util.cast(femRoutine.getParameter(), FemRoutineParameter.class))
        {
            if (param.getKind() == ParameterDirectionKindEnum.PDK_RETURN) {
                continue;
            }
            list.add(param.getName());
        }
        return list;
    }

    public FarragoPreparingStmt getPreparingStmt()
    {
        return preparingStmt;
    }

    public FemRoutine getFemRoutine()
    {
        return routine;
    }

    // override SqlOperator
    protected void preValidateCall(
        SqlValidator validator,
        SqlValidatorScope scope,
        SqlCall call)
    {
        if (!hasDefinition()) {
            throw FarragoResource.instance().ValidatorConstructorUndefined.ex(
                getAllowedSignatures());
        }
        super.preValidateCall(validator, scope, call);
    }

    public RelDataType getReturnType()
    {
        return returnType;
    }

    public boolean isTableFunction()
    {
        // TODO jvs 9-Jan-2006:  somewhere or other we need to validate
        // that a table function can't be invoked from anywhere other
        // than as a table reference; or rather, if invoked elsewhere,
        // it should return a MULTISET value per the standard.
        return FarragoCatalogUtil.isTableFunction(routine);
    }

    public FemJar getJar()
    {
        return femJar;
    }

    /**
     * Uses an external Java routine definition plus reflection to find a
     * corresponding Java method.
     *
     * @return Java method
     *
     * @exception SqlValidatorException if matching Java method could not be
     * found
     */
    public Method getJavaMethod()
        throws SqlValidatorException
    {
        FarragoRepos repos = stmtValidator.getRepos();

        // TODO jvs 18-Jan-2005:  support OUT and INOUT parameters

        String externalName = routine.getExternalName();

        // TODO jvs 11-Jan-2005:  move some of this code to FarragoPluginCache
        String jarName = null;
        String fullMethodName;
        if (!FarragoPluginClassLoader.isLibraryClass(
                externalName))
        {
            int iColon = externalName.indexOf(':');
            if (iColon == -1) {
                // force error below
                fullMethodName = "";
            } else {
                fullMethodName = externalName.substring(iColon + 1);
                jarName = externalName.substring(0, iColon);
            }
        } else {
            fullMethodName =
                FarragoPluginClassLoader.getLibraryClassReference(
                    externalName);
        }
        int iLeftParen = fullMethodName.indexOf('(');
        String classPlusMethodName;
        if (iLeftParen == -1) {
            classPlusMethodName = fullMethodName;
        } else {
            classPlusMethodName = fullMethodName.substring(0, iLeftParen);
        }
        int iLastDot = classPlusMethodName.lastIndexOf('.');
        if (iLastDot == -1) {
            throw FarragoResource.instance().ValidatorRoutineInvalidJavaMethod
            .ex(
                repos.getLocalizedObjectName(routine),
                repos.getLocalizedObjectName(externalName));
        }
        String javaClassName = classPlusMethodName.substring(0, iLastDot);
        String javaMethodName = classPlusMethodName.substring(iLastDot + 1);
        int nParams = FarragoCatalogUtil.getRoutineParamCount(routine);
        int nJavaParams = nParams;

        if (isTableFunction()) {
            // one extra for PreparedStatement
            ++nJavaParams;
        }

        Class [] javaParamClasses = new Class[nJavaParams];
        boolean hasListParam = false;
        if (iLeftParen == -1) {
            for (int i = 0; i < nParams; ++i) {
                RelDataType type = getParamTypes()[i];
                javaParamClasses[i] =
                    stmtValidator.getTypeFactory().getClassForJavaParamStyle(
                        type);
                if (javaParamClasses[i] == null) {
                    throw Util.needToImplement(type);
                }
                if (javaParamClasses[i] == List.class) {
                    hasListParam = true;
                }
            }
            if (isTableFunction()) {
                javaParamClasses[nParams] = PreparedStatement.class;
            }
        } else {
            int iNameStart = iLeftParen + 1;
            boolean last = false;
            int i = 0;
            for (; (i < nJavaParams) && !last; ++i) {
                int iComma = fullMethodName.indexOf(',', iNameStart);
                if (iComma == -1) {
                    iComma = fullMethodName.indexOf(')', iNameStart);

                    // TODO:  assert nothing past rparen
                    if (iComma == -1) {
                        throw FarragoResource.instance()
                        .ValidatorRoutineInvalidJavaMethod.ex(
                            repos.getLocalizedObjectName(routine),
                            repos.getLocalizedObjectName(externalName));
                    }
                    last = true;
                }
                String typeName =
                    fullMethodName.substring(
                        iNameStart,
                        iComma);
                Class paramClass;
                try {
                    paramClass = ReflectUtil.getClassForName(typeName);
                } catch (Exception ex) {
                    // TODO jvs 16-Jan-2005:  more specific err msg
                    throw FarragoResource.instance().PluginInitFailed.ex(
                        javaClassName,
                        ex);
                }
                javaParamClasses[i] = paramClass;
                iNameStart = iComma + 1;
            }
            if (!last || (i != nJavaParams)) {
                // TODO jvs 16-Jan-2005:  specific err msg for mismatch
                // between number of SQL routine parameters and number of
                // Java method parameters
                throw FarragoResource.instance().PluginInitFailed.ex(
                    javaClassName);
            }
        }

        Class javaClass;
        if (jarName == null) {
            try {
                javaClass = Class.forName(javaClassName);
            } catch (Exception ex) {
                throw FarragoResource.instance().PluginInitFailed.ex(
                    javaClassName,
                    ex);
            }
        } else {
            if (femJar == null) {
                femJar = stmtValidator.findJarFromLiteralName(jarName);
            }
            String url = FarragoCatalogUtil.getJarUrl(femJar);
            javaClass =
                stmtValidator.getSession().getPluginClassLoader()
                .loadClassFromJarUrl(url, javaClassName);
            if (preparingStmt != null) {
                preparingStmt.addJarUrl(url);
            }
        }

        String javaUnmangledMethodName =
            ReflectUtil.getUnmangledMethodName(
                javaClass,
                javaMethodName,
                javaParamClasses);

        Method javaMethod;
        try {
            javaMethod = javaClass.getMethod(javaMethodName, javaParamClasses);
        } catch (NoSuchMethodException ex) {
            throw FarragoResource.instance().ValidatorRoutineJavaMethodNotFound
            .ex(
                repos.getLocalizedObjectName(routine),
                repos.getLocalizedObjectName(javaUnmangledMethodName));
        } catch (Exception ex) {
            throw FarragoResource.instance().PluginInitFailed.ex(
                javaClassName,
                ex);
        }

        int modifiers = javaMethod.getModifiers();
        if (!Modifier.isStatic(modifiers) || !Modifier.isPublic(modifiers)) {
            throw FarragoResource.instance()
            .ValidatorRoutineJavaMethodNotPublicStatic.ex(
                repos.getLocalizedObjectName(routine),
                repos.getLocalizedObjectName(javaUnmangledMethodName));
        }

        // verify compatibility between SQL and Java types for return type
        // and all parameters
        JavaToSqlTypeConversionRules rules =
            JavaToSqlTypeConversionRules.instance();

        Class javaReturnClass = javaMethod.getReturnType();
        if ((routine.getType() == ProcedureTypeEnum.FUNCTION)
            && (!isTableFunction()))
        {
            SqlTypeName actualReturnSqlType = rules.lookup(javaReturnClass);
            SqlTypeName declReturnSqlType = returnType.getSqlTypeName();
            if (!checkCompatibility(actualReturnSqlType, declReturnSqlType)) {
                throw FarragoResource.instance()
                .ValidatorRoutineJavaReturnMismatch.ex(
                    repos.getLocalizedObjectName(routine),
                    returnType.toString(),
                    repos.getLocalizedObjectName(javaUnmangledMethodName),
                    javaReturnClass.toString());
            }
        } else {
            if (!javaReturnClass.equals(void.class)) {
                throw FarragoResource.instance()
                .ValidatorRoutineJavaProcReturnVoid.ex(
                    repos.getLocalizedObjectName(routine),
                    repos.getLocalizedObjectName(javaUnmangledMethodName),
                    javaReturnClass.toString());
            }
        }

        for (int i = 0; i < nParams; ++i) {
            Class javaParamClass = javaParamClasses[i];
            SqlTypeName actualParamSqlType = rules.lookup(javaParamClass);
            SqlTypeName declParamSqlType = getParamTypes()[i].getSqlTypeName();
            if (!checkCompatibility(actualParamSqlType, declParamSqlType)) {
                throw FarragoResource.instance()
                .ValidatorRoutineJavaParamMismatch.ex(
                    repos.getLocalizedObjectName(
                        (FemRoutineParameter) (routine.getParameter().get(i))),
                    repos.getLocalizedObjectName(routine),
                    getParamTypes()[i].toString(),
                    repos.getLocalizedObjectName(javaUnmangledMethodName),
                    javaParamClass.toString());
            }
            if (isTableFunction()) {
                if (javaParamClasses[nParams] != PreparedStatement.class) {
                    throw FarragoResource.instance()
                    .ValidatorRoutineJavaParamMismatch.ex(
                        "RETURNS TABLE",
                        repos.getLocalizedObjectName(routine),
                        "java.sql.PreparedStatement",
                        repos.getLocalizedObjectName(
                            javaUnmangledMethodName),
                        javaParamClass.toString());
                }
            }
        }

        // verify that List parameters corresponding to COLUMN_LIST parameters
        // are declared as List<String>
        if (hasListParam) {
            if (!validateListParams(javaMethod, javaParamClasses)) {
                throw FarragoResource.instance().ValidatorInvalidColumnListParam
                .ex(
                    repos.getLocalizedObjectName(routine),
                    repos.getLocalizedObjectName(
                        javaUnmangledMethodName));
            }
        }

        return javaMethod;
    }

    private static boolean checkCompatibility(SqlTypeName t1, SqlTypeName t2)
    {
        if ((t1 == null) || (t2 == null)) {
            return false;
        }
        return t1.getFamily() == t2.getFamily();
    }

    /**
     * Examines parameters corresponding to List parameters and ensures that
     * they are List&lt;String&gt; types
     *
     * @param javaMethod the java method containing the parameters
     * @param javaParamClasses classes of the parameter
     *
     * @return true if List parameters are List&lt;String&gt; parameters
     */
    private boolean validateListParams(
        Method javaMethod,
        Class [] javaParamClasses)
    {
        Type [] genericTypes = javaMethod.getGenericParameterTypes();
        for (int i = 0; i < javaParamClasses.length; i++) {
            if (javaParamClasses[i] == List.class) {
                if (!(genericTypes[i] instanceof ParameterizedType)) {
                    return false;
                }
                ParameterizedType pType = (ParameterizedType) genericTypes[i];
                Type [] typeArgs = pType.getActualTypeArguments();
                if ((typeArgs.length != 1) || !(typeArgs[0] instanceof Class)) {
                    return false;
                }
                Class typeClass = (Class) typeArgs[0];
                if (typeClass != String.class) {
                    return false;
                }
            }
        }

        return true;
    }

    // implement OJRexImplementor
    public Expression implement(
        RexToOJTranslator translator,
        RexCall call,
        Expression [] operands)
    {
        FarragoRexToOJTranslator farragoTranslator =
            (FarragoRexToOJTranslator) translator;

        assert (call.getOperator() == this);
        Method method;
        try {
            method = getJavaMethod();
        } catch (SqlValidatorException ex) {
            throw FarragoResource.instance().PluginMethodMismatch.ex(ex);
        }

        int nJavaArgs = operands.length;
        if (isTableFunction()) {
            ++nJavaArgs;
        }

        Expression [] args = new Expression[nJavaArgs];
        Class [] javaParams = method.getParameterTypes();
        for (int i = 0; i < operands.length; ++i) {
            args[i] =
                translateOperand(
                    farragoTranslator,
                    (FemRoutineParameter) routine.getParameter().get(i),
                    operands[i],
                    javaParams[i],
                    getParamTypes()[i]);
        }

        if (isTableFunction()) {
            // NOTE jvs 8-Jan-2006:  "this" here refers to the
            // calling instance of FarragoJavaUdxIterator
            args[operands.length] =
                new MethodCall(
                    "getResultInserter",
                    new ExpressionList());
        }

        FarragoOJRexStaticMethodImplementor implementor =
            new FarragoOJRexStaticMethodImplementor(
                method,
                routine.getDataAccess() != RoutineDataAccessEnum.RDA_NO_SQL,
                returnType);
        Expression varResult =
            implementor.implementFarrago(farragoTranslator, call, args);

        if (method.getReturnType() == Void.TYPE) {
            // for procedures, need to skip extra conversions below
            return varResult;
        }

        RelDataType actualReturnType;
        if (method.getReturnType().isPrimitive()) {
            actualReturnType =
                stmtValidator.getTypeFactory().createTypeWithNullability(
                    returnType,
                    false);
        } else {
            actualReturnType =
                stmtValidator.getTypeFactory().createJavaType(
                    method.getReturnType());
        }
        return farragoTranslator.convertCastOrAssignment(
            call.toString(),
            returnType,
            actualReturnType,
            null,
            varResult);
    }

    // implement OJRexImplementor
    public boolean canImplement(RexCall call)
    {
        return call.getOperator() == this;
    }

    private Expression translateOperand(
        FarragoRexToOJTranslator farragoTranslator,
        FemRoutineParameter param,
        Expression argExpr,
        Class javaParamClass,
        RelDataType paramType)
    {
        if (javaParamClass.isPrimitive()) {
            // TODO jvs 16-Jan-2005:  specialize error message for
            // case of NULL argument detection; also optimize
            // away NULL detection when the routine is declared as
            // RETURNS NULL ON NULL INPUT
            return farragoTranslator.convertCastOrAssignment(
                preparingStmt.getRepos().getLocalizedObjectName(
                    param.getName()),
                stmtValidator.getTypeFactory().createTypeWithNullability(
                    paramType,
                    false),
                paramType,
                null,
                argExpr);
        } else {
            return argExpr;
        }
    }

    public boolean hasDefinition()
    {
        if (routine.getLanguage().equals(
                ExtensionLanguageEnum.SQL.toString()))
        {
            return !routine.getBody().getBody().equals(";");
        } else {
            return routine.getExternalName() != null;
        }
    }

    public boolean requiresDecimalExpansion()
    {
        return false;
    }

    public static String removeReturnPrefix(String body)
    {
        assert (hasReturnPrefix(body));
        return body.substring(RETURN_PREFIX.length());
    }

    public static String addReturnPrefix(String body)
    {
        assert (!hasReturnPrefix(body));
        return RETURN_PREFIX + body;
    }

    public static boolean hasReturnPrefix(String body)
    {
        return body.startsWith(RETURN_PREFIX);
    }

    public boolean isDynamicFunction()
    {
        return getFemRoutine().isDynamicFunction();
    }

    // override SqlOperator
    public boolean isDeterministic()
    {
        return getFemRoutine().isDeterministic();
    }

    // override SqlOperator
    public RelDataType inferReturnType(
        SqlOperatorBinding opBinding)
    {
        returnType = super.inferReturnType(opBinding);
        return returnType;
    }
}

// End FarragoUserDefinedRoutine.java
