/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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

package org.eigenbase.sql;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.resource.EigenbaseResource;
import org.eigenbase.sql.parser.SqlParserPos;
import org.eigenbase.sql.parser.SqlParserUtil;
import org.eigenbase.sql.test.SqlOperatorTests;
import org.eigenbase.sql.test.SqlTester;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.validate.SqlValidatorScope;
import org.eigenbase.sql.validate.SqlValidator;
import org.eigenbase.util.Util;
import org.eigenbase.util.EnumeratedValues;

import java.util.List;

/**
 * Defines the BETWEEN operator.
 *
 * <p>Syntax:
 * <blockquote><code>X [NOT] BETWEEN [ASYMMETRIC | SYMMETRIC] Y AND
 * Z</code></blockquote>
 *
 * <p>If the asymmetric/symmeteric keywords are left out ASYMMETRIC is default.
 *
 * <p>This operator is always expanded (into something like <code>Y &lt;= X
 * AND X &lt;= Z</code>) before being converted into Rex nodes.
 *
 * @author Wael Chatila
 * @since Jun 9, 2004
 * @version $Id$
 */
public class SqlBetweenOperator extends SqlInfixOperator
{
    //~ Static fields/initializers --------------------------------------------

    private static final String [] betweenNames =
        new String [] { "BETWEEN", "AND" };
    private static final String [] notBetweenNames =
        new String [] { "NOT BETWEEN", "AND" };

    /** Ordinal of the 'value' operand. */
    public static final int VALUE_OPERAND = 0;
    /** Ordinal of the 'lower' operand. */
    public static final int LOWER_OPERAND = 1;
    /** Ordinal of the 'upper' operand. */
    public static final int UPPER_OPERAND = 2;
    /** Ordinal of the 'symmetric' operand. */
    public static final int SYMFLAG_OPERAND = 3;

    //~ Instance fields -------------------------------------------------------

    /** todo: Use a wrapper 'class SqlTempCall(SqlOperator,SqlParserPos)
     * extends SqlNode' to store extra flags (neg and asymmetric) to calls to
     * BETWEEN. Then we can obsolete flag. SqlTempCall would never have any
     * SqlNodes as children, but it can have flags. */
    private final Flag flag;

    /** If true the call represents 'NOT BETWEEN'. */
    private final boolean negated;

    //~ Constructors ----------------------------------------------------------

    public SqlBetweenOperator(
        Flag flag,
        boolean negated)
    {
        super(negated ? notBetweenNames : betweenNames, SqlKind.Between, 15,
            null, null, null);
        this.flag = flag;
        this.negated = negated;
    }

    //~ Methods ---------------------------------------------------------------

    public boolean isNegated()
    {
        return negated;
    }
    
    private RelDataType [] getTypeArray(
        SqlValidator validator,
        SqlValidatorScope scope,
        SqlCall call)
    {
        RelDataType [] argTypes =
            SqlTypeUtil.collectTypes(validator, scope, call.operands);
        RelDataType [] newArgTypes = {
            argTypes[VALUE_OPERAND],
            argTypes[LOWER_OPERAND],
            argTypes[UPPER_OPERAND]
        };
        return newArgTypes;
    }

    protected RelDataType getType(
        SqlValidator validator,
        SqlValidatorScope scope,
        RelDataTypeFactory typeFactory,
        CallOperands callOperands)
    {
        CallOperands.RelDataTypesCallOperands newCallOperands =
            new CallOperands.RelDataTypesCallOperands(
                getTypeArray(validator,
                             scope,
                             (SqlCall) callOperands.getUnderlyingObject()));
        return ReturnTypeInferenceImpl.useNullableBoolean.getType(
            validator, scope, typeFactory,
            newCallOperands);
    }

    protected String getSignatureTemplate(final int operandsCount)
    {
        Util.discard(operandsCount);
        return "{1} {0} {2} AND {3}";
    }

    public String getAllowedSignatures(String name)
    {
        StringBuffer ret = new StringBuffer();
        ret.append(
            OperandsTypeChecking.typeNullableNumericNumericNumeric
                .getAllowedSignatures(this));
        ret.append(NL);
        ret.append(
            OperandsTypeChecking.typeNullableBinariesBinariesBinaries
                .getAllowedSignatures(this));
        ret.append(NL);
        ret.append(
            OperandsTypeChecking.typeNullableVarcharVarcharVarchar
                .getAllowedSignatures(this));
        return replaceAnonymous(
            ret.toString(),
            name);
    }

    protected boolean checkArgTypes(
        SqlCall call,
        SqlValidator validator,
        SqlValidatorScope scope,
        boolean throwOnFailure)
    {
        OperandsTypeChecking [] rules =
            new OperandsTypeChecking [] {
                OperandsTypeChecking.typeNullableNumeric,
                OperandsTypeChecking.typeNullableBinariesBinaries,
                OperandsTypeChecking.typeNullableVarchar
            };
        int failCount = 0;
        for (int i = 0; i < rules.length; i++) {
            OperandsTypeChecking rule = rules[i];
            boolean ok;
            ok = rule.check(call, validator, scope,
                call.operands[VALUE_OPERAND], 0, false);
            ok = ok && rule.check(call, validator, scope,
                call.operands[LOWER_OPERAND], 0, false);
            ok = ok && rule.check(call, validator, scope,
                call.operands[UPPER_OPERAND], 0, false);
            if (!ok) {
                failCount++;
            }
        }

        if (failCount >= 3) {
            if (throwOnFailure){
                throw call.newValidationSignatureError(validator, scope);
            }
            return false;
        }
        return true;
    }

    public SqlOperator.OperandsCountDescriptor getOperandsCountDescriptor()
    {
        //exp1 [ASYMMETRIC|SYMMETRIC] BETWEEN exp4 AND exp4
        return new OperandsCountDescriptor(4);
    }

    public void unparse(
        SqlWriter writer,
        SqlNode [] operands,
        int leftPrec,
        int rightPrec)
    {
        operands[VALUE_OPERAND].unparse(writer, getLeftPrec(), 0);
        writer.print(" ");
        writer.print(getName());
        writer.print(" ");
        operands[SYMFLAG_OPERAND].unparse(writer, 0, 0);
        writer.print(" ");
        operands[LOWER_OPERAND].unparse(writer, 0, 0);
        writer.print(" AND ");
        operands[UPPER_OPERAND].unparse(writer, 0, getRightPrec());
    }

    public int reduceExpr(
        int opOrdinal,
        List list)
    {
        final SqlParserUtil.ToTreeListItem betweenNode =
            (SqlParserUtil.ToTreeListItem) list.get(opOrdinal);
        SqlOperator op = betweenNode.getOperator();
        assert op == this;

        // Break the expression up into expressions. For example, a simple
        // expression breaks down as follows:
        //
        //            opOrdinal   endExp1
        //            |           |
        //     a + b BETWEEN c + d AND e + f
        //    |_____|       |_____|   |_____|
        //     exp0          exp1      exp2
        // Create the expression between 'BETWEEN' and 'AND'.
        final SqlParserPos pos =
            ((SqlNode) list.get(opOrdinal + 1)).getParserPosition();
        SqlNode exp1 =
            SqlParserUtil.toTreeEx(list, opOrdinal + 1, 0, SqlKind.And);
        if (((opOrdinal + 2) >= list.size())
                || !(list.get(opOrdinal + 2) instanceof SqlParserUtil.ToTreeListItem)
                || (((SqlParserUtil.ToTreeListItem) list.get(opOrdinal + 2)).getOperator().getKind() != SqlKind.And)) {
            throw EigenbaseResource.instance().newBetweenWithoutAnd(
                new Integer(pos.getLineNum()),
                new Integer(pos.getColumnNum()));
        }

        // Create the expression after 'AND', but stopping if we encounter an
        // operator of lower precedence.
        //
        // For example,
        //   a BETWEEN b AND c + d OR e
        // becomes
        //   (a BETWEEN b AND c + d) OR e
        // because OR has lower precedence than BETWEEN.
        SqlNode exp2 =
            SqlParserUtil.toTreeEx(
                list, opOrdinal + 3, getRightPrec(), SqlKind.Other);

        // Create the call.
        SqlNode exp0 = (SqlNode) list.get(opOrdinal - 1);
        SqlCall newExp =
            createCall(
                new SqlNode [] {
                    exp0, exp1, exp2, SqlLiteral.createSymbol(flag, null) },
                betweenNode.getPos());

        // Replace all of the matched nodes with the single reduced node.
        SqlParserUtil.replaceSublist(list, opOrdinal - 1, opOrdinal + 4, newExp);

        // Return the ordinal of the new current node.
        return opOrdinal - 1;
    }

    public void test(SqlTester tester)
    {
        if (negated) {
            SqlOperatorTests.testNotBetween(tester);
        } else {
            SqlOperatorTests.testBetween(tester);
        }
    }

    //~ Inner Classes ---------------------------------------------------------

    /**
     * Defines the "SYMMETRIC" and "ASYMMETRIC" keywords.
     */
    public static class Flag extends EnumeratedValues.BasicValue
    {
        public static final int Asymmetric_ordinal = 0;
        public static final Flag Asymmetric = new Flag("Asymmetric", Asymmetric_ordinal);
        public static final int Symmetric_ordinal = 1;
        public static final Flag Symmetric = new Flag("Symmetric", Symmetric_ordinal);
        public static final EnumeratedValues enumeration =
            new EnumeratedValues(new Flag[] {Asymmetric, Symmetric});

        private Flag(String name, int ordinal)
        {
            super(name, ordinal, null);
        }
    }
}


// End SqlBetweenOperator.java
