/*
// $Id$
// Package org.eigenbase is a class library of database components.
// Copyright (C) 2002-2004 Disruptive Tech
// Copyright (C) 2003-2004 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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
import org.eigenbase.resource.EigenbaseResource;
import org.eigenbase.sql.parser.ParserPosition;
import org.eigenbase.sql.parser.ParserUtil;
import org.eigenbase.sql.test.SqlOperatorTests;
import org.eigenbase.sql.test.SqlTester;
import org.eigenbase.sql.type.OperandsTypeChecking;
import org.eigenbase.sql.type.ReturnTypeInference;
import org.eigenbase.sql.type.TypeUtil;
import org.eigenbase.util.Util;

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

    /** todo: Use a wrapper 'class SqlTempCall(SqlOperator,ParserPosition)
     * extends SqlNode' to store extra flags (neg and asymmetric) to calls to
     * BETWEEN. Then we can obsolete flag. SqlTempCall would never have any
     * SqlNodes as children, but it can have flags. */
    private final Flag flag;

    /** If true the call represents 'NOT BETWEEN'. */
    public final boolean negated;

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

    private RelDataType [] getTypeArray(
        SqlValidator validator,
        SqlValidator.Scope scope,
        SqlCall call)
    {
        RelDataType [] argTypes =
            TypeUtil.collectTypes(validator, scope, call.operands);
        RelDataType [] newArgTypes = {
            argTypes[VALUE_OPERAND],
            argTypes[LOWER_OPERAND],
            argTypes[UPPER_OPERAND]
        };
        return newArgTypes;
    }

    protected RelDataType inferType(
        SqlValidator validator,
        SqlValidator.Scope scope,
        SqlCall call)
    {
        return ReturnTypeInference.useNullableBoolean.getType(
            validator.typeFactory,
            getTypeArray(validator, scope, call));
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
        SqlValidator.Scope scope,
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
        operands[VALUE_OPERAND].unparse(writer, this.leftPrec, this.rightPrec);
        writer.print(" " + name);
        if (((SqlBetweenOperator.Flag) operands[SYMFLAG_OPERAND]).isAsymmetric) {
            writer.print(" ASYMMETRIC ");
        } else {
            writer.print(" SYMMETRIC ");
        }
        operands[LOWER_OPERAND].unparse(writer, this.leftPrec, this.rightPrec);
        writer.print(" AND ");
        operands[UPPER_OPERAND].unparse(writer, this.leftPrec, this.rightPrec);
    }

    public int reduceExpr(
        int opOrdinal,
        List list)
    {
        final ParserUtil.ToTreeListItem betweenNode =
            (ParserUtil.ToTreeListItem) list.get(opOrdinal);
        SqlOperator op = betweenNode.op;
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
        final ParserPosition pos =
            ((SqlNode) list.get(opOrdinal + 1)).getParserPosition();
        SqlNode exp1 =
            ParserUtil.toTreeEx(list, opOrdinal + 1, 0, SqlKind.And);
        if (((opOrdinal + 2) >= list.size())
                || !(list.get(opOrdinal + 2) instanceof ParserUtil.ToTreeListItem)
                || (((ParserUtil.ToTreeListItem) list.get(opOrdinal + 2)).op.kind != SqlKind.And)) {
            throw EigenbaseResource.instance().newBetweenWithoutAnd(
                new Integer(pos.getBeginLine()),
                new Integer(pos.getBeginColumn()));
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
            ParserUtil.toTreeEx(list, opOrdinal + 3, rightPrec, SqlKind.Other);

        // Create the call.
        SqlNode exp0 = (SqlNode) list.get(opOrdinal - 1);
        SqlCall newExp =
            createCall(
                new SqlNode [] { exp0, exp1, exp2, flag },
                betweenNode.pos);

        // Replace all of the matched nodes with the single reduced node.
        ParserUtil.replaceSublist(list, opOrdinal - 1, opOrdinal + 4, newExp);

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
     * TODO javadoc
     *
     * REVIEW jhyde 2004/8/11 Convert this back to an enumeration.
     *   If you need to provide a parser position, wrap it in a
     *   {@link SqlLiteral} or better, make {@link SqlSymbol} a subtype of
     *   SqlLiteral.
     */
    public static class Flag extends SqlSymbol
    {
        public final boolean isAsymmetric;

        private Flag(
            String name,
            boolean isAsymmetric,
            ParserPosition pos)
        {
            super(name, pos);
            this.isAsymmetric = isAsymmetric;
        }

        public static final Flag createAsymmetric(ParserPosition pos)
        {
            return new Flag("Asymmetric", true, pos);
        }

        public static final Flag createSymmetric(ParserPosition pos)
        {
            return new Flag("Symmetric", false, pos);
        }
    }
}


// End SqlBetweenOperator.java
