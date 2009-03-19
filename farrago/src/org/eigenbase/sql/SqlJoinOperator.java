/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2002-2007 SQLstream, Inc.
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
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

import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;


/**
 * <code>SqlJoinOperator</code> describes the syntax of the SQL <code>
 * JOIN</code> operator. Since there is only one such operator, this class is
 * almost certainly a singleton.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 19, 2003
 */
public class SqlJoinOperator
    extends SqlOperator
{
    //~ Static fields/initializers ---------------------------------------------

    private static final SqlWriter.FrameType UsingFrameType =
        SqlWriter.FrameTypeEnum.create("USING");

    //~ Enums ------------------------------------------------------------------

    /**
     * Enumerates the types of condition in a join expression.
     */
    public enum ConditionType
        implements SqlLiteral.SqlSymbol
    {
        /**
         * Join clause has no condition, for example "FROM EMP, DEPT"
         */
        None,

        /**
         * Join clause has an ON condition, for example "FROM EMP JOIN DEPT ON
         * EMP.DEPTNO = DEPT.DEPTNO"
         */
        On,

        /**
         * Join clause has a USING condition, for example "FROM EMP JOIN DEPT
         * USING (DEPTNO)"
         */
        Using;
    }

    /**
     * Enumerates the types of join.
     */
    public enum JoinType
        implements SqlLiteral.SqlSymbol
    {
        /**
         * Inner join.
         */
        Inner,

        /**
         * Full outer join.
         */
        Full,

        /**
         * Cross join (also known as Cartesian product).
         */
        Cross,

        /**
         * Left outer join.
         */
        Left,

        /**
         * Right outer join.
         */
        Right,

        /**
         * Comma join: the good old-fashioned SQL <code>FROM</code> clause,
         * where table expressions are specified with commas between them, and
         * join conditions are specified in the <code>WHERE</code> clause.
         */
        Comma;
    }

    //~ Constructors -----------------------------------------------------------

    public SqlJoinOperator()
    {
        super("JOIN", SqlKind.Join, 16, true, null, null, null);
    }

    //~ Methods ----------------------------------------------------------------

    public SqlSyntax getSyntax()
    {
        return SqlSyntax.Special;
    }

    public SqlCall createCall(
        SqlLiteral functionQualifier,
        SqlParserPos pos,
        SqlNode ... operands)
    {
        assert functionQualifier == null;
        assert (operands[SqlJoin.IS_NATURAL_OPERAND] instanceof SqlLiteral);
        final SqlLiteral isNatural =
            (SqlLiteral) operands[SqlJoin.IS_NATURAL_OPERAND];
        assert (isNatural.getTypeName() == SqlTypeName.BOOLEAN);
        assert operands[SqlJoin.CONDITION_TYPE_OPERAND] != null : "precondition: operands[CONDITION_TYPE_OPERAND] != null";
        assert (operands[SqlJoin.CONDITION_TYPE_OPERAND] instanceof SqlLiteral)
            && (SqlLiteral.symbolValue(operands[SqlJoin.CONDITION_TYPE_OPERAND])
                instanceof ConditionType);
        assert operands[SqlJoin.TYPE_OPERAND] != null : "precondition: operands[TYPE_OPERAND] != null";
        assert (operands[SqlJoin.TYPE_OPERAND] instanceof SqlLiteral)
            && (SqlLiteral.symbolValue(operands[SqlJoin.TYPE_OPERAND])
                instanceof JoinType);
        return new SqlJoin(this, operands, pos);
    }

    public SqlCall createCall(
        SqlNode left,
        SqlLiteral isNatural,
        SqlLiteral joinType,
        SqlNode right,
        SqlLiteral conditionType,
        SqlNode condition,
        SqlParserPos pos)
    {
        return createCall(
            pos,
            left,
            isNatural,
            joinType,
            right,
            conditionType,
            condition);
    }

    public void unparse(
        SqlWriter writer,
        SqlNode [] operands,
        int leftPrec,
        int rightPrec)
    {
        final SqlNode left = operands[SqlJoin.LEFT_OPERAND];

        // REVIEW jvs 16-June-2006:  I commented out this and
        // corresponding endList below because it is redundant
        // with enclosing FROM frame pushed by SqlSelectOperator.
        /*
        final SqlWriter.Frame frame0 =
         writer.startList(SqlWriter.FrameTypeEnum.FromList, "", "");
         */
        left.unparse(
            writer,
            leftPrec,
            getLeftPrec());
        String natural = "";
        if (SqlLiteral.booleanValue(operands[SqlJoin.IS_NATURAL_OPERAND])) {
            natural = "NATURAL ";
        }
        final SqlJoinOperator.JoinType joinType =
            (JoinType) SqlLiteral.symbolValue(operands[SqlJoin.TYPE_OPERAND]);
        switch (joinType) {
        case Comma:
            writer.sep(",", true);
            break;
        case Cross:
            writer.sep(natural + "CROSS JOIN");
            break;
        case Full:
            writer.sep(natural + "FULL JOIN");
            break;
        case Inner:
            writer.sep(natural + "INNER JOIN");
            break;
        case Left:
            writer.sep(natural + "LEFT JOIN");
            break;
        case Right:
            writer.sep(natural + "RIGHT JOIN");
            break;
        default:
            throw Util.unexpected(joinType);
        }
        final SqlNode right = operands[SqlJoin.RIGHT_OPERAND];
        right.unparse(
            writer,
            getRightPrec(),
            rightPrec);
        final SqlNode condition = operands[SqlJoin.CONDITION_OPERAND];
        if (condition != null) {
            final SqlJoinOperator.ConditionType conditionType =
                (ConditionType) SqlLiteral.symbolValue(
                    operands[SqlJoin.CONDITION_TYPE_OPERAND]);
            switch (conditionType) {
            case Using:

                // No need for an extra pair of parens -- the condition is a
                // list. The result is something like "USING (deptno, gender)".
                writer.keyword("USING");
                assert condition instanceof SqlNodeList;
                final SqlWriter.Frame frame =
                    writer.startList(UsingFrameType, "(", ")");
                condition.unparse(writer, 0, 0);
                writer.endList(frame);
                break;
            case On:
                writer.keyword("ON");
                condition.unparse(writer, leftPrec, rightPrec);
                break;
            default:
                throw Util.unexpected(conditionType);
            }
        }
        /*
        writer.endList(frame0);
         */
    }
}

// End SqlJoinOperator.java
