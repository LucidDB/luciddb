/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// (C) Copyright 2003-2004 John V. Sichi
// You must accept the terms in LICENSE.html to use this software.
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

package net.sf.saffron.sql;

import net.sf.saffron.util.EnumeratedValues;
import net.sf.saffron.sql.test.SqlTester;
import net.sf.saffron.sql.type.SqlTypeName;


/**
 * <code>SqlJoinOperator</code> describes the syntax of the SQL
 * <code>JOIN</code> operator. Since there is only one such operator, this
 * class is almost certainly a singleton.
 *
 * @author jhyde
 * @version $Id$
 *
 * @since Mar 19, 2003
 */
public class SqlJoinOperator extends SqlOperator
{
    //~ Static fields/initializers --------------------------------------------

    public static final int LEFT_OPERAND = 0;

    /**
     * One of the following: {@link SqlLiteral#True}, {@link
     * SqlLiteral#False}.
     */
    public static final int IS_NATURAL_OPERAND = 1;

    /**
     * Value must be a {@link SqlLiteral}, one of the integer codes for {@link
     * JoinType}.
     */
    public static final int TYPE_OPERAND = 2;
    public static final int RIGHT_OPERAND = 3;

    /**
     * Value must be a {@link SqlLiteral}, one of the integer codes for {@link
     * ConditionType}.
     */
    public static final int CONDITION_TYPE_OPERAND = 4;
    public static final int CONDITION_OPERAND = 5;

    //~ Constructors ----------------------------------------------------------

    public SqlJoinOperator()
    {
        super("JOIN",SqlKind.Join,8,true, null,null, null);
    }

    //~ Methods ---------------------------------------------------------------

    public int getSyntax()
    {
        return Syntax.Special;
    }

    public SqlCall createCall(SqlNode [] operands)
    {
        assert(operands[IS_NATURAL_OPERAND] instanceof SqlLiteral);
        final SqlLiteral isNatural = (SqlLiteral) operands[IS_NATURAL_OPERAND];
        assert(isNatural._typeName == SqlTypeName.Boolean);
        assert operands[CONDITION_TYPE_OPERAND] != null :
                "precondition: operands[CONDITION_TYPE_OPERAND] != null";
        assert operands[CONDITION_TYPE_OPERAND] instanceof SqlLiteral &&
                ((SqlLiteral) operands[CONDITION_TYPE_OPERAND]).getValue()
                instanceof ConditionType;
        assert operands[TYPE_OPERAND] != null :
                "precondition: operands[TYPE_OPERAND] != null";
        assert operands[TYPE_OPERAND] instanceof SqlLiteral &&
                ((SqlLiteral) operands[TYPE_OPERAND]).getValue()
                instanceof JoinType;
        return new SqlJoin(this,operands);
    }

    public void test(SqlTester tester) {
        /* empty implementaion */
    }

    public SqlCall createCall(
        SqlNode left,
        SqlLiteral isNatural,
        SqlLiteral joinType,
        SqlNode right,
        SqlLiteral conditionType,
        SqlNode condition)
    {
        return createCall(
            new SqlNode [] {
                left,isNatural,joinType,right,conditionType,condition
            });
    }

    public void unparse(
        SqlWriter writer,
        SqlNode [] operands,
        int leftPrec,
        int rightPrec)
    {
        final SqlNode left = operands[LEFT_OPERAND];
        left.unparse(writer,leftPrec,this.leftPrec);
        writer.print(' ');
        if (SqlLiteral.booleanValue(operands[IS_NATURAL_OPERAND])) {
            writer.print("NATURAL ");
        }
        final SqlJoinOperator.JoinType joinType = (JoinType)
                ((SqlLiteral) operands[TYPE_OPERAND]).getValue();
        switch (joinType.getOrdinal()) {
        case JoinType.Comma_ORDINAL:
            writer.print(", ");
            break;
        case JoinType.Cross_ORDINAL:
            writer.print("CROSS JOIN ");
            break;
        case JoinType.Full_ORDINAL:
            writer.print("FULL JOIN ");
            break;
        case JoinType.Inner_ORDINAL:
            writer.print("INNER JOIN ");
            break;
        case JoinType.Left_ORDINAL:
            writer.print("LEFT JOIN ");
            break;
        case JoinType.Right_ORDINAL:
            writer.print("RIGHT JOIN ");
            break;
        default:
            throw joinType.unexpected();
        }
        final SqlNode right = operands[RIGHT_OPERAND];
        right.unparse(writer,this.rightPrec,rightPrec);
        final SqlNode condition = operands[CONDITION_OPERAND];
        if (condition != null) {
            final SqlJoinOperator.ConditionType conditionType = (ConditionType)
                    ((SqlLiteral) operands[CONDITION_TYPE_OPERAND]).getValue();
            switch (conditionType.getOrdinal()) {
            case ConditionType.Using_ORDINAL:
                writer.print(" USING (");
                condition.unparse(writer,leftPrec,rightPrec); // e.g. "using (deptno, gender)"
                writer.print(")");
                break;
            case ConditionType.On_ORDINAL:
                writer.print(" ON ");
                condition.unparse(writer,leftPrec,rightPrec);
                break;
            default:
                throw conditionType.unexpected();
            }
        }
    }

    //~ Inner Classes ---------------------------------------------------------

    /**
     * Enumerates the types of condition in a join expression.
     */
    public static class ConditionType extends EnumeratedValues.BasicValue
    {
        private ConditionType(String name, int ordinal) {
            super(name, ordinal, null);
        }

        public static final int None_ORDINAL = 0;
        /** Join clause has no condition, for example "FROM EMP, DEPT" */
        public static final ConditionType None = new ConditionType("None", None_ORDINAL);
        public static final int On_ORDINAL = 1;
        /** Join clause has an ON condition, for example
         * "FROM EMP JOIN DEPT ON EMP.DEPTNO = DEPT.DEPTNO" */
        public static final ConditionType On = new ConditionType("On", On_ORDINAL);
        public static final int Using_ORDINAL = 2;
        /** Join clause has a USING condition, for example
         * "FROM EMP JOIN DEPT USING (DEPTNO)" */
        public static final ConditionType Using = new ConditionType("Using", Using_ORDINAL);

        /**
         * List of all allowable {@link SqlJoinOperator.ConditionType} values.
         */
        public static final EnumeratedValues enumeration = new EnumeratedValues(
                new ConditionType [] { None,On,Using });
        /**
         * Looks up a condition type from its ordinal.
         */
        public static ConditionType get(int ordinal) {
            return (ConditionType) enumeration.getValue(ordinal);
        }
        /**
         * Looks up a condition type from its name.
         */
        public static ConditionType get(String name) {
            return (ConditionType) enumeration.getValue(name);
        }
    }

    /**
     * Enumerates the types of join.
     */
    public static class JoinType extends EnumeratedValues.BasicValue
    {
        private JoinType(String name, int ordinal) {
            super(name, ordinal, null);
        }
        public static final int Inner_ORDINAL = 0;
        /** Inner join. */
        public static final JoinType Inner = new JoinType("Inner",Inner_ORDINAL);
        public static final int Full_ORDINAL = 1;
        /** Full outer join. */
        public static final JoinType Full = new JoinType("Full",Full_ORDINAL);
        public static final int Cross_ORDINAL = 2;
        /** Cross join (also known as Cartesian product). */
        public static final JoinType Cross = new JoinType("Cross",Cross_ORDINAL);
        public static final int Left_ORDINAL = 3;
        /** Left outer join. */
        public static final JoinType Left = new JoinType("Left",Left_ORDINAL);
        public static final int Right_ORDINAL = 4;
        /** Right outer join. */
        public static final JoinType Right = new JoinType("Right",Right_ORDINAL);
        public static final int Comma_ORDINAL = 5;
        /** Comma join: the good old-fashioned SQL <code>FROM</code> clause,
         * where table expressions are specified with commas between them,
         * and join conditions are specified in the <code>WHERE</code>
         * clause. */
        public static final JoinType Comma = new JoinType("Comma",Comma_ORDINAL);
        /**
         * List of all allowable {@link SqlJoinOperator.JoinType} values.
         */
        public static final EnumeratedValues enumeration = new EnumeratedValues(
                new JoinType [] { Inner,Full,Cross,Left,Right,Comma });
        /**
         * Looks up a join type from its ordinal.
         */
        public static JoinType get(int ordinal) {
            return (JoinType) enumeration.getValue(ordinal);
        }
        /**
         * Looks up a join type from its name.
         */
        public static JoinType get(String name) {
            return (JoinType) enumeration.getValue(name);
        }
    }
}


// End SqlJoinOperator.java
