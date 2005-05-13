/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2004-2005 Disruptive Tech
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
package org.eigenbase.sql.type;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.sql.*;
import org.eigenbase.sql.validate.SqlValidatorScope;
import org.eigenbase.sql.validate.SqlValidator;
import org.eigenbase.rex.*;
import org.eigenbase.util.Util;

/**
 * A common interface into the SqlNode and RexNode worlds.
 * Used during type inference, post validation.
 *
 * @author Wael Chatila
 * @since Dec 16, 2004
 * @version $Id$
 */
public interface CallOperands
{
    /**
     * Returns the string value of a string literal
     * @param ordinal the n<i>th</i> operand of the call (zero based).
     *        Operand MUST be a string literal
     */
    String        getStringLiteral(int ordinal);

    /**
     * Returns the integer value of a numerical literal
     * @param ordinal the nth operand of the call (zero based)
     *        Operand MUST be an numerical literal
     */
    int       getIntLiteral(int ordinal);

    /**
     * Returns if node is NULL, allowing infintite number of null casts.<br>
     * <code>
     * NULL<BR>
     * CAST(NULL AS <type>)<br>
     * CAST(CAST(NULL AS <type>) AS <type>))<br>
     * ...<br>
     * </code>
     * all return true.
     * @param ordinal the nth operand of the call (zero based)
     */
    boolean       isNull(int ordinal);

    /**
     * Returns SqlOperator of the call
     */
    SqlOperator   getOperator();

    /**
     * Returns the number of operands
     */
    int           size();

    /**
     * Gets the type of node at position ordinal (zero based)
     */
    RelDataType   getType(int ordinal);


    /**
     * TODO
     */
    RelDataType[] collectTypes();

    /**
     * Returns the underlying object, accessed from all other methods in this interface.
     * This method should only be used for highly specialized code and should be avoided as far as possible.
     */
    Object        getUnderlyingObject();

    //~ Inner Classes ---------------------------------------------------------

    public abstract static class AbstractCallOperands implements CallOperands {
        protected final RelDataTypeFactory typeFactory;
        private final SqlOperator sqlOperator;

        public AbstractCallOperands(RelDataTypeFactory typeFactory,
                                    SqlOperator sqlOperator)
        {
            this.typeFactory = typeFactory;
            this.sqlOperator = sqlOperator;
        }

        public SqlOperator getOperator()
        {
            return sqlOperator;
        }

        public RelDataType[] collectTypes()
        {
            RelDataType[] ret = new RelDataType[size()];
            for (int i = 0; i < ret.length; i++) {
                ret[i] = getType(i);
            }
            return ret;
        }

        public String getStringLiteral(int ordinal)
        {
            throw Util.newInternal();
        }

        public int getIntLiteral(int ordinal)
        {
            throw Util.newInternal();
        }

        public boolean isNull(int ordinal)
        {
            throw Util.newInternal();
        }
    }

    public static class SqlCallOperands extends AbstractCallOperands
    {

        private final SqlValidator validator;
        private final SqlValidatorScope scope;
        private final SqlCall call;

        public SqlCallOperands(SqlValidator validator,
            SqlValidatorScope scope,
            SqlCall call)
        {
            super(validator.getTypeFactory(), call.operator);
            this.validator = validator;
            this.scope = scope;
            this.call = call;
        }

        public String getStringLiteral(int ordinal)
        {
            SqlLiteral sqlLiteral = (SqlLiteral) call.operands[ordinal];
            assert(SqlTypeUtil.inCharFamily(sqlLiteral.getTypeName()));
            return sqlLiteral.getStringValue();
        }

        public int getIntLiteral(int ordinal)
        {
            //todo: move this to SqlTypeUtil
            SqlNode node = call.operands[ordinal];
            if (node instanceof SqlLiteral) {
                SqlLiteral sqlLiteral = (SqlLiteral) node;
                return sqlLiteral.intValue();
            } else if (node instanceof SqlCall) {
                final SqlCall c = (SqlCall) node;
                if (c.isA(SqlKind.MinusPrefix)) {
                    SqlNode child = c.operands[0];
                    if (child instanceof SqlLiteral) {
                        return -((SqlLiteral) child).intValue();
                    }
                }
            }
            throw Util.newInternal("should never come here");
        }

        public boolean isNull(int ordinal)
        {
            return SqlUtil.isNull(call.operands[ordinal]);
        }

        public int size()
        {
            return call.operands.length;
        }

        public RelDataType getType(int ordinal)
        {
            return validator.deriveType(scope, call.operands[ordinal]);
        }

        public Object getUnderlyingObject()
        {
            return call;
        }
    }

    public static class RexCallOperands extends AbstractCallOperands {
        RexNode[] operands;

        public RexCallOperands(RelDataTypeFactory typeFactory,
                               SqlOperator sqlOperator,
                               RexNode[] operands)
        {
            super(typeFactory, sqlOperator);
            this.operands = operands;
        }

        public String getStringLiteral(int ordinal)
        {
             return RexLiteral.stringValue(operands[ordinal]);
        }

        public int getIntLiteral(int ordinal)
        {
            //todo move this to RexUtil
            RexNode node = operands[ordinal];
            if (node instanceof RexLiteral) {
                return RexLiteral.intValue(node);
            } else if (node instanceof RexCall) {
                RexCall call = (RexCall) node;
                if (call.isA(RexKind.MinusPrefix)) {
                    RexNode child = call.operands[0];
                    if (child instanceof RexLiteral) {
                        return -RexLiteral.intValue(child);
                    }
                }
            }
            throw Util.newInternal("should never come here");
        }

        public boolean isNull(int ordinal)
        {
            return RexUtil.isNull(operands[ordinal]);
        }

        public int size()
        {
            return operands.length;
        }

        public RelDataType getType(int ordinal)
        {
            return operands[ordinal].getType();
        }

        public Object getUnderlyingObject()
        {
            return operands;
        }


    }

    public static class RelDataTypesCallOperands extends AbstractCallOperands {
        RelDataType[] types;

        public RelDataTypesCallOperands(RelDataType[] types)
        {
            super(null, null);
            this.types = types;
        }

        public int size()
        {
            return types.length;
        }

        public RelDataType getType(int ordinal)
        {
            return types[ordinal];
        }

        public Object getUnderlyingObject()
        {
            return types;
        }
    }
}
