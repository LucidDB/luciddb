package org.eigenbase.sql.type;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.sql.*;
import org.eigenbase.rex.*;
import org.eigenbase.util.Util;

/**
 * ...
 * @author Wael Chatila
 * @since Dec 16, 2004
 * @version $Id$
 */
public interface CallOperands
{
    String        getStringLiteral(int ordinal);
    Integer           getIntLiteral(int ordinal);
    boolean       isNull(int ordinal);
    SqlOperator   getOperator();
    int           size();
    RelDataType   getType(int ordinal);
    RelDataType[] collectTypes();
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

        public Integer getIntLiteral(int ordinal)
        {
            throw Util.newInternal();
        }

        public boolean isNull(int ordinal)
        {
            throw Util.newInternal();
        }
    }

    public static class SqlCallOperands extends AbstractCallOperands {

        private final SqlValidator validator;
        private final SqlValidator.Scope scope;
        private final SqlCall call;

        public SqlCallOperands(SqlValidator validator,
                               SqlValidator.Scope scope,
                               SqlCall call)
        {
            super(validator.typeFactory, call.operator);
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

        public Integer getIntLiteral(int ordinal)
        {
            //todo: move this to SqlTypeUtil
            SqlNode node = call.operands[ordinal];
            if (node instanceof SqlLiteral) {
                SqlLiteral sqlLiteral = (SqlLiteral) node;
                return new Integer(sqlLiteral.intValue());
            } else if (node instanceof SqlCall) {
                final SqlCall c = (SqlCall) node;
                if (c.isA(SqlKind.MinusPrefix)) {
                    SqlNode child = c.operands[0];
                    if (child instanceof SqlLiteral) {
                        return new Integer(-((SqlLiteral) child).intValue());
                    }
                }
            }
            return null;
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

        public Integer getIntLiteral(int ordinal)
        {
            //todo move this to RexUtil
            RexNode node = operands[ordinal];
            if (node instanceof RexLiteral) {
                return new Integer(RexLiteral.intValue(node));
            } else if (node instanceof RexCall) {
                RexCall call = (RexCall) node;
                if (call.isA(RexKind.MinusPrefix)) {
                    RexNode child = call.operands[0];
                    if (child instanceof RexLiteral) {
                        return new Integer(-RexLiteral.intValue(child));
                    }
                }
            }
            return null;
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
