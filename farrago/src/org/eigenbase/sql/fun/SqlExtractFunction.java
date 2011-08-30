/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
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
package org.eigenbase.sql.fun;

import java.util.ArrayList;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.sql.*;
import org.eigenbase.sql.SqlIntervalQualifier.TimeUnit;
import org.eigenbase.sql.SqlLiteral.SqlSymbol;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.validate.SqlValidator;
import org.eigenbase.sql.validate.SqlValidatorScope;
import org.eigenbase.util.*;


/**
 * The SQL <code>EXTRACT</code> operator. Extracts a specified field value from
 * a DATETIME or an INTERVAL. E.g.<br>
 * <code>EXTRACT(HOUR FROM INTERVAL '364 23:59:59')</code> returns <code>
 * 23</code>
 *
 * @author Wael Chatila
 * @version $Id$
 */
public class SqlExtractFunction
    extends SqlFunction
{
    //~ Constructors -----------------------------------------------------------

    // SQL2003, Part 2, Section 4.4.3 - extract returns a exact numeric
    // TODO: Return type should be decimal for seconds
    public SqlExtractFunction(boolean decimalForSeconds)
    {
        super(
            "EXTRACT",
            SqlKind.OTHER_FUNCTION,
            decimalForSeconds
                ? rtiNullableCustom : SqlTypeStrategies.rtiNullableBigint,
            null,
            otcCustom,
            SqlFunctionCategory.System);
    }

    //~ Methods ----------------------------------------------------------------

    public String getSignatureTemplate(int operandsCount)
    {
        Util.discard(operandsCount);
        return "{0}({1} FROM {2})";
    }

    public void unparse(
        SqlWriter writer,
        SqlNode [] operands,
        int leftPrec,
        int rightPrec)
    {
        final SqlWriter.Frame frame = writer.startFunCall(getName());
        operands[0].unparse(writer, leftPrec, rightPrec);
        writer.sep("FROM");
        operands[1].unparse(writer, leftPrec, rightPrec);
        writer.endFunCall(frame);
    }

    @Override
    public void validateCall(
        SqlCall call,
        SqlValidator validator,
        SqlValidatorScope scope,
        SqlValidatorScope operandScope)
    {
        super.validateCall(call, validator, scope, operandScope);
        SqlCallBinding binding = new SqlCallBinding(validator, scope, call);
        TimeUnit field = (TimeUnit) binding.getSymbolLiteralOperand(0);
        RelDataType type = binding.getOperandType(1);
        boolean valid = true;
        if (type.getSqlTypeName() == SqlTypeName.DATE) {
            valid = field.ordinal() <= TimeUnit.DAY.ordinal();
        } else if (type.getSqlTypeName() == SqlTypeName.TIME) {
            valid = field.ordinal() >= TimeUnit.HOUR.ordinal();
        } else if (type.getSqlTypeName() == SqlTypeName.INTERVAL_DAY_TIME) {
            valid = field.ordinal() >= TimeUnit.DAY.ordinal();
        } else if (type.getSqlTypeName() == SqlTypeName.INTERVAL_YEAR_MONTH) {
            valid = field.ordinal() <= TimeUnit.MONTH.ordinal();
        }
        if (!valid) {
            throw binding.newValidationSignatureError();
        }
    }

    private static final SqlOperandTypeChecker otcCustom =
        new CompositeOperandTypeChecker(
            CompositeOperandTypeChecker.Composition.SEQUENCE,
            SqlTypeStrategies.otcNotNullLit,
            SqlTypeStrategies.otcDatetimeOrInterval)
        {
            public String getAllowedSignatures(SqlOperator op, String opName)
            {
                StringBuilder ret = new StringBuilder();
                for (SqlTypeFamily f : new SqlTypeFamily[] {
                    SqlTypeFamily.DATETIME_INTERVAL, SqlTypeFamily.DATETIME})
                {
                    if (ret.length() > 0) {
                        ret.append(NL);
                    }
                    ArrayList<String> list = new ArrayList<String>();
                    list.add("TIMEUNIT");
                    list.add(f.toString());
                    ret.append(SqlUtil.getAliasedSignature(op, opName, list));
                }
                return ret.toString();
            }
        };

    private static final SqlReturnTypeInference rtiCustom =
        new SqlReturnTypeInference() {
            public RelDataType inferReturnType(SqlOperatorBinding opBinding)
            {
                SqlSymbol timeUnit = opBinding.getSymbolLiteralOperand(0);
                if (timeUnit == TimeUnit.SECOND) {
                    return opBinding.getTypeFactory().createSqlType(
                        SqlTypeName.DECIMAL,
                        5,
                        3);
                }
                return SqlTypeStrategies.rtiBigint.inferReturnType(opBinding);
            }
        };

    private static final SqlReturnTypeInference rtiNullableCustom =
        new SqlTypeTransformCascade(
            rtiCustom,
            SqlTypeTransforms.toNullable);
}

// End SqlExtractFunction.java
