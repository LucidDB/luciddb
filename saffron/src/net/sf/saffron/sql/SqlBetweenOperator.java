/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2004-2004 Disruptive Technologies, Inc.
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
*/package net.sf.saffron.sql;

import net.sf.saffron.sql.test.SqlTester;
import net.sf.saffron.sql.type.SqlTypeName;
import net.sf.saffron.sql.parser.ParserPosition;
import net.sf.saffron.core.SaffronType;
import net.sf.saffron.core.SaffronTypeFactory;
import net.sf.saffron.util.EnumeratedValues;

import java.util.List;
import java.util.ArrayList;

/**
 * Defines the between operator.<br>
 * Syntax:<br>
 * <code>X [NOT] BETWEEN [ASSYMETRIC | SYMMETRIC] Y AND Z</code><br>
 * if the assymetric/symmeteric keywords are left out ASSYMETRIC is default
 *
 * @author Wael Chatila
 * @since Jun 9, 2004
 * @version $Id$
 */
public abstract class SqlBetweenOperator extends SqlSpecialOperator {
    public SqlBetweenOperator(String name, SqlKind kind) {
        super(name, kind, 15, true,
                null,
                null,
                null);
    }

    private SaffronType[] getTypeArray(SqlValidator validator,
            SqlValidator.Scope scope, SqlCall call) {
        SaffronType[] argTypes = collectTypes(validator, scope, call.operands);
        SaffronType[] newArgTypes =
                new SaffronType[]{argTypes[0], argTypes[2], argTypes[3]};
         return newArgTypes;
    }

    protected SaffronType inferType(SqlValidator validator,
            SqlValidator.Scope scope, SqlCall call) {
         return SqlOperatorTable.useNullableBoolean.getType(
                validator.typeFactory, getTypeArray(validator, scope, call));
    }

    protected String getSignatureTemplate() {
        return "{1} {0} {2} AND {3}";
    }

    public String getAllowedSignatures(String name) {
        StringBuffer ret = new StringBuffer();
        ret.append(SqlOperatorTable.typeNullableNumericNumericNumeric.
                getAllowedSignatures(this));
        ret.append(NL);
        ret.append(SqlOperatorTable.typeNullableBinariesBinariesBinaries.
                getAllowedSignatures(this));
        ret.append(NL);
        ret.append(SqlOperatorTable.typeNullableVarcharVarcharVarchar.
                getAllowedSignatures(this));
        return replaceAnonymous(ret.toString(), name);
    }

    protected boolean checkArgTypesNoThrow(SqlCall call, SqlValidator validator, SqlValidator.Scope scope) {
        return super.checkArgTypesNoThrow(call, validator, scope);
    }

    protected void checkArgTypes(SqlCall call, SqlValidator validator,
            SqlValidator.Scope scope) {
        SqlOperator.AllowedArgInference[] rules =
                new SqlOperator.AllowedArgInference[]{
                    SqlOperatorTable.typeNullableNumeric,
                    SqlOperatorTable.typeNullableBinariesBinaries,
                    SqlOperatorTable.typeNullableVarchar
                };
        int nbrOfFails=0;
        for (int i = 0; i < rules.length; i++) {
            SqlOperator.AllowedArgInference rule = rules[i];
            boolean ok;
            ok = rule.check(call,validator,scope,call.operands[0],0);
            ok = ok && rule.check(call,validator,scope,call.operands[2],0);
            ok = ok && rule.check(call,validator,scope,call.operands[3],0);
            if (!ok) {
                nbrOfFails++;
            }
        }

        if (nbrOfFails>=3) {
            throw validator.newValidationError(call.getValidationSignatureErrorString(validator, scope));
        }
    }

    public int getNumOfOperands(int desiredCount) {
        return 4;
    }

    public void unparse(
            SqlWriter writer,
            SqlNode[] operands,
            int leftPrec,
            int rightPrec) {
        operands[0].unparse(writer, this.leftPrec, this.rightPrec);
        writer.print(" "+name);
        if (((SqlBetweenOperator.Flag) operands[1]).isAsymmetric) {
            writer.print(" ASYMMETRIC ");
        } else {
            writer.print(" SYMMETRIC ");
        }
        operands[2].unparse(writer, this.leftPrec, this.rightPrec);
        writer.print(" AND ");
        operands[3].unparse(writer, this.leftPrec, this.rightPrec);
    }

    public static class Flag extends SqlSymbol {
        public final boolean isAsymmetric;

        private Flag(String name, boolean isAsymmetric, ParserPosition parserPosition) {
            super(name,parserPosition);
            this.isAsymmetric = isAsymmetric;
        }

        public static final SqlSymbol createAsymmetric(ParserPosition parserPosition)
        {
             return  new Flag("Assymetric", true, parserPosition);
        }
        public static final SqlSymbol createSymmetric(ParserPosition parserPosition)
        {
             return  new Flag("Symmetric",  false, parserPosition);
        }

    }
}

// End SqlBetweenOperator.java

