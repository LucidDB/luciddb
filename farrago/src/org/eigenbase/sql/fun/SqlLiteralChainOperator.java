package org.eigenbase.sql.fun;

import org.eigenbase.sql.*;
import org.eigenbase.sql.test.SqlTester;
import org.eigenbase.sql.parser.ParserPosition;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.sql.type.UnknownParamInference;
import org.eigenbase.sql.type.ReturnTypeInference;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.resource.EigenbaseResource;
import org.eigenbase.util.NlsString;
import org.eigenbase.util.BitString;

/**
 * Internal operator, by which the parser represents a continued string
 * literal.  The string fragments are SqlLiterals (of the same type),
 * collected as the operands of an SqlCall with this operator.  The
 * validator concatenates them into a single RexLiteral.
 * (For a chain of SqlLiteral.CharStrings, an SqlCollation is attached
 * only to the head of the chain.)
 *
 * @author Marc Berkowitz
 * @since Sep 7, 2004
 * @version $Id$
 */
public class SqlLiteralChainOperator extends SqlInternalOperator {

    SqlLiteralChainOperator() {
        super("$LitChain", SqlKind.LitChain, 40, true,
            // precedence tighter than the * and || operators
            ReturnTypeInference.useFirstArgType, UnknownParamInference.useFirstKnown, null);
    }

    // REVIEW mb 8/8/04: Can't use SqlOperator.OperandsTypeChecking here;
    // it doesn't handle variadicCountDescriptor operators well.
    public OperandsCountDescriptor getOperandsCountDescriptor()
    {
        return OperandsCountDescriptor.variadicCountDescriptor;
    }

    // all operands must be the same type
    private boolean argTypesValid(
        SqlCall call,
        SqlValidator validator,
        SqlValidator.Scope scope)
    {
        if (call.operands.length < 2) {
            return true; // nothing to compare
        }
        SqlNode operand = call.operands[0];
        RelDataType firstType = validator.deriveType(scope, operand);
        for (int i = 1; i < call.operands.length; i++) {
            operand = call.operands[i];
            RelDataType otherType =
                validator.deriveType(scope, operand);
            if (!firstType.isSameType(otherType)) {
                return false;
            }
        }
        return true;
    }

    protected void checkArgTypes(
        SqlCall call,
        SqlValidator validator,
        SqlValidator.Scope scope)
    {
        if (!argTypesValid(call, validator, scope)) {
            throw call.newValidationSignatureError(validator, scope);
        }
    }

    // Result type is the same as all the args, but its size is the
    // total size.
    // REVIEW mb 8/8/04: Possibly this can be achieved by combining
    // the strategy useFirstArgType with a new transformer.
    protected RelDataType inferType(
        SqlValidator validator,
        SqlValidator.Scope scope,
        SqlCall call)
    {
        // Here we know all the operands have the same type,
        // which has a size (precision), but not a scale.
        RelDataType rt =
            validator.getValidatedNodeType(call.operands[0]);
        SqlTypeName tname = rt.getSqlTypeName();
        assert tname.allowsPrecNoScale() : "LitChain has impossible operand type "
            + tname;
        int size = rt.getPrecision();
        for (int i = 1; i < call.operands.length; i++) {
            rt = validator.getValidatedNodeType(call.operands[i]);
            size += rt.getPrecision();
        }
        return validator.typeFactory.createSqlType(tname, size);
    }

    public String getAllowedSignatures(String opName)
    {
        return opName + "(...)";
    }

    // per the SQL std, each string fragment must be on a different line
    void validateCall(
        SqlCall call,
        SqlValidator validator)
    {
        ParserPosition pos;
        ParserPosition prevPos = call.operands[0].getParserPosition();
        for (int i = 1; i < call.operands.length;
             i++, prevPos = pos) {
            pos = call.operands[i].getParserPosition();
            if (pos.getBeginLine() <= prevPos.getBeginLine()) {
                // FIXME jvs 28-Aug-2004:  i18n
                throw EigenbaseResource.instance().newValidationError("String literal continued on same line, at "
                    + pos);
            }
        }
    }

    public void unparse(
        SqlWriter writer,
        SqlNode [] rands,
        int leftPrec,
        int rightPrec)
    {
        SqlCollation collation = null;
        for (int i = 0; i < rands.length; i++) {
            if (i > 0) {
                writer.print(" ");
            }
            SqlLiteral rand = (SqlLiteral) rands[i];
            if (rand instanceof SqlLiteral.CharString) {
                NlsString nls =
                    ((SqlLiteral.CharString) rand).getNlsString();
                if (i == 0) {
                    collation = nls.getCollation();
                    writer.print(nls.asSql(true, false)); // print with prefix
                } else {
                    writer.print(nls.asSql(false, false)); // print without prefix
                }
            } else if (i == 0) {
                // print with prefix
                rand.unparse(writer, leftPrec, rightPrec);
            } else {
                // print without prefix
                writer.print("'");
                if (rand.getTypeName() == SqlTypeName.Binary) {
                    BitString bs = (BitString) rand.getValue();
                    writer.print(bs.toHexString());
                } else {
                    writer.print(rand.toValue());
                }
                writer.print("'");
            }
        }
        if (collation != null) {
            writer.print(" ");
            writer.print(collation.toString());
        }
    }

    // test evaluated literals
    public void test(SqlTester tester)
    {
        tester.checkString("'buttered'\n' toast'", "buttered toast");
        tester.checkString("'corned'\n' beef'\n' on'\n' rye'",
            "corned beef on rye");
        tester.checkString("_latin1'Spaghetti'\n' all''Amatriciana'",
            "Spaghetti all'Amatriciana");
        tester.checkBoolean("B'0101'\n'0011' = B'01010011'",
            Boolean.TRUE);
        tester.checkBoolean("x'1234'\n'abcd' = x'1234abcd'",
            Boolean.TRUE);
    }
}
