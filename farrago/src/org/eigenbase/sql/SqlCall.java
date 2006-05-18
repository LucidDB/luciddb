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
import org.eigenbase.sql.parser.SqlParserPos;
import org.eigenbase.sql.util.SqlVisitor;
import org.eigenbase.sql.validate.SqlMoniker;
import org.eigenbase.sql.validate.SqlValidator;
import org.eigenbase.sql.validate.SqlValidatorScope;
import org.eigenbase.util.Util;

import java.util.ArrayList;

/**
 * A <code>SqlCall</code> is a call to an {@link SqlOperator operator}.
 * (Operators can be used to describe any syntactic construct, so in
 * practice, every non-leaf node in a SQL parse tree is a
 * <code>SqlCall</code> of some kind.)
 */
public class SqlCall extends SqlNode
{
    //~ Instance fields -------------------------------------------------------

    private SqlOperator operator;
    public final SqlNode [] operands;
    public SqlLiteral functionQuantifier;

    //~ Constructors ----------------------------------------------------------

    protected SqlCall(
        SqlOperator operator,
        SqlNode [] operands,
        SqlParserPos pos)
    {
        super(pos);
        this.operator = operator;
        this.operands = operands;
        this.functionQuantifier = null;
    }

    //~ Methods ---------------------------------------------------------------

    public boolean isA(SqlKind kind)
    {
        return operator.getKind().isA(kind);
    }

    public SqlKind getKind()
    {
        return operator.getKind();
    }

    // REVIEW jvs 10-Sept-2003:  I added this to allow for some rewrite by
    // SqlValidator.  Is mutability OK?
    public void setOperand(
        int i,
        SqlNode operand)
    {
        operands[i] = operand;
    }

    public void setOperator(SqlOperator operator)
    {
        this.operator = operator;
    }

    public SqlOperator getOperator()
    {
        return operator;
    }

    public SqlNode [] getOperands()
    {
        return operands;
    }

    public Object clone()
    {
        return operator.createCall(
            SqlNode.cloneArray(operands),
            getParserPosition());
    }

    public void unparse(
        SqlWriter writer,
        int leftPrec,
        int rightPrec)
    {
        if (leftPrec > operator.getLeftPrec() ||
            (operator.getRightPrec() <= rightPrec && rightPrec != 0) ||
            (writer.isAlwaysUseParentheses() && isA(SqlKind.Expression))) {
            final SqlWriter.Frame frame = writer.startList("(", ")");
            operator.unparse(writer, operands, 0, 0);
            writer.endList(frame);
        } else {
            operator.unparse(writer, operands, leftPrec, rightPrec);
        }
    }

    /**
     * Validates this call.
     *
     * <p>The default implementation delegates the validation to the operator's
     * {@link SqlOperator#validateCall}. Derived classes may override (as do,
     * for example {@link SqlSelect} and {@link SqlUpdate}).
     */
    public void validate(SqlValidator validator, SqlValidatorScope scope)
    {
        validator.validateCall(this, scope);
    }

    /**
     * Find out all the valid alternatives for the operand of this node's
     * operator that matches the parse position indicated by pos
     *
     * @param validator Validator
     * @param scope Validation scope
     * @param pos SqlParserPos indicating the cursor position at which
     * competion hints are requested for
     * @return a {@link SqlMoniker} array of valid options
     */
    public SqlMoniker[] findValidOptions(
        SqlValidator validator,
        SqlValidatorScope scope,
        SqlParserPos pos)
    {
        final SqlNode[] operands = getOperands();
        for (int i = 0; i < operands.length; i++) {
            if (operands[i] instanceof SqlIdentifier) {
                SqlIdentifier id = (SqlIdentifier) operands[i];
                String posstring = id.getParserPosition().toString();
                if (posstring.equals(pos.toString())) {
                    return id.findValidOptions(validator, scope);
                }
            }
        }
        return Util.emptySqlMonikerArray;
    }

    public <R> R accept(SqlVisitor<R> visitor)
    {
        return visitor.visit(this);
    }

    public boolean equalsDeep(SqlNode node, boolean fail)
    {
        if (!(node instanceof SqlCall)) {
            assert !fail : this + "!=" + node;
            return false;
        }
        SqlCall that = (SqlCall) node;
        // Compare operators by name, not identity, because they may not
        // have been resolved yet.
        if (!this.operator.getName().equals(that.operator.getName())) {
            assert !fail : this + "!=" + node;
            return false;
        }
        if (this.operands.length != that.operands.length) {
            assert !fail : this + "!=" + node;
            return false;
        }
        for (int i = 0; i < this.operands.length; i++) {
            if (!SqlNode.equalDeep(this.operands[i], that.operands[i], fail)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns a string describing the actual argument types of a call, e.g.
     * "SUBSTR(VARCHAR(12), NUMBER(3,2), INTEGER)".
     */
    protected String getCallSignature(
        SqlValidator validator,
        SqlValidatorScope scope)
    {

        ArrayList signatureList = new ArrayList();
        for (int i = 0; i < operands.length; i++) {
            final SqlNode operand = operands[i];
            final RelDataType argType = validator.deriveType(scope, operand);
            if (null == argType) {
                continue;
            }
            signatureList.add(argType.toString());
        }
        return SqlUtil.getOperatorSignature(operator, signatureList);

    }

    public boolean isMonotonic(SqlValidatorScope scope)
    {
        // Delegate to operator.
        return operator.isMonotonic(this, scope);
    }

    /**
     * Test operator name against supplied value
     * @param name Test string
     * @return true if operator name matches parameter
     */
    public boolean isName(String name) {
        return operator.isName(name);
    }

    /**
     * Test to see if it is the function COUNT(*)
     *
     * @return boolean true if function call to COUNT(*)
     */
    public boolean isCountStar() {
        if (operator.isName("COUNT") && operands.length == 1) {
            final SqlNode parm = operands[0];
            if (parm instanceof SqlIdentifier) {
                SqlIdentifier id = (SqlIdentifier) parm;
                if (id.isStar() && id.names.length == 1) {
                    return true;
                }
            }
        }

        return false;
    }

    public void setFunctionQuantifier(SqlLiteral quantifier)
    {
        functionQuantifier = quantifier;
    }

    public SqlLiteral getFunctionQuantifier()
    {
        return functionQuantifier;
    }
}


// End SqlCall.java
