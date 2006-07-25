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

import java.util.*;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.util.*;
import org.eigenbase.sql.validate.*;


/**
 * A <code>SqlCall</code> is a call to an {@link SqlOperator operator}.
 * (Operators can be used to describe any syntactic construct, so in practice,
 * every non-leaf node in a SQL parse tree is a <code>SqlCall</code> of some
 * kind.)
 */
public class SqlCall
    extends SqlNode
{

    //~ Instance fields --------------------------------------------------------

    private SqlOperator operator;
    public final SqlNode [] operands;
    private final SqlLiteral functionQuantifier;
    private final boolean expanded;

    //~ Constructors -----------------------------------------------------------

    protected SqlCall(
        SqlOperator operator,
        SqlNode [] operands,
        SqlParserPos pos)
    {
        this(operator, operands, pos, false, null);
    }

    protected SqlCall(
        SqlOperator operator,
        SqlNode [] operands,
        SqlParserPos pos,
        boolean expanded,
        SqlLiteral functionQualifier)
    {
        super(pos);
        this.operator = operator;
        this.operands = operands;
        this.expanded = expanded;
        this.functionQuantifier = functionQualifier;
    }

    //~ Methods ----------------------------------------------------------------

    public boolean isA(SqlKind kind)
    {
        return operator.getKind().isA(kind);
    }

    public SqlKind getKind()
    {
        return operator.getKind();
    }

    /**
     * Whether this call was created by expanding a parentheses-free call to
     * what was syntactically an identifier.
     */
    public boolean isExpanded()
    {
        return expanded;
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

    public SqlNode clone(SqlParserPos pos)
    {
        return operator.createCall(
                SqlNode.cloneArray(operands),
                pos);
    }

    public void unparse(
        SqlWriter writer,
        int leftPrec,
        int rightPrec)
    {
        if ((leftPrec > operator.getLeftPrec())
            || ((operator.getRightPrec() <= rightPrec) && (rightPrec != 0))
            || (writer.isAlwaysUseParentheses() && isA(SqlKind.Expression))) {
            final SqlWriter.Frame frame = writer.startList("(", ")");
            operator.unparse(writer, operands, 0, 0);
            writer.endList(frame);
        } else {
            if (functionQuantifier != null) {
                // REVIEW jvs 24-July-2006:  This is currently the only
                // way to get the quantifier through to the unparse
                SqlUtil.unparseFunctionSyntax(
                    operator,
                    writer,
                    operands,
                    true,
                    functionQuantifier);
            } else {
                operator.unparse(writer, operands, leftPrec, rightPrec);
            }
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

    public void findValidOptions(
        SqlValidator validator,
        SqlValidatorScope scope,
        SqlParserPos pos,
        List<SqlMoniker> hintList)
    {
        final SqlNode [] operands = getOperands();
        for (int i = 0; i < operands.length; i++) {
            if (operands[i] instanceof SqlIdentifier) {
                SqlIdentifier id = (SqlIdentifier) operands[i];
                String posstring = id.getParserPosition().toString();
                if (posstring.equals(pos.toString())) {
                    id.findValidOptions(validator, scope, hintList);
                    return;
                }
            }
        }
        // no valid options
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
        List<String> signatureList = new ArrayList<String>();
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
     * Tests whether operator name matches supplied value.
     *
     * @param name Test string
     *
     * @return whether operator name matches parameter
     */
    public boolean isName(String name)
    {
        return operator.isName(name);
    }

    /**
     * Test to see if it is the function COUNT(*)
     *
     * @return boolean true if function call to COUNT(*)
     */
    public boolean isCountStar()
    {
        if (operator.isName("COUNT") && (operands.length == 1)) {
            final SqlNode parm = operands[0];
            if (parm instanceof SqlIdentifier) {
                SqlIdentifier id = (SqlIdentifier) parm;
                if (id.isStar() && (id.names.length == 1)) {
                    return true;
                }
            }
        }

        return false;
    }

    public SqlLiteral getFunctionQuantifier()
    {
        return functionQuantifier;
    }
}

// End SqlCall.java
