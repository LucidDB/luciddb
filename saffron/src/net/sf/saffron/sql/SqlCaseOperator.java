/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2004 Disruptive Technologies, Inc.
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

import net.sf.saffron.core.SaffronType;
import net.sf.saffron.core.SaffronTypeFactory;
import net.sf.saffron.util.Util;
import net.sf.saffron.sql.fun.SqlStdOperatorTable;
import net.sf.saffron.resource.SaffronResource;

import java.util.List;
import java.util.ArrayList;

/**
 * An operator describing a <code>CASE</code>, <code>NULLIF</code> or
 * <code>COALESCE</code> expression. All of these forms are normalized at parse
 * time to a to a simple <code>CASE</code> statement like this:
 *
 * <blockquote><code><pre>CASE
 *   WHEN &lt;when expression_0&gt; THEN &lt;then expression_0&gt;
 *   WHEN &lt;when expression_1&gt; THEN &lt;then expression_1&gt;
 *   ...
 *   WHEN &lt;when expression_N&gt; THEN &ltthen expression_N&gt;
 *   ELSE &lt;else expression&gt;
 * END</pre></code></blockquote>
 *
 * The switched form of the <code>CASE</code> statement is normalized to the
 * simple form by inserting calls to the <code>=</code> operator. For example,
 *
 * <blockquote<code><pre>CASE x + y
 *   WHEN 1 THEN 'fee'
 *   WHEN 2 THEN 'fie'
 *   ELSE 'foe'
 * END</pre></code></blockquote>
 *
 * becomes
 *
 * <blockquote><code><pre>CASE
 * WHEN Equals(x + y, 1) THEN 'fee'
 * WHEN Equals(x + y, 2) THEN 'fie'
 * ELSE 'foe'
 * END</pre></code></blockquote>
 *
 * <p>REVIEW jhyde 2004/3/19 Does <code>Equals</code> handle NULL semantics
 * correctly?</p>
 *
 * <p><code>COALESCE(x, y, z)</code> becomes
 *
 * <blockquote><code><pre>CASE
 * WHEN x IS NOT NULL THEN x
 * WHEN y IS NOT NULL THEN y
 * ELSE z
 * END</pre></code></blockquote></p>
 *
 * <p><code>NULLIF(x, -1)</code> becomes
 *
 * <blockquote><code><pre>CASE
 * WHEN x = -1 THEN NULL
 * ELSE x
 * END</pre></code></blockquote></p>
 *
 * <p>Note that some of these normalizations cause expressions to be
 * duplicated. This may make it more difficult to write optimizer rules
 * (because the rules will have to deduce that expressions are equivalent).
 * It also requires that some part of the planning process (probably the
 * generator of the calculator program) does common sub-expression
 * elimination.</p>
 *
 * <p>REVIEW jhyde 2004/3/19. Expanding expressions at parse time has some
 * other drawbacks. It is more difficult to give meaningful validation errors:
 * given <code>COALESCE(DATE '2004-03-18', 3.5)</code>, do we issue a
 * type-checking error against a <code>CASE</code> operator? Second, I'd like
 * to use the {@link SqlNode} object model to generate SQL to send to 3rd-party
 * databases, but there's now no way to represent a call to COALESCE or NULLIF.
 * All in all, it would be better to have operators for COALESCE, NULLIF, and
 * both simple and switched forms of CASE, then translate to simple CASE when
 * building the {@link net.sf.saffron.rex.RexNode} tree.</p>

 * <p>The arguments are physically represented as follows:<ul>
 * <li>The <i>when</i> expressions are stored in a {@link SqlNodeList}
 *     whenList.</li>
 * <li>The <i>then</i> expressions are stored in a {@link SqlNodeList}
 *     thenList.</li>
 * <li>The <i>else</i> expression is stored as a regular {@link SqlNode}.</li>
 * </ul></p>
 *
 * @author Wael Chatila
 * @since Mar 14, 2004
 * @version $Id$
 **/

public abstract class SqlCaseOperator extends SqlOperator
{
    //~ Constructors ----------------------------------------------------------

    public SqlCaseOperator()
    {
        super("CASE",SqlKind.Case,1,true, SqlOperatorTable.useBiggest,
                SqlOperatorTable.useReturnForParam, null);
    }

    //~ Methods ---------------------------------------------------------------

    public SaffronType getType(SqlValidator validator,
            SqlValidator.Scope scope, SqlCall call) {
        SqlCase caseCall = (SqlCase) call;
        List whenList = caseCall.getWhenOperands();
        List thenList = caseCall.getThenOperands();
        List nullList = new ArrayList();
        SaffronType[] argTypes = new SaffronType[whenList.size()*2+1];
        assert(whenList.size()==thenList.size());

        //checking that search conditions are ok...
        for (int i = 0; i < whenList.size(); i++) {
            SqlNode node = (SqlNode) whenList.get(i);
            Util.pre(node instanceof SqlCall,"node!=SqlCall");
            SqlCall searchCall = (SqlCall) node;
            //should throw validation error if something wrong...
            argTypes[i*2]= searchCall.operator.getType(validator,scope,searchCall);
        }

        boolean foundNotNull = false;
        for (int i = 0; i < thenList.size(); i++) {
            SqlNode node = (SqlNode) thenList.get(i);
            if (!SqlLiteral.isNullLiteral(node)) {
                foundNotNull = true;
            } else {
                nullList.add(node);
            }
            argTypes[i*2+1]= validator.deriveType(scope, node);
        }

        SqlNode elseClause = caseCall.getElseOperand();
        if (!SqlLiteral.isNullLiteral(elseClause)) {
            foundNotNull=true;
        } else {
            nullList.add(elseClause);
        }

        if (!foundNotNull) {
            // according to the sql standard we can not have all of the THEN
            // statements and the ELSE returning null
            throw validator.newValidationError("ELSE clause or at least one THEN clause must be non-NULL");
        }


        argTypes[argTypes.length-1] = validator.deriveType(scope, elseClause);
        SaffronType ret = this.getType(validator.typeFactory,argTypes);
        for (int i = 0; i < nullList.size(); i++) {
            SqlNode node = (SqlNode) nullList.get(i);
            validator.setValidatedNodeType(node, ret);
        }
        return ret;
    }

    public SaffronType getType(SaffronTypeFactory typeFactory,
            SaffronType[] argTypes) {
        assert (argTypes.length % 2) == 1 :
                "odd number of arguments expected: " + argTypes.length;
        assert argTypes.length > 1 : argTypes.length;
        SaffronType[] thenTypes = new SaffronType[(argTypes.length-1)/2+1];
        for (int i=0,j=1; j < (argTypes.length-1); i++,j+=2) {
            thenTypes[i] = argTypes[j];
        }

        thenTypes[thenTypes.length-1] = argTypes[argTypes.length-1];
        SaffronType ret = super.getType(typeFactory, thenTypes);
        if (null == ret) {
            throw SaffronResource.instance().newValidationError(
                    "Illegal mixing of types");
        }
        return ret;
    }

    public int getSyntax()
    {
        return Syntax.Special;
    }

    public SqlCall createCall(SqlNode [] operands)
    {
        return new SqlCase(this,operands);
    }

    public SqlCase createCall(
        SqlNode caseIdentifier,
        SqlNodeList whenList,
        SqlNodeList thenList,
        SqlNode elseClause)
    {
        SqlStdOperatorTable stdOps = SqlOperatorTable.std();
        if (null != caseIdentifier) {
            List list = whenList.getList();
            for (int i = 0; i < list.size(); i++) {
                SqlNode e = (SqlNode) list.get(i);
                list.set(i, stdOps.equalsOperator.createCall(caseIdentifier, e));
            }
        }

        if (null==elseClause) {
            elseClause = SqlLiteral.createNull();
        }

        return (SqlCase) createCall(
            new SqlNode [] {
                whenList,thenList,elseClause
            });
    }

    public void unparse(
        SqlWriter writer,
        SqlNode [] operands,
        int leftPrec,
        int rightPrec)
    {
        throw Util.needToImplement("need to implement");
    }

    protected SaffronType inferType(SqlValidator validator,
            SqlValidator.Scope scope, SqlCall call) {
        return super.inferType(validator, scope, call);
    }
}


// End SqlCaseOperator.java
