/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Tech
// You must accept the terms in LICENSE.html to use this software.
*/

package openjava.ptree;

import openjava.mop.Environment;
import openjava.mop.OJClass;
import openjava.mop.QueryEnvironment;
import openjava.mop.Toolbox;
import openjava.ptree.util.ParseTreeVisitor;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.reltype.RelDataTypeFactoryImpl;
import org.eigenbase.util.Util;
import org.eigenbase.oj.*;
import org.eigenbase.oj.util.*;

/**
 * <code>QueryExpression</code> is a Saffron extension to Java syntax
 * which represents a SQL-like relational expression.
 *
 * <p>The general form is<pre><blockquote>
 * <code>select ... group by ... from ... where ... order by ...</code>
 * </blockquote></pre>
 *
 * <p>When used as an expression, a <code>QueryExpression</code> evaluates to an
 * array, but you can also use it inside a <code>for
 * (<i>variable</i> in select ...)</code> construct.  When used in this way, it
 * is evaluated as an iterator.
 *
 * <p> A null <code>groupList</code> means don't aggregate, whereas an empty
 * <code>groupList</code> means group by the 0-tuple <code>{}</code>, which
 * produces a single row even if there are no input rows.
 **/
public class QueryExpression extends SetExpression {
    boolean boxed;

    public QueryExpression(
            ExpressionList selectList,
            boolean boxed,
            ExpressionList groupList,
            Expression from,
            Expression where,
            ExpressionList sortList) {
        if (selectList == null) {
            selectList = new ExpressionList();
            Expression[] expressions = SetExpression.flatten(from);
            for (int i = 0; i < expressions.length; i++) {
                String alias = Toolbox.getAlias(expressions[i]);
                if (alias == null) {
                    assert(expressions.length == 1) :
                        "joined expressions must have aliases";
                    assert(from == expressions[0]);
                    alias = "$" + Integer.toHexString(from.getObjectID());
                    from = new AliasedExpression(from, alias);
                }
                selectList.add(new Variable(alias));
            }
            boxed = (expressions.length != 1);
        }
        this.boxed = boxed;
        if (!boxed) {
            assert(selectList.size() == 1);
        }
        set(selectList, groupList, from, where, sortList);
    }

    public ExpressionList getSelectList() {
        return (ExpressionList) elementAt(0);
    }

    public boolean isBoxed() {
        return boxed;
    }

    public ExpressionList getGroupList() {
        return (ExpressionList) elementAt(1);
    }

    public Expression getFrom() {
        return (Expression) elementAt(2);
    }

    public Expression getWhere() {
        return (Expression) elementAt(3);
    }

    public ExpressionList getSort() {
        return (ExpressionList) elementAt(4);
    }

    public void accept(ParseTreeVisitor v)
            throws ParseTreeException {
        v.visit(this);
    }

    public OJClass deriveRowType(Environment env) throws Exception {
        final QueryEnvironment queryEnv = new QueryEnvironment(env, this);
        final ExpressionList selectList = getSelectList();
        if (!boxed) {
            assert(selectList.size() == 1);
            Expression select = selectList.get(0);
            return Toolbox.getType(queryEnv, select);
        }
        final RelDataTypeFactory typeFactory = OJUtil.threadTypeFactory();
        final RelDataType projectType = typeFactory.createStructType(
                new RelDataTypeFactory.FieldInfo() {
                    public int getFieldCount() {
                        return selectList.size();
                    }

                    public String getFieldName(int index) {
                        final Expression expression = selectList.get(index);
                        return Toolbox.getAlias(expression);
                    }

                    public RelDataType getFieldType(int index) {
                        final Expression expression = selectList.get(index);
                        final OJClass ojClass = Toolbox.getType(queryEnv,
                                expression);
                        return OJUtil.ojToType(typeFactory, ojClass);
                    }
                }
        );
        return OJUtil.typeToOJClass(projectType, typeFactory);
    }
}

// End QueryExpression.java
