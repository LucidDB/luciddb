/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// You must accept the terms in LICENSE.html to use this software.
*/

package openjava.ptree;

import openjava.mop.Environment;
import openjava.mop.OJClass;
import openjava.tools.parser.ParserConstants;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.reltype.RelDataTypeFactoryImpl;
import org.eigenbase.util.Util;
import org.eigenbase.oj.util.*;

/**
 * JoinExpression is a pair of joined relational expressions appearing in the
 * from clause of a {@link QueryExpression}.
 */
public class JoinExpression extends SetExpression {
	/**
	 * Constructs a <code>JoinExpression</code>.
	 *
	 * @param joinType type of join; allowable values are {@link
	 *   ParserConstants#LEFT}, {@link ParserConstants#RIGHT}, {@link
	 *   ParserConstants#FULL}, and {@link ParserConstants#INNER}
	 *   signifies an ordinary join.
	 **/
	public JoinExpression(
			Expression left,
			Expression right,
			int joinType,
			Expression condition) {
		set(left, right, new Integer(joinType), condition);
	}

	public Expression getLeft() {
		return (Expression) elementAt(0);
	}

	public Expression getRight() {
		return (Expression) elementAt(1);
	}

	public int getJoinType() {
		return ((Integer) elementAt(2)).intValue();
	}

	public String getJoinTypeName() {
		switch (getJoinType()) {
		case ParserConstants.INNER:
			return "inner";
		case ParserConstants.LEFT:
			return "left";
		case ParserConstants.RIGHT:
			return "right";
		case ParserConstants.FULL:
			return "full";
		default:
			throw new UnsupportedOperationException();
		}
	}

	/**
	 * Returns {@link ParserConstants#INNER} if <code>s</code> is "inner",
	 * and similarly for {@link ParserConstants#LEFT}, {@link
	 * ParserConstants#RIGHT}, {@link ParserConstants#FULL}.
	 **/
	public static int getJoinTypeCode(String s) {
		if (s.equalsIgnoreCase("inner")) {
			return ParserConstants.INNER;
		} else if (s.equalsIgnoreCase("left")) {
			return ParserConstants.LEFT;
		} else if (s.equalsIgnoreCase("right")) {
			return ParserConstants.RIGHT;
		} else if (s.equalsIgnoreCase("full")) {
			return ParserConstants.FULL;
		} else if (s.equalsIgnoreCase("inner")) {
			return ParserConstants.INNER;
		} else {
			throw new UnsupportedOperationException();
		}
	}

	public Expression getCondition() {
		return (Expression) elementAt(3);
	}

	public void accept(openjava.ptree.util.ParseTreeVisitor v)
			throws ParseTreeException {
		v.visit(this);
	}

	public OJClass deriveRowType(Environment env) throws Exception {
		Expression[] expressions = flatten(this);
        final RelDataTypeFactory typeFactory = RelDataTypeFactoryImpl.threadInstance();
        RelDataType[] types = new RelDataType[expressions.length];
		for (int i = 0; i < expressions.length; i++) {
            final OJClass ojClass = expressions[i].getRowType(env);
            types[i] = OJUtil.ojToType(typeFactory, ojClass);
		}
        final RelDataType joinType = typeFactory.createJoinType(types);
        return OJUtil.typeToOJClass(joinType);
	}
}

// End JoinExpression.java
