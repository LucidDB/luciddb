/*
// $Id$
// Package org.eigenbase is a class library of database components.
// Copyright (C) 2002-2004 Disruptive Tech
// Copyright (C) 2003-2004 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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

package org.eigenbase.oj.rex;

import java.util.*;

import openjava.ptree.*;

import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;


/**
 * OJRexImplementorTableImpl is a default implementation of {@link
 * OJRexImplementorTable}, containing implementors for standard operators
 * and functions.  Say that three times fast.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class OJRexImplementorTableImpl implements OJRexImplementorTable
{
    //~ Static fields/initializers --------------------------------------------

    private static OJRexImplementorTableImpl instance;

    //~ Instance fields -------------------------------------------------------

    private final Map implementorMap;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates an empty table.
     *
     *<p>
     *
     * You probably want to call the public method {@link #std} instead.
     */
    protected OJRexImplementorTableImpl()
    {
        implementorMap = new HashMap();
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * Creates a table and initializes it with implementations of all of the
     * standard SQL functions and operators.
     */
    public synchronized static OJRexImplementorTable instance()
    {
        if (instance == null) {
            instance = new OJRexImplementorTableImpl();
            instance.initStandard(SqlOperatorTable.std());
        }
        return instance;
    }

    // implement OJRexImplementorTable
    public OJRexImplementor get(SqlOperator op)
    {
        return (OJRexImplementor) implementorMap.get(op);
    }

    /**
     * Registers implementations for the standard set of functions and
     * operators.
     */
    protected void initStandard(final SqlStdOperatorTable opTab)
    {
        registerBinaryOperator(opTab.equalsOperator, BinaryExpression.EQUAL);

        registerBinaryOperator(opTab.lessThanOperator, BinaryExpression.LESS);

        registerBinaryOperator(opTab.greaterThanOperator,
            BinaryExpression.GREATER);

        registerBinaryOperator(opTab.plusOperator, BinaryExpression.PLUS);

        registerBinaryOperator(opTab.minusOperator, BinaryExpression.MINUS);

        registerBinaryOperator(opTab.multiplyOperator, BinaryExpression.TIMES);

        registerBinaryOperator(opTab.divideOperator, BinaryExpression.DIVIDE);

        registerBinaryOperator(opTab.andOperator, BinaryExpression.LOGICAL_AND);

        registerBinaryOperator(opTab.orOperator, BinaryExpression.LOGICAL_OR);

        registerOperator(
            opTab.isTrueOperator,
            new OJRexIgnoredCallImplementor());

        registerOperator(
            opTab.castFunc,
            new OJRexCastImplementor());
    }

    protected void registerOperator(
        SqlOperator op,
        OJRexImplementor implementor)
    {
        implementorMap.put(op, implementor);
    }

    protected void registerBinaryOperator(
        SqlBinaryOperator op,
        int ojBinaryExpressionOrdinal)
    {
        registerOperator(
            op,
            new OJRexBinaryExpressionImplementor(ojBinaryExpressionOrdinal));
    }
}


// End OJRexImplementorTableImpl.java
