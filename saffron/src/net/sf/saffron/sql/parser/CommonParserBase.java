/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
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

package net.sf.saffron.sql.parser;

import net.sf.saffron.sql.SqlOperatorTable;
import net.sf.saffron.sql.SqlFunctionTable;

/**
 * Abstract base for parsers generated from CommonParser.jj.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class CommonParserBase
{
    protected final SqlOperatorTable opTab = SqlOperatorTable.instance();
    protected final SqlFunctionTable funcTab = SqlFunctionTable.instance();

    /**
     * Accept any kind of expression in this context.
     */
    protected static final ExprContext EXPR_ACCEPT_ALL = new ExprContext();

    /**
     * Accept only query expressions in this context.
     */
    protected static final ExprContext EXPR_ACCEPT_QUERY = new ExprContext();

    /**
     * Accept only non-query expressions in this context.
     */
    protected static final ExprContext EXPR_ACCEPT_NONQUERY = new ExprContext();

    /**
     * Accept only parenthesized queries or non-query expressions
     * in this context.
     */
    protected static final ExprContext EXPR_ACCEPT_SUBQUERY = new ExprContext();
    

    protected int nDynamicParams;

    /**
     * Type-safe enum for context of acceptable expressions.
     */
    protected static class ExprContext
    {
    }
}

// End CommonParserBase.java
