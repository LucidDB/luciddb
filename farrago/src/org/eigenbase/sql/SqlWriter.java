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

import java.io.PrintWriter;
import java.util.Stack;


/**
 * A <code>SqlWriter</code> is the target to construct a SQL statement from a
 * parse tree. It deals with dialect differences; for example, Oracle quotes
 * identifiers as <code>"scott"</code>, while SQL Server quotes them as
 * <code>[scott]</code>.
 */
public class SqlWriter
{
    //~ Static fields/initializers --------------------------------------------

    static final boolean alwaysUseParentheses = true;

    //~ Instance fields -------------------------------------------------------

    private final SqlDialect dialect;
    SqlSelect query;
    Stack queryStack = new Stack();
    private PrintWriter pw;

    //~ Constructors ----------------------------------------------------------

    public SqlWriter(
        SqlDialect dialect,
        PrintWriter pw)
    {
        this.dialect = dialect;
        this.pw = pw;
    }

    //~ Methods ---------------------------------------------------------------

    public SqlDialect getDialect()
    {
        return dialect;
    }
    
    public void popQuery(SqlSelect query)
    {
        assert (query == this.query);
        SqlSelect oldQuery = (SqlSelect) queryStack.pop();
        assert (oldQuery == this.query);
        if (queryStack.isEmpty()) {
            this.query = null;
        } else {
            this.query = (SqlSelect) queryStack.lastElement();
        }
    }

    public void pushQuery(SqlSelect query)
    {
        assert (query != null);
        this.query = query;
        this.queryStack.push(query);
    }

    public void print(String s)
    {
        pw.print(s);
    }

    public void print(char x)
    {
        pw.print(x);
    }

    public void print(int x)
    {
        pw.print(x);
    }

    public void printIdentifier(String name)
    {
        print(dialect.quoteIdentifier(name));
    }

    public void println()
    {
        pw.println();
    }

    public SqlSelect getQuery()
    {
        return query;
    }
}


// End SqlWriter.java
