/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 LucidEra, Inc.
// Copyright (C) 2005-2005 The Eigenbase Project
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
package com.lucidera.lurql;

import java.io.*;

import java.util.*;

import org.eigenbase.util.*;


/**
 * LurqlFilter represents a filter condition in a LURQL query. Currently the
 * only filters supported are of the form
 *
 * <ul>
 * <li><code>ATTRIBUTE = 'VALUE'</code>
 * <li><code>ATTRIBUTE = ?scalar-param</code>
 * <li><code>ATTRIBUTE IN ('VALUE1', 'VALUE2', ...)</code>
 * <li><code>ATTRIBUTE IN ?set-param</code>
 * <li><code>ATTRIBUTE IN [SQL-QUERY]</code>
 * <li><code>EXISTS (path-spec)</code>
 * </ul>
 *
 * TODO jvs 6-July-2006: refactor into LurqlExistsFilter, LurqlComparisonFilter
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class LurqlFilter
    extends LurqlQueryNode
{

    //~ Static fields/initializers ---------------------------------------------

    public static final LurqlFilter [] EMPTY_ARRAY = new LurqlFilter[0];

    //~ Instance fields --------------------------------------------------------

    private final String attributeName;

    private final Set<Object> values;

    private final String sqlQuery;

    private final LurqlDynamicParam setParam;

    private final LurqlExists exists;

    private boolean hasDynamicParams;

    //~ Constructors -----------------------------------------------------------

    public LurqlFilter(String attributeName, Set<Object> values)
    {
        this.attributeName = attributeName;
        this.values = Collections.unmodifiableSet(values);
        this.sqlQuery = null;
        this.setParam = null;
        this.exists = null;
        for (Object obj : values) {
            if (obj instanceof LurqlDynamicParam) {
                hasDynamicParams = true;
                break;
            }
        }
    }

    public LurqlFilter(String attributeName, String sqlQuery)
    {
        this.attributeName = attributeName;
        this.values = null;
        this.setParam = null;
        this.sqlQuery = sqlQuery;
        this.exists = null;
    }

    public LurqlFilter(String attributeName, LurqlDynamicParam param)
    {
        this.attributeName = attributeName;
        this.values = null;
        this.sqlQuery = null;
        this.setParam = param;
        hasDynamicParams = true;
        this.exists = null;
    }

    public LurqlFilter(LurqlExists exists)
    {
        this.attributeName = null;
        this.values = null;
        this.sqlQuery = null;
        this.setParam = null;
        this.exists = exists;
    }

    //~ Methods ----------------------------------------------------------------

    public LurqlExists getExists()
    {
        return exists;
    }

    public String getAttributeName()
    {
        return attributeName;
    }

    public String getSqlQuery()
    {
        return sqlQuery;
    }

    public Set getValues()
    {
        return values;
    }

    public boolean isMofId()
    {
        return attributeName.equals("mofId");
    }

    public boolean hasDynamicParams()
    {
        return hasDynamicParams;
    }

    public LurqlDynamicParam getSetParam()
    {
        return setParam;
    }

    // implement LurqlQueryNode
    public void unparse(PrintWriter pw)
    {
        if (exists != null) {
            exists.unparse(pw);
            return;
        }

        StackWriter.printSqlIdentifier(pw, attributeName);
        if (sqlQuery == null) {
            if (values == null) {
                pw.print(" in ");
                setParam.unparse(pw);
                return;
            }
            Iterator<Object> iter = values.iterator();
            if (values.size() == 1) {
                pw.print(" = ");
                unparseValue(
                    pw,
                    iter.next());
            } else {
                pw.print(" in (");
                while (iter.hasNext()) {
                    Object obj = iter.next();
                    unparseValue(pw, obj);
                    if (iter.hasNext()) {
                        pw.print(", ");
                    }
                }
                pw.print(")");
            }
        } else {
            pw.print(" in [");
            pw.write(StackWriter.INDENT);
            pw.println();
            pw.println(sqlQuery);
            pw.write(StackWriter.OUTDENT);
            pw.print("]");
        }
    }

    private void unparseValue(PrintWriter pw, Object value)
    {
        if (value instanceof LurqlDynamicParam) {
            ((LurqlDynamicParam) value).unparse(pw);
        } else {
            StackWriter.printSqlStringLiteral(
                pw,
                value.toString());
        }
    }
}

// End LurqlFilter.java
