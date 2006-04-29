/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
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
package net.sf.farrago.ddl.gen;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

import net.sf.farrago.cwm.core.CwmClassifier;
import net.sf.farrago.cwm.core.CwmModelElement;
import net.sf.farrago.fem.med.FemStoredColumn;
import net.sf.farrago.fem.sql2003.FemKeyComponent;
import net.sf.farrago.fem.sql2003.FemPrimaryKeyConstraint;

import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.util.ReflectUtil;


/**
 * Base class for DDL generators which use the visitor pattern to
 * generate DDL given a catalog object.
 *
 * Escape rules:
 *
 * 1. In a SET SCHEMA command, apostrophes (') and quotes (")
 * enclose the schema name, like this:  '"Foo"'.  In this context,
 * apostrophes and quotes must be escaped.
 *
 * 2. CREATE and DROP commands use quotes (") to enclose the object name.
 * Only quotes are escaped.
 *
 * @author Jason Ouellette
 * @version $Id$
 **/
public abstract class DdlGenerator
{
    //~ Static fields/initializers --------------------------------------------

    protected static String VALUE_NULL = "NULL";
    protected static String NL = System.getProperty("line.separator");

    //~ Methods ---------------------------------------------------------------

    public void generateSetSchema(GeneratedDdlStmt stmt, String schemaName) {
        if (schemaName != null) {
            StringBuilder sb = new StringBuilder();
            sb.append("SET SCHEMA ");
            sb.append(DdlGenerator.apostropheAndQuote(schemaName));
            stmt.addStmt(sb.toString());
        }
    }
    
    public void generateCreate(CwmModelElement e, GeneratedDdlStmt stmt) {
        generate("create", e, stmt);
    }

    public void generateDrop(CwmModelElement e, GeneratedDdlStmt stmt) {
        generate("drop", e, stmt);
    }

    private void generate(String method, CwmModelElement e, GeneratedDdlStmt stmt) {
        Method m = ReflectUtil.lookupVisitMethod(this.getClass(),
                e.getClass(),
                method, Arrays.asList(new Class[] { GeneratedDdlStmt.class }));
        if (m != null) {
            try {
                m.invoke(this, new Object[] { e, stmt });
            } catch (Throwable t) {
                //TODO: handle
                t.printStackTrace();
            }
        }
    }

    protected static String apostropheAndQuote(String str)
    {
        return "'\"" + escapeApostrophesAndQuotes(str) + "\"'";
    }

    /**
     * Escape strings for SQL.  Converts quotes (") into double
     * quotes ("") and apostrophes (') into double apostrophes ('').
     *
     * @param str String to escape
     * @return escapes version of <code>str</code>
     * @see com.sqlstream.plugin.impl.DdlGenerator.
     */
    protected static String escapeApostrophesAndQuotes(String str)
    {
        StringBuffer buf = new StringBuffer(str);
        for (int i = 0; i < buf.length(); i++) {
            if (buf.charAt(i) == '\"') {
                buf.insert(i, '\"');
                i++;
            } else if (buf.charAt(i) == '\'') {
                buf.insert(i, '\'');
                i++;
            }
        }
        return buf.toString();
    }

    /**
     * Escape strings for SQL.  Converts quotes (") into double
     * quotes ("").
     *
     * @param str String to escape
     * @return escapes version of <code>str</code>
     * @see com.sqlstream.plugin.impl.DdlGenerator.
     */
    protected static String escapeQuotes(String str)
    {
        if (str == null) {
            return "";
        }

        StringBuffer buf = new StringBuffer(str);
        for (int i = 0; i < buf.length(); i++) {
            switch (buf.charAt(i)) {
            case '\"':
                buf.insert(i, '\"');
                i++;
                break;
            }
        }
        return buf.toString();
    }

    protected static String quote(String str)
    {
        return "\"" + escapeQuotes(str) + "\"";
    }

    protected static String literal(String str)
    {
        return "\'" + escapeQuotes(str) + "\'";
    }

    protected static SqlTypeName getSqlTypeName(CwmClassifier classifier)
    {
        //REVIEW: make this work for UDTs
        if (classifier == null) {
            return SqlTypeName.Any;
        } else {
            String typeName = classifier.getName();
            SqlTypeName stn = SqlTypeName.get(typeName);
            if (stn == null) {
                return SqlTypeName.Any;
            }
            return stn;
        }
    }

    protected static boolean hasPrimaryKeyConstraint(FemStoredColumn col)
    {
        boolean result = false;

        if (col != null) {
            Collection keyComponent = col.getKeyComponent();
            if (keyComponent != null) {
                Iterator i = keyComponent.iterator();
                while (i.hasNext()) {
                    FemKeyComponent kc = (FemKeyComponent) i.next();
                    if (kc.getKeyConstraint() instanceof FemPrimaryKeyConstraint) {
                        result = true;
                        break;
                    }
                }
            }
        }
        return result;
    }
}
