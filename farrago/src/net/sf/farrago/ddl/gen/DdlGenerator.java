/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
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

import java.lang.reflect.*;

import java.util.*;

import net.sf.farrago.cwm.core.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fem.sql2003.*;

import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;


/**
 * Base class for DDL generators which use the visitor pattern to generate DDL
 * given a catalog object. Escape rules: 1. In a SET SCHEMA command, apostrophes
 * (') and quotes (") enclose the schema name, like this: '"Foo"'. In this
 * context, apostrophes and quotes must be escaped. 2. CREATE and DROP commands
 * use quotes (") to enclose the object name. Only quotes are escaped.
 *
 * @author Jason Ouellette
 * @version $Id$
 */
public abstract class DdlGenerator
{
    //~ Static fields/initializers ---------------------------------------------

    protected static final SqlDialect sqlDialect = SqlUtil.eigenbaseDialect;
    protected static final String VALUE_NULL = "NULL";
    protected static final String NL = System.getProperty("line.separator");
    protected static final String SEP = ";" + NL + NL;

    //~ Methods ----------------------------------------------------------------

    public void generateSetSchema(GeneratedDdlStmt stmt, String schemaName)
    {
        if (schemaName != null) {
            StringBuilder sb = new StringBuilder();
            sb.append("SET SCHEMA ");
            sb.append(literal(quote(schemaName)));
            stmt.addStmt(sb.toString());
        }
    }

    public void generateCreate(CwmModelElement e, GeneratedDdlStmt stmt)
    {
        generate("create", e, stmt);
    }

    public void generateDrop(CwmModelElement e, GeneratedDdlStmt stmt)
    {
        generate("drop", e, stmt);
    }

    private void generate(
        String method,
        CwmModelElement e,
        GeneratedDdlStmt stmt)
    {
        Method m =
            ReflectUtil.lookupVisitMethod(
                this.getClass(),
                e.getClass(),
                method,
                Collections.singletonList((Class) GeneratedDdlStmt.class));
        if (m != null) {
            try {
                m.invoke(
                    this,
                    e,
                    stmt);
            } catch (InvocationTargetException e1) {
                throw Util.newInternal(e1, "while exporting '" + e + "'");
            } catch (IllegalAccessException e1) {
                throw Util.newInternal(e1, "while exporting '" + e + "'");
            } catch (RuntimeException e1) {
                throw Util.newInternal(e1, "while exporting '" + e + "'");
            }
        }
    }

    public static String quote(String str)
    {
        return sqlDialect.quoteIdentifier(str);
    }

    public static String literal(String str)
    {
        return sqlDialect.quoteStringLiteral(str);
    }

    public static String unquoteLiteral(String str)
    {
        return sqlDialect.unquoteStringLiteral(str);
    }

    protected static SqlTypeName getSqlTypeName(CwmClassifier classifier)
    {
        //REVIEW: make this work for UDTs
        if (classifier == null) {
            return SqlTypeName.ANY;
        } else {
            String typeName = classifier.getName();
            SqlTypeName stn = SqlTypeName.get(typeName);
            if (stn == null) {
                return SqlTypeName.ANY;
            }
            return stn;
        }
    }

    protected static boolean hasPrimaryKeyConstraint(FemStoredColumn col)
    {
        boolean result = false;

        if (col != null) {
            Collection<FemKeyComponent> keyComponent = col.getKeyComponent();
            if (keyComponent != null) {
                for (FemKeyComponent kc : keyComponent) {
                    if (kc.getKeyConstraint()
                        instanceof FemPrimaryKeyConstraint)
                    {
                        result = true;
                        break;
                    }
                }
            }
        }
        return result;
    }

    /**
     * Converts a set of elements to a string using this generator.
     *
     * @param exportList List of elements to export
     *
     * @return DDL script
     */
    public String getExportText(List<CwmModelElement> exportList)
    {
        StringBuilder outBuf = new StringBuilder();
        GeneratedDdlStmt stmt = new GeneratedDdlStmt();
        for (CwmModelElement elem : exportList) {
            // proceed if a catalog object has an ddlgen error
            try {
                stmt.clear();
                generateCreate(elem, stmt);
                if (!stmt.isTopLevel()) {
                    continue;
                }
                final String ddl = stmt.toString();
                assert (ddl != null) && !ddl.equals("") : "Do not know how to generate DDL for "
                    + elem.getClass();
                outBuf.append(ddl);
                outBuf.append(SEP);
            } catch (RuntimeException e) {
                throw Util.newInternal(
                    e,
                    "Error while exporting '" + elem + "'");
            }
        }
        return outBuf.toString();
    }
}

// End DdlGenerator.java
