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

import java.lang.reflect.*;

import java.util.*;

import net.sf.farrago.cwm.core.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fem.sql2003.*;

import org.eigenbase.sql.SqlDialect;
import org.eigenbase.sql.SqlUtil;
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

    protected static SqlDialect sqlDialect = SqlUtil.eigenbaseDialect;
    protected static String VALUE_NULL = "NULL";
    protected static String NL = System.getProperty("line.separator");

    //~ Methods ----------------------------------------------------------------

    public void generateSetSchema(GeneratedDdlStmt stmt, String schemaName)
    {
        if (schemaName != null) {
            StringBuilder sb = new StringBuilder();
            sb.append("SET SCHEMA ");
            sb.append(DdlGenerator.literal(quote(schemaName)));
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

    private void generate(String method,
        CwmModelElement e,
        GeneratedDdlStmt stmt)
    {
        Method m =
            ReflectUtil.lookupVisitMethod(
                this.getClass(),
                e.getClass(),
                method,
                Arrays.asList(new Class[] { GeneratedDdlStmt.class }));
        if (m != null) {
            try {
                m.invoke(
                    this,
                    new Object[] { e, stmt });
            } catch (Throwable t) {
                //TODO: handle
                t.printStackTrace();
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
                    if (kc.getKeyConstraint()
                        instanceof FemPrimaryKeyConstraint) {
                        result = true;
                        break;
                    }
                }
            }
        }
        return result;
    }
}

// End DdlGenerator.java
