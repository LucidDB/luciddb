/*
// $Id$
// LucidDB is a DBMS optimized for business intelligence.
// Copyright (C) 2008-2008 LucidEra, Inc.
// Copyright (C) 2008-2008 The Eigenbase Project
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
package net.sf.luciddb.aggdes;

import org.pentaho.aggdes.*;

import java.util.*;

/**
 * CreateMondrianAggregatesUdp is a SQL-invocable procedure for running the
 * Mondrian aggregate designer in order to create aggregate tables for a
 * LucidDB warehouse schema.
 *
 * @author John Sichi
 * @version $Id$
 */
public class CreateMondrianAggregatesUdp
{
    public static void execute(
        String schemaFile, String cubeName, String algorithmClass,
        int timeLimitSeconds, int aggregateLimit,
        String aggSchemaFile)
        throws Exception
    {
        List<String> args = new ArrayList<String>();
        args.add("--loaderClass");
        args.add("org.pentaho.aggdes.model.mondrian.MondrianSchemaLoader");
        args.add("--loaderParam");
        args.add("connectString");
        args.add("'Provider=mondrian;Jdbc=jdbc:default:connection;Catalog="
            + schemaFile);
        args.add("--loaderParam");
        args.add("cube");
        args.add(cubeName);
        args.add("--algorithmClass");
        args.add(algorithmClass);
        args.add("--algorithmParam");
        args.add("timeLimitSeconds");
        args.add(Integer.toString(timeLimitSeconds));
        args.add("--algorithmParam");
        args.add("aggregateLimit");
        args.add(Integer.toString(aggregateLimit));
        args.add("--resultClass");
        args.add("net.sf.luciddb.aggdes.LucidDbAggResultHandler");
        args.add("--resultParam");
        args.add("tables");
        args.add("true");
        args.add("--resultParam");
        args.add("indexes");
        args.add("true");
        args.add("--resultParam");
        args.add("populate");
        args.add("true");
        args.add("--resultParam");
        args.add("mondrianSchema");
        args.add("true");
        args.add("--resultParam");
        args.add("mondrianOutput");
        args.add(aggSchemaFile);
        Main.main(args.toArray(new String[0]));
    }
}

// End CreateMondrianAggregatesUdp.java
