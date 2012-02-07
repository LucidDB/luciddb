/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
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
