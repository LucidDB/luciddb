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

import org.pentaho.aggdes.model.*;
import org.pentaho.aggdes.algorithm.*;
import org.pentaho.aggdes.output.*;
import org.pentaho.aggdes.output.impl.*;

import java.util.*;
import java.io.*;
import java.sql.*;

/**
 * LucidDbAggResultHandler implements the ResultHandler interface
 * by executing DDL commands again the local LucidDB instance.
 *
 * @author John Sichi
 * @version $Id$
 */
public class LucidDbAggResultHandler extends ResultHandlerImpl
{
    public LucidDbAggResultHandler()
    {
    }

    public void handle(
        Map<Parameter, Object> parameterValues,
        Schema schema,
        Result result)
    {
        try {
            handleImpl(parameterValues, schema, result);
        } catch (Exception ex) {
            throw new RuntimeException(
                "Aggregate designer failed; see trace for details", ex);
        }
    }

    private void handleImpl(
        Map<Parameter, Object> parameterValues,
        Schema schema,
        Result result)
        throws Exception
    {
        AggregateTableOutputFactory outputFactory =
            new AggregateTableOutputFactory();
        
        List<Output> outputs = outputFactory.createOutputs(
            schema, result.getAggregates());
        
        Connection conn =
            DriverManager.getConnection("jdbc:default:connection");

        Statement stmt = conn.createStatement();

        // CREATE TABLE
        CreateTableGenerator tgen = new CreateTableGenerator();
        for (Output output : outputs) {
            String sql = tgen.generate(schema, output);
            sql = sql.replaceAll("INTEGER\\(.*\\)", "INTEGER");
            sql = sql.replaceAll("BIGINT\\(.*\\)", "BIGINT");
            sql = sql.replaceAll("SMALLINT\\(.*\\)", "SMALLINT");
            executeSql(stmt, sql);
        }

        PopulateTableGenerator pgen = new PopulateTableGenerator();
        for (Output output : outputs) {
            // INSERT
            AggregateTableOutput tableOutput = (AggregateTableOutput) output;
            String sql = pgen.generate(schema, output);
            executeSql(stmt, sql);
            
            // ANALYZE
            final StringBuilder buf = new StringBuilder();
            buf.append("ANALYZE TABLE ");
            schema.getDialect().quoteIdentifier(
                buf, tableOutput.getCatalogName(), tableOutput.getSchemaName(),
                tableOutput.getTableName());
            buf.append(" ESTIMATE STATISTICS FOR ALL COLUMNS;");
            sql = buf.toString();
            executeSql(stmt, sql);
        }

        // Regenerate XML
        String mondrianOutput = null;
        for (Parameter p : parameterValues.keySet()) {
            if (p.getName().equals("mondrianOutput")) {
                mondrianOutput = (String) parameterValues.get(p);
                break;
            }
        }
        MondrianSchemaGenerator sgen = new MondrianSchemaGenerator();
        String xml = sgen.generateFull(schema, outputs);
        FileWriter fw = new FileWriter(mondrianOutput);
        try {
            fw.write(xml);
        } finally {
            fw.close();
        }
    }

    private void executeSql(Statement stmt, String sql)
        throws SQLException
    {
        int i = sql.lastIndexOf(';');
        sql = sql.substring(0, i);
        stmt.executeUpdate(sql);
    }
}

// End LucidDbAggResultHandler.java
