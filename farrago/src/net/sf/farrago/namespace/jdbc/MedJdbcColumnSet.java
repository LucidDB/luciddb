/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
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

package net.sf.farrago.namespace.jdbc;

import net.sf.farrago.namespace.*;
import net.sf.farrago.type.*;
import net.sf.farrago.util.*;

import net.sf.saffron.ext.*;
import net.sf.saffron.sql.*;
import net.sf.saffron.rel.*;
import net.sf.saffron.rel.jdbc.*;
import net.sf.saffron.rel.convert.*;
import net.sf.saffron.opt.*;
import net.sf.saffron.core.*;
import net.sf.saffron.util.*;

import java.sql.*;
import java.util.*;

import javax.sql.*;

/**
 * MedJdbcColumnSet implements FarragoNamedColumnSet for foreign JDBC tables.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class MedJdbcColumnSet extends AbstractTable implements FarragoNamedColumnSet
{
    final MedJdbcNameDirectory directory;
    
    final String [] localName;
    
    final String [] foreignName;

    final SqlSelect select;

    final SqlDialect dialect;
    
    MedJdbcColumnSet(
        MedJdbcNameDirectory directory,
        FarragoTypeFactory typeFactory,
        String [] foreignName,
        String [] localName,
        SqlSelect select,
        SqlDialect dialect,
        SaffronType rowType)
    {
        super(
            null,
            localName[localName.length - 1],
            rowType);
        this.directory = directory;
        this.foreignName = foreignName;
        this.localName = localName;
        this.select = select;
        this.dialect = dialect;
    }
    
    // implement SaffronTable
    public String [] getQualifiedName()
    {
        return localName;
    }

    // implement SaffronTable
    public double getRowCount()
    {
        // TODO:  use getStatistics?
        return super.getRowCount();
    }
    
    // implement SaffronTable
    public SaffronRel toRel(
        VolcanoCluster cluster,
        SaffronConnection connection)
    {
        // make sure optimizer knows about JDBC-specific transformations
        JdbcQuery.register(cluster.getPlanner());
        cluster.getPlanner().addRule(
            new ConverterRule(
                SaffronRel.class,
                CallingConvention.RESULT_SET,
                CallingConvention.ITERATOR,
                "ResultSetToFarragoIteratorRule")
            {
                public SaffronRel convert(SaffronRel rel)
                {
                    return new ResultSetToFarragoIteratorConverter(
                        rel.getCluster(),
                        rel);
                }
            });
        
        return new MedJdbcQueryRel(
            this,
            cluster,
            getRowType(),
            connection,
            dialect,
            select);
    }
}

// End MedJdbcColumnSet.java
