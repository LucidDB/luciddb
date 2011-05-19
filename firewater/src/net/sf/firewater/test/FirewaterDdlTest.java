/*
// $Id$
// Firewater is a scaleout column store DBMS.
// Copyright (C) 2009-2009 John V. Sichi
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
package net.sf.firewater.test;

import org.eigenbase.test.*;

import net.sf.farrago.test.*;

import junit.extensions.*;
import junit.framework.*;

import net.sf.firewater.*;

import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fem.security.*;

import com.lucidera.luciddb.test.*;

/**
 * FirewaterDdlTest is a unit test for Firewater distributed DDL execution.
 *
 * @author John Sichi
 * @version $Id$
 */
public class FirewaterDdlTest extends FirewaterTestCase
{
    public FirewaterDdlTest(String testName)
        throws Exception
    {
        super(testName);
    }

    // implement TestCase
    public static Test suite()
    {
        return wrappedSuite(new TestSuite(FirewaterDdlTest.class));
    }

    private void runAndDiff(String sql)
        throws Exception
    {
        DiffRepository diffRepos = getDiffRepos();
        stmt.execute(sql);
        String remoteSql = System.getProperty("firewater.test.lastSql");
        diffRepos.assertEquals("remoteSql", "${remoteSql}", remoteSql);
    }

    private DiffRepository getDiffRepos()
    {
        return DiffRepository.lookup(FirewaterDdlTest.class);
    }

    public void testCreatePartition()
        throws Exception
    {
        String sql = "create partition p1 on (sys_firewater_embedded_server)";
        runAndDiff(sql);

        sql = "drop partition p1";
        stmt.execute(sql);
    }

    public void testDropPartition()
        throws Exception
    {
        String sql = "create partition p2 on (sys_firewater_embedded_server)";
        stmt.execute(sql);

        sql = "drop partition p2";
        runAndDiff(sql);
    }

    public void testCreateSchema()
        throws Exception
    {
        String sql = "create schema s1 description 'Lumpy'";
        runAndDiff(sql);
    }

    public void testDropSchema()
        throws Exception
    {
        String sql = "create schema s5";
        stmt.execute(sql);

        sql = "drop schema s5";
        runAndDiff(sql);
    }

    public void testCreateTable()
        throws Exception
    {
        String sql = "create schema s2";
        stmt.execute(sql);

        sql = "create table s2.t(i int, v varchar(128) not null primary key)";
        runAndDiff(sql);
    }

    public void testDropTable()
        throws Exception
    {
        String sql = "create schema s6";
        stmt.execute(sql);

        sql = "create table s6.t(i int, v varchar(128) not null primary key)";
        stmt.execute(sql);

        sql = "drop table s6.t";
        runAndDiff(sql);
    }

    public void testCreateIndex()
        throws Exception
    {
        String sql = "create schema s3";
        stmt.execute(sql);

        sql = "create table s3.t(i int, v varchar(128) not null primary key)";
        stmt.execute(sql);

        sql = "create index x on s3.t(i)";
        runAndDiff(sql);
    }

    public void testDropIndex()
        throws Exception
    {
        String sql = "create schema s7";
        stmt.execute(sql);

        sql = "create table s7.t(i int, v varchar(128) not null primary key)";
        stmt.execute(sql);

        sql = "create index x on s7.t(i)";
        stmt.execute(sql);

        sql = "drop index s7.x";
        runAndDiff(sql);
    }

    public void testCreateLabel()
        throws Exception
    {
        String sql = "create label l1";
        runAndDiff(sql);
    }

    public void testDropLabel()
        throws Exception
    {
        String sql = "create label l2";
        stmt.execute(sql);

        sql = "drop label l2";
        runAndDiff(sql);
    }

    public void testCreateView()
        throws Exception
    {
        // note that view creation is currently NOT distributed,
        // so we expect to see the schema creation as the last stmt
        String sql = "create schema s4";
        stmt.execute(sql);

        sql = "create view s4.v as select * from (values(0))";
        runAndDiff(sql);
    }

    public void testDropView()
        throws Exception
    {
        // note that view creation is currently NOT distributed,
        // so we expect to see the schema creation as the last stmt
        String sql = "create schema s8";
        stmt.execute(sql);

        sql = "create view s8.v as select * from (values(0))";
        stmt.execute(sql);

        sql = "drop view s8.v";
        runAndDiff(sql);
    }
}

// End FirewaterDdlTest.java
