/*
// Saffron preprocessor and data engine.
// Copyright (C) 2002-2004 Disruptive Tech
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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

package sales;


/**
 * An <code>Emp</code> represents an employee.
 */
public class Emp
{
    public int empno;
    public String name;
    public int deptno;
    public String gender;
    public String city;
    public boolean slacker;

    public Emp(
        int empno,
        String name,
        int deptno,
        String gender,
        String city,
        boolean slacker)
    {
        this.empno = empno;
        this.name = name;
        this.deptno = deptno;
        this.gender = gender;
        this.city = city;
        this.slacker = slacker;
    }

    public Emp(java.sql.ResultSet resultSet)
        throws java.sql.SQLException
    {
        this.empno = resultSet.getInt(1);
        this.name = resultSet.getString(2);
        this.deptno = resultSet.getInt(3);
        this.gender = resultSet.getString(4);
        this.city = resultSet.getString(5);
        this.slacker = resultSet.getBoolean(6);
    }
}


// End Emp.java
