/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// You must accept the terms in LICENSE.html to use this software.
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

package sales;

/**
 * An <code>Emp</code> represents an employee.
 */
public class Emp
{
    //~ Instance fields -------------------------------------------------------

    public int empno;
    public String name;
    public int deptno;
    public String gender;
    public String city;

    //~ Constructors ----------------------------------------------------------

    public Emp(int empno,String name,int deptno,String gender,String city)
    {
        this.empno = empno;
        this.name = name;
        this.deptno = deptno;
        this.gender = gender;
        this.city = city;
    }

    public Emp(java.sql.ResultSet resultSet) throws java.sql.SQLException
    {
        this.empno = resultSet.getInt(1);
        this.name = resultSet.getString(2);
        this.deptno = resultSet.getInt(3);
        this.gender = resultSet.getString(4);
        this.city = resultSet.getString(5);
    }
}


// End Emp.java
