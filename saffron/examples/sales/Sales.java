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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.StringTokenizer;

import javax.sql.DataSource;

import net.sf.saffron.ext.JdbcSchema;
import net.sf.saffron.ext.JdbcTable;
import net.sf.saffron.ext.ReflectSchema;

import org.eigenbase.relopt.RelOptConnection;
import org.eigenbase.relopt.RelOptSchema;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.sql.SqlDialect;
import org.eigenbase.util.JdbcDataSource;
import org.eigenbase.util.SaffronProperties;
import org.eigenbase.util.Util;


/**
 * <code>Sales</code> is a JDBC connection to a Sales database.
 */
public class Sales extends net.sf.saffron.ext.JdbcConnection
{
    private static final SalesSchema schema = createSchema();

    public Sales(java.sql.Connection connection)
    {
        super(connection);
    }

    // for RelOptConnection
    public static RelOptSchema getRelOptSchemaStatic()
    {
        return schema;
    }

    // implement Connection
    public RelOptSchema getRelOptSchema()
    {
        return schema;
    }

    private static SalesSchema createSchema()
    {
        String connectString = Util.getSalesConnectString();
        return new SalesSchema(new JdbcDataSource(connectString));
    }

    public static class Customer
    {
        public java.sql.Date birthdate;
        public java.sql.Date date_accnt_opened;
        public String address1;
        public String address2;
        public String address3;
        public String address4;
        public String city;
        public String country;
        public String education;
        public String fname;
        public String gender;
        public String lname;
        public String marital_status;
        public String mi;
        public String phone1;
        public String phone2;
        public String postal_code;
        public String state_province;
        public String yearly_income;
        public int customer_id;
        public int customer_region_id;
        public int num_children_at_home;
        public int total_children;
        public long account_num;

        public Customer(java.sql.ResultSet resultSet)
            throws java.sql.SQLException
        {
            this.customer_id = resultSet.getInt(1);
            this.account_num = resultSet.getLong(2);
            this.lname = resultSet.getString(3);
            this.fname = resultSet.getString(4);
            this.mi = resultSet.getString(5);
            this.address1 = resultSet.getString(6);
            this.address2 = resultSet.getString(7);
            this.address3 = resultSet.getString(8);
            this.address4 = resultSet.getString(9);
            this.city = resultSet.getString(10);
            this.state_province = resultSet.getString(11);
            this.postal_code = resultSet.getString(12);
            this.country = resultSet.getString(13);
            this.customer_region_id = resultSet.getInt(14);
            this.phone1 = resultSet.getString(15);
            this.phone2 = resultSet.getString(16);
            this.birthdate = resultSet.getDate(17);
            this.marital_status = resultSet.getString(18);
            this.yearly_income = resultSet.getString(19);
            this.gender = resultSet.getString(20);
            this.total_children = resultSet.getInt(21);
            this.num_children_at_home = resultSet.getInt(22);
            this.education = resultSet.getString(23);
            this.date_accnt_opened = resultSet.getDate(24);
        }
    }

    public static class Product
    {
        public String category;
        public String delivery;
        public String product;
        public float price;
        public int prod_id;

        public Product(java.sql.ResultSet resultSet)
            throws java.sql.SQLException
        {
            this.prod_id = resultSet.getInt(1);
            this.delivery = resultSet.getString(2);
            this.category = resultSet.getString(3);
            this.product = resultSet.getString(4);
            this.price = resultSet.getFloat(5);
        }
    }

    public static class Region
    {
        public String city;
        public String state;

        public Region(java.sql.ResultSet resultSet)
            throws java.sql.SQLException
        {
            this.city = resultSet.getString(2);
            this.state = resultSet.getString(3);
        }

        public boolean cityStartsWith(String s)
        {
            return city.startsWith(s);
        }
    }

    public static class SalesSchema extends ReflectSchema implements JdbcSchema
    {
        public final DataSource dataSource;
        public final RelOptTable depts;
        public final RelOptTable emps;
        public final RelOptTable products;
        private SqlDialect dialect;

        public SalesSchema(DataSource dataSource)
        {
            this.dataSource = dataSource;
            Connection connection = null;
            try {
                connection = dataSource.getConnection();
                this.dialect = SqlDialect.create(connection.getMetaData());
                RelDataTypeFactory typeFactory = null;
                this.emps =
                    new JdbcTable(
                        this,
                        "EMP",
                        typeFactory.createJavaType(Emp.class));
                this.depts =
                    new JdbcTable(
                        this,
                        "DEPT",
                        typeFactory.createJavaType(Dept.class));
                this.products =
                    new JdbcTable(
                        this,
                        "PRODUCT",
                        typeFactory.createJavaType(Product.class));
            } catch (SQLException e) {
                throw Util.newInternal(e);
            } finally {
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (SQLException e) {
                    }
                }
            }
        }

        public DataSource getDataSource(RelOptConnection connection)
        {
            return dataSource;
        }

        public SqlDialect getSqlDialect()
        {
            return dialect;
        }
    }
}


// End Sales.java
