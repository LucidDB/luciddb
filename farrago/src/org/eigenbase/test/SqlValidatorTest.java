/*
// $Id$
// Package org.eigenbase is a class library of database components.
// Copyright (C) 2002-2004 Disruptive Tech
// Copyright (C) 2003-2004 John V. Sichi
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

package org.eigenbase.test;

import org.eigenbase.reltype.RelDataTypeFactoryImpl;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.sql.SqlOperatorTable;
import org.eigenbase.sql.SqlValidator;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.sql.parser.ParseException;
import org.eigenbase.sql.parser.SqlParser;

import java.util.ArrayList;
import java.util.HashMap;


/**
 * Concrete child class of {@link SqlValidatorTestCase}.
 *
 * <p>It contains a mock schema with <code>EMP</code> and <code>DEPT</code>
 * tables, which can run without having to start up Farrago.
 *
 * @author Wael Chatila
 * @since Jan 14, 2004
 * @version $Id$
 **/
public class SqlValidatorTest extends SqlValidatorTestCase
{
    //~ Methods ---------------------------------------------------------------

    public SqlParser getParser(String sql)
        throws ParseException
    {
        return new SqlParser(sql);
    }

    public SqlValidator getValidator()
    {
        final RelDataTypeFactory typeFactory = new RelDataTypeFactoryImpl();
        return new SqlValidator(
            SqlOperatorTable.instance(),
            new MockCatalogReader(typeFactory),
            typeFactory);
    }

    //~ Inner Classes ---------------------------------------------------------

    /**
     * Mock implmenentation of {@link SqlValidator.CatalogReader} which returns
     * tables "EMP", "DEPT", "BONUS", "SALGRADE" (same as Oracle's SCOTT
     * schema).
     */
    public class MockCatalogReader implements SqlValidator.CatalogReader
    {
        private final RelDataTypeFactory typeFactory;
        private final HashMap tables = new HashMap();

        public MockCatalogReader(RelDataTypeFactory typeFactory) {
            this.typeFactory = typeFactory;
            // Register "EMP" table.
            MockTable empTable = new MockTable("EMP");
            empTable.addColumn("EMPNO",
                typeFactory.createSqlType(SqlTypeName.Integer));
            empTable.addColumn("ENAME",
                typeFactory.createSqlType(SqlTypeName.Varchar, 20));
            empTable.addColumn("JOB",
                typeFactory.createSqlType(SqlTypeName.Varchar, 10));
            empTable.addColumn("MGR",
                typeFactory.createSqlType(SqlTypeName.Integer));
            empTable.addColumn("HIREDATE",
                typeFactory.createSqlType(SqlTypeName.Date));
            empTable.addColumn("SAL",
                typeFactory.createSqlType(SqlTypeName.Integer));
            empTable.addColumn("COMM",
                typeFactory.createSqlType(SqlTypeName.Integer));
            empTable.addColumn("DEPTNO",
                typeFactory.createSqlType(SqlTypeName.Integer));
            registerTable(empTable);
            // Register "DEPT" table.
            MockTable deptTable = new MockTable("DEPT");
            deptTable.addColumn("DEPTNO",
                typeFactory.createSqlType(SqlTypeName.Integer));
            deptTable.addColumn("NAME",
                typeFactory.createSqlType(SqlTypeName.Varchar, 10));
            registerTable(deptTable);
            // Register "BONUS" table.
            MockTable bonusTable = new MockTable("BONUS");
            bonusTable.addColumn("ENAME",
                typeFactory.createSqlType(SqlTypeName.Varchar, 20));
            bonusTable.addColumn("JOB",
                typeFactory.createSqlType(SqlTypeName.Varchar, 10));
            bonusTable.addColumn("SAL",
                typeFactory.createSqlType(SqlTypeName.Integer));
            bonusTable.addColumn("COMM",
                typeFactory.createSqlType(SqlTypeName.Integer));
            registerTable(bonusTable);
            // Register "SALGRADE" table.
            MockTable salgradeTable = new MockTable("SALGRADE");
            salgradeTable.addColumn("GRADE",
                typeFactory.createSqlType(SqlTypeName.Integer));
            salgradeTable.addColumn("LOSAL",
                typeFactory.createSqlType(SqlTypeName.Integer));
            salgradeTable.addColumn("HISAL",
                typeFactory.createSqlType(SqlTypeName.Integer));
            registerTable(salgradeTable);
        }

        private void registerTable(MockTable table) {
            table.onRegister(typeFactory);
            tables.put(table.names[0], table);
        }

        public SqlValidator.Table getTable(final String [] names)
        {
            if (names.length == 1) {
                return (SqlValidator.Table) tables.get(names[0]);
            }
            return null;
        }
    }

    /**
     * Mock implementation of {@link SqlValidator.Table}.
     */
    private static class MockTable implements SqlValidator.Table
    {
        private final ArrayList columnNames = new ArrayList();
        private final ArrayList columnTypes = new ArrayList();
        private RelDataType rowType;
        private final String[] names;

        public MockTable(String name) {
            this.names = new String[] {name};
        }

        public RelDataType getRowType() {
            return rowType;
        }

        public void onRegister(RelDataTypeFactory typeFactory) {
            rowType = typeFactory.createProjectType(
                (RelDataType []) columnTypes.toArray(new RelDataType[0]),
                (String []) columnNames.toArray(new String[0]));
        }

        public String[] getQualifiedName() {
            return names;
        }

        public void addColumn(String name, RelDataType type) {
            columnNames.add(name);
            columnTypes.add(type);
        }
    }
}


// End SqlValidatorTest.java
