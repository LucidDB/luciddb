/*
// $Id$
// Package org.eigenbase is a class library of database components.
// Copyright (C) 2004-2004 Disruptive Tech
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

import org.eigenbase.sql.SqlValidator;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.reltype.RelDataType;

import java.util.HashMap;
import java.util.ArrayList;

/**
 * Mock implementation of
 * {@link org.eigenbase.sql.SqlValidator.CatalogReader} which returns
 * tables "EMP", "DEPT", "BONUS", "SALGRADE" (same as Oracle's SCOTT
 * schema).
 *
 * @author jhyde
 * @version $Id$
 */
public class MockCatalogReader implements SqlValidator.CatalogReader
{
    protected final RelDataTypeFactory typeFactory;
    private final HashMap tables = new HashMap();

    public MockCatalogReader(RelDataTypeFactory typeFactory) {
        this.typeFactory = typeFactory;
        final RelDataType intType =
            typeFactory.createSqlType(SqlTypeName.Integer);
        final RelDataType varchar10Type =
            typeFactory.createSqlType(SqlTypeName.Varchar, 10);
        final RelDataType varchar20Type =
            typeFactory.createSqlType(SqlTypeName.Varchar, 20);
        final RelDataType dateType =
            typeFactory.createSqlType(SqlTypeName.Date);
        // Register "EMP" table.
        MockTable empTable = new MockTable("EMP");
        empTable.addColumn("EMPNO", intType);
        empTable.addColumn("ENAME", varchar20Type);
        empTable.addColumn("JOB", varchar10Type);
        empTable.addColumn("MGR", intType);
        empTable.addColumn("HIREDATE", dateType);
        empTable.addColumn("SAL", intType);
        empTable.addColumn("COMM", intType);
        empTable.addColumn("DEPTNO", intType);
        registerTable(empTable);
        // Register "DEPT" table.
        MockTable deptTable = new MockTable("DEPT");
        deptTable.addColumn("DEPTNO", intType);
        deptTable.addColumn("NAME", varchar10Type);
        registerTable(deptTable);
        // Register "BONUS" table.
        MockTable bonusTable = new MockTable("BONUS");
        bonusTable.addColumn("ENAME", varchar20Type);
        bonusTable.addColumn("JOB", varchar10Type);
        bonusTable.addColumn("SAL", intType);
        bonusTable.addColumn("COMM", intType);
        registerTable(bonusTable);
        // Register "SALGRADE" table.
        MockTable salgradeTable = new MockTable("SALGRADE");
        salgradeTable.addColumn("GRADE", intType);
        salgradeTable.addColumn("LOSAL", intType);
        salgradeTable.addColumn("HISAL", intType);
        registerTable(salgradeTable);
    }

    protected void registerTable(MockTable table) {
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

    public ArrayList getAllSchemaNames() { return null; }
    public ArrayList getAllSchemaNames(String catalogName) { return null; }
    public ArrayList getAllTableNames(String schemaName) { return null; }
    public ArrayList getAllTableNames(String catalogName, String schemaName) 
        { return null; }
    public ArrayList getAllTables() { return null; }

    /**
     * Mock implementation of {@link SqlValidator.Table}.
     */
    public static class MockTable implements SqlValidator.Table
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
            rowType = typeFactory.createStructType(
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
