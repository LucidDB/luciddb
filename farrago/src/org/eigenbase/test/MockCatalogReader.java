/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2004-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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
package org.eigenbase.test;

import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.reltype.*;
import org.eigenbase.util.Util;

import java.util.HashMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Vector;

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
    private final HashMap schemas = new HashMap();
    private final RelDataType addressType;

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

        // TODO jvs 12-Feb-2005: register this canonical instance with type
        // factory
        addressType = new ObjectSqlType(
            SqlTypeName.Structured,
            new SqlIdentifier("ADDRESS", null),
            false,
            new RelDataTypeField [] {
                new RelDataTypeFieldImpl("STREET", 0, varchar20Type),
                new RelDataTypeFieldImpl("CITY", 1, varchar20Type),
                new RelDataTypeFieldImpl("ZIP", 1, intType), 
                new RelDataTypeFieldImpl("STATE", 1, varchar20Type)
            });

        // Register "SALES" schema.
        MockSchema salesSchema = new MockSchema("SALES");
        registerSchema(salesSchema);

        // Register "EMP" table.
        MockTable empTable = new MockTable(salesSchema, "EMP");
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
        MockTable deptTable = new MockTable(salesSchema, "DEPT");
        deptTable.addColumn("DEPTNO", intType);
        deptTable.addColumn("NAME", varchar10Type);
        registerTable(deptTable);
        // Register "BONUS" table.
        MockTable bonusTable = new MockTable(salesSchema, "BONUS");
        bonusTable.addColumn("ENAME", varchar20Type);
        bonusTable.addColumn("JOB", varchar10Type);
        bonusTable.addColumn("SAL", intType);
        bonusTable.addColumn("COMM", intType);
        registerTable(bonusTable);
        // Register "SALGRADE" table.
        MockTable salgradeTable = new MockTable(salesSchema, "SALGRADE");
        salgradeTable.addColumn("GRADE", intType);
        salgradeTable.addColumn("LOSAL", intType);
        salgradeTable.addColumn("HISAL", intType);
        registerTable(salgradeTable);

        // Register "EMP_ADDRESS" table
        MockTable contactAddressTable =
            new MockTable(salesSchema, "EMP_ADDRESS");
        contactAddressTable.addColumn("EMPNO", intType);
        contactAddressTable.addColumn("HOME_ADDRESS", addressType);
        contactAddressTable.addColumn("MAILING_ADDRESS", addressType);
        registerTable(contactAddressTable);
        
        // Register "CUSTOMER" schema.
        MockSchema customerSchema = new MockSchema("CUSTOMER");
        registerSchema(customerSchema);
        // Register "CONTACT" table.
        MockTable contactTable = new MockTable(customerSchema, "CONTACT");
        contactTable.addColumn("CONTACTNO", intType);
        contactTable.addColumn("FNAME", varchar10Type);
        contactTable.addColumn("LNAME", varchar10Type);
        contactTable.addColumn("EMAIL", varchar20Type);
        registerTable(contactTable);
        // Register "ACCOUNT" table.
        MockTable accountTable = new MockTable(customerSchema, "ACCOUNT");
        accountTable.addColumn("ACCTNO", intType);
        accountTable.addColumn("TYPE", varchar20Type);
        accountTable.addColumn("BALANCE", intType);
        registerTable(accountTable);
    }

    protected void registerTable(MockTable table) {
        table.onRegister(typeFactory);
        tables.put(convertToVector(table.getQualifiedName()), table);
    }

    protected void registerSchema(MockSchema schema) {
        schemas.put(schema.name, schema);
    }

    public SqlValidator.Table getTable(final String [] names)
    {
        if (names.length == 1) {
            // assume table in SALES schema (the original default)
            // if it's not supplied, because SqlValidatorTest is effectively 
            // using SALES as its default schema.
            String [] qualifiedName = { "SALES", names[0] };
            return (SqlValidator.Table) tables.get(
                convertToVector(qualifiedName));
        }
        else if (names.length == 2) {
            return (SqlValidator.Table) tables.get(convertToVector(names));
        }
        return null;
    }

    public RelDataType getNamedType(SqlIdentifier typeName)
    {
        if (typeName.equalsDeep(addressType.getSqlIdentifier())) {
            return addressType;
        } else {
            return null;
        }
    }
    
    public String [] getAllSchemaObjectNames(String [] names)
    {
        if (names.length == 1) {
            // looking for both schema and object names
            Collection schemasColl = schemas.values(); 
            Iterator i = schemasColl.iterator();
            ArrayList result = new ArrayList();
            while (i.hasNext()) {
                MockSchema schema = (MockSchema) i.next();
                result.add(schema.name);
                result.addAll(schema.tableNames);
            }     
            return (String [])result.toArray(Util.emptyStringArray);
        }
        else if (names.length == 2) {
            // looking for table names under the schema
            MockSchema schema = (MockSchema) schemas.get(names[0]);
            return (String [])schema.tableNames.toArray(Util.emptyStringArray);
        }
        else {
            return Util.emptyStringArray;
        }
    }

    private Vector convertToVector(String [] names) {
        Vector v = new Vector(names.length);
        for (int i = 0; i < names.length; i++) {
            v.addElement(names[i]);
        }
        return v;
    }

    public static class MockSchema
    {
        private final ArrayList tableNames = new ArrayList();
        private String name; 

        public MockSchema(String name) {
            this.name = name;
        }

        public void addTable(String name) {
            tableNames.add(name);
        }
    }

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
            // default schema is SALES
            this.names = new String[] {"SALES", name};
        }

        public MockTable(MockSchema schema, String name) {
            this.names = new String[] {schema.name, name};
            schema.addTable(name);
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
