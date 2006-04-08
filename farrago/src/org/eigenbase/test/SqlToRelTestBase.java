/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2006 The Eigenbase Project
// Copyright (C) 2002-2006 Disruptive Tech
// Copyright (C) 2005-2006 LucidEra, Inc.
// Portions Copyright (C) 2003-2006 John V. Sichi
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

import junit.framework.*;
import openjava.mop.*;
import org.eigenbase.oj.util.*;
import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.relopt.hep.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.sql2rel.SqlToRelConverter;
import org.eigenbase.util.*;

import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

/**
 * SqlToRelTestBase is an abstract base for tests which involve conversion
 * from SQL to relational algebra.
 *
 *<p>
 *
 * SQL statements to be translated can use the schema defined in {@link
 * MockCatalogReader}; note that this is slightly different from Farrago's
 * SALES schema.  If you get a parser or validator error from your test SQL,
 * look down in the stack until you see "Caused by", which will usually tell
 * you the real error.
 *
 * @author jhyde
 * @version $Id$
 */
public class SqlToRelTestBase extends TestCase
{
    protected static final String NL = System.getProperty("line.separator");
    protected final Tester tester = createTester();

    protected Tester createTester()
    {
        return new TesterImpl();
    }

    /**
     * Mock implementation of {@link RelOptSchema}.
     */
    protected static class MockRelOptSchema implements RelOptSchema {
        private final SqlValidatorCatalogReader catalogReader;
        private final RelDataTypeFactory typeFactory;

        public MockRelOptSchema(
            SqlValidatorCatalogReader catalogReader,
            RelDataTypeFactory typeFactory)
        {
            this.catalogReader = catalogReader;
            this.typeFactory = typeFactory;
        }

        public RelOptTable getTableForMember(String[] names)
        {
            final SqlValidatorTable table = catalogReader.getTable(names);
            final RelDataType rowType = table.getRowType();
            final List<RelCollation> collationList =
                new ArrayList<RelCollation>();
            // Deduce which fields the table is sorted on.
            int i = -1;
            for (RelDataTypeField field : rowType.getFields()) {
                ++i;
                if (table.isMonotonic(field.getName())) {
                    collationList.add(
                        new RelCollationImpl(
                            Collections.singletonList(
                                new RelFieldCollation(
                                    i,
                                    RelFieldCollation.Direction.Ascending))));
                }
            }
            return createColumnSet(names, rowType, collationList);
        }

        protected MockColumnSet createColumnSet(
            String[] names,
            final RelDataType rowType,
            final List<RelCollation> collationList)
        {
            return new MockColumnSet(names, rowType, collationList);
        }

        public RelDataTypeFactory getTypeFactory()
        {
            return typeFactory;
        }

        public void registerRules(RelOptPlanner planner)
            throws Exception
        {
        }

        protected class MockColumnSet implements RelOptTable
        {
            private final String[] names;
            private final RelDataType rowType;
            private final List<RelCollation> collationList;

            protected MockColumnSet(
                String[] names,
                RelDataType rowType,
                final List<RelCollation> collationList)
            {
                this.names = names;
                this.rowType = rowType;
                this.collationList = collationList;
            }

            public String[] getQualifiedName() 
            {
                return names;
            }

            public double getRowCount()
            {
                // use something other than 0 to give costing tests
                // some room, and make emps bigger than depts for
                // join asymmetry
                if (names[names.length - 1].equals("EMP")) {
                    return 1000;
                } else {
                    return 100;
                }
            }

            public RelDataType getRowType()
            {
                return rowType;
            }

            public RelOptSchema getRelOptSchema()
            {
                return MockRelOptSchema.this;
            }

            public RelNode toRel(RelOptCluster cluster, RelOptConnection connection)
            {
                return new TableAccessRel(cluster, this, connection);
            }

            public List<RelCollation> getCollationList()
            {
                return collationList;
            }
        }
    }

    /**
     * Mock implementation of {@link RelOptConnection}, contains a
     * {@link MockRelOptSchema}.
     */
    private static class MockRelOptConnection implements RelOptConnection
    {
        private final RelOptSchema relOptSchema;

        public MockRelOptConnection(RelOptSchema relOptSchema)
        {
            this.relOptSchema = relOptSchema;
        }

        public RelOptSchema getRelOptSchema()
        {
            return relOptSchema;
        }

        public Object contentsAsArray(
            String qualifier,
            String tableName)
        {
            return null;
        }
    }

    /**
     * Helper class which contains default implementations of methods used
     * for running sql-to-rel conversion tests.
     */
    public static interface Tester
    {
        /**
         * Converts a SQL string to a {@link RelNode} tree.
         *
         * @param sql SQL statement
         * @return Relational expression, never null
         *
         * @pre sql != null
         * @post return != null
         */
        RelNode convertSqlToRel(String sql);

        SqlNode parseQuery(String sql) throws Exception;

        /**
         * Factory method to create a {@link SqlValidator}.
         */
        SqlValidator createValidator(
            SqlValidatorCatalogReader catalogReader,
            RelDataTypeFactory typeFactory);

        /**
         * Factory method for a {@link SqlValidatorCatalogReader}.
         */
        SqlValidatorCatalogReader createCatalogReader(
            RelDataTypeFactory typeFactory);

        RelOptPlanner createPlanner();

        /**
         * Returns the {@link SqlOperatorTable} to use.
         */
        SqlOperatorTable getOperatorTable();

        MockRelOptSchema createRelOptSchema(
            SqlValidatorCatalogReader catalogReader,
            RelDataTypeFactory typeFactory);
    };

    /**
     * Default implementation of {@link Tester}, using mock classes
     * {@link MockRelOptSchema}, {@link MockRelOptConnection} and
     * {@link MockRelOptPlanner}.
     */
    public static class TesterImpl implements Tester
    {
        private RelOptPlanner planner;
        private SqlOperatorTable opTab;

        protected TesterImpl()
        {
        }

        public RelNode convertSqlToRel(String sql)
        {
            Util.pre(sql != null, "sql != null");
            final SqlNode sqlQuery;
            try {
                sqlQuery = parseQuery(sql);
            } catch (Exception e) {
                throw Util.newInternal(e); // todo: better handling
            }
            final RelDataTypeFactory typeFactory = createTypeFactory();
            final SqlValidatorCatalogReader catalogReader =
                createCatalogReader(typeFactory);
            final SqlValidator validator =
                createValidator(catalogReader, typeFactory);
            final RelOptSchema relOptSchema =
                createRelOptSchema(catalogReader, typeFactory);
            final RelOptConnection relOptConnection =
                new MockRelOptConnection(relOptSchema);
            final SqlToRelConverter converter =
                createSqlToRelConverter(
                    validator, relOptSchema, relOptConnection, typeFactory);
            final RelNode rel;
            if (Bug.Dt471Fixed) {
                final SqlNode validatedQuery = validator.validate(sqlQuery);
                rel = converter.convertQuery(validatedQuery, false, true);
            } else {
                rel = converter.convertQuery(sqlQuery, true, true);
            }
            Util.post(rel != null, "return != null");
            return rel;
        }

        public MockRelOptSchema createRelOptSchema(
            final SqlValidatorCatalogReader catalogReader,
            final RelDataTypeFactory typeFactory)
        {
            return new MockRelOptSchema(catalogReader, typeFactory);
        }

        protected SqlToRelConverter createSqlToRelConverter(
            final SqlValidator validator,
            final RelOptSchema relOptSchema,
            final RelOptConnection relOptConnection,
            final RelDataTypeFactory typeFactory)
        {
            final SqlToRelConverter converter =
                new SqlToRelConverter(
                    validator,
                    relOptSchema,
                    OJSystem.env,
                    getPlanner(),
                    relOptConnection,
                    new JavaRexBuilder(typeFactory));
            return converter;
        }

        protected RelDataTypeFactory createTypeFactory()
        {
            return new SqlTypeFactoryImpl();
        }

        protected final RelOptPlanner getPlanner()
        {
            if (planner == null) {
                planner = createPlanner();
            }
            return planner;
        }

        public SqlNode parseQuery(String sql) throws Exception {
            SqlParser parser = new SqlParser(sql);
            SqlNode sqlNode = parser.parseQuery();
            return sqlNode;
        }

        public SqlValidator createValidator(
            SqlValidatorCatalogReader catalogReader,
            RelDataTypeFactory typeFactory)
        {
            return new FarragoTestValidator(
                getOperatorTable(),
                new MockCatalogReader(typeFactory),
                typeFactory);
        }

        public final SqlOperatorTable getOperatorTable()
        {
            if (opTab == null) {
                opTab = createOperatorTable();
            }
            return opTab;
        }

        protected SqlOperatorTable createOperatorTable()
        {
            final MockSqlOperatorTable opTab =
                new MockSqlOperatorTable(SqlStdOperatorTable.instance());
            MockSqlOperatorTable.addRamp(opTab);
            return opTab;
        }

        public SqlValidatorCatalogReader createCatalogReader(
            RelDataTypeFactory typeFactory)
        {
            return new MockCatalogReader(typeFactory);
        }

        public RelOptPlanner createPlanner()
        {
            return new MockRelOptPlanner();
        }
    }

    private static class FarragoTestValidator extends SqlValidatorImpl
    {
        public FarragoTestValidator(
            SqlOperatorTable opTab,
            SqlValidatorCatalogReader catalogReader,
            RelDataTypeFactory typeFactory)
        {
            super(opTab, catalogReader, typeFactory);
        }

        // override SqlValidator
        protected boolean shouldExpandIdentifiers()
        {
            return true;
        }
    }
}

// End SqlToRelTestBase.java
