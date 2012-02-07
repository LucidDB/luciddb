/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/
package net.sf.farrago.runtime;

import java.sql.*;

import java.util.List;
import java.util.logging.*;

import net.sf.farrago.fennel.tuple.*;
import net.sf.farrago.session.*;
import net.sf.farrago.type.FarragoResultSetMetaData;

import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.runtime.*;


/**
 * FennelOnlyResultSet is a refinement of FarragoTupleIterResultSet, where the
 * result set consists of Fennel tuples.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class FennelOnlyResultSet
    extends FarragoTupleIterResultSet
{
    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FennelOnlyResultSet object.
     *
     * @param tupleIter underlying iterator
     * @param rowType type info for rows produced
     * @param fieldOrigins Origin of each field as a column of a catalog object
     * @param runtimeContext runtime context for this execution
     */
    public FennelOnlyResultSet(
        TupleIter tupleIter,
        RelDataType rowType,
        List<List<String>> fieldOrigins,
        FarragoSessionRuntimeContext runtimeContext)
    {
        super(
            tupleIter,
            null,
            rowType,
            fieldOrigins,
            runtimeContext,
            new FennelColumnGetter(
                new FarragoResultSetMetaData(rowType, fieldOrigins),
                rowType));
        if (tracer.isLoggable(Level.FINE)) {
            tracer.fine(toString());
        }
    }

    //~ Methods ----------------------------------------------------------------

    // implement AbstractResultSet
    protected Object getRaw(int columnIndex)
    {
        Object obj = super.getRaw(columnIndex);
        wasNull = (obj == null);
        return obj;
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * ColumnGetter that reads columns from a Fennel tuple
     *
     * @author Zelaine Fong
     */
    private static class FennelColumnGetter
        implements ColumnGetter
    {
        final private ResultSetMetaData metaData;
        final private RelDataType rowType;

        /**
         * @param metaData metadata corresponding to the result set from which
         * the columns will be read
         * @param rowType row type of the tuple containing the columns to be
         * read
         */
        public FennelColumnGetter(
            ResultSetMetaData metaData,
            RelDataType rowType)
        {
            this.metaData = metaData;
            this.rowType = rowType;
        }

        public String [] getColumnNames()
        {
            return RelOptUtil.getFieldNames(rowType);
        }

        public Object get(
            Object o,
            int columnIndex)
        {
            try {
                return FennelTupleResultSet.getRawColumnData(
                    columnIndex,
                    metaData,
                    (FennelTupleData) o);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }
}

// End FennelOnlyResultSet.java
