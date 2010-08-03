/*
// $Id$
// LucidDB is a DBMS optimized for business intelligence.
// Copyright (C) 2006-2007 LucidEra, Inc.
// Copyright (C) 2006-2007 The Eigenbase Project
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
package com.lucidera.luciddb.applib.cursor;

import java.sql.*;
import java.util.*;

import com.lucidera.luciddb.applib.resource.*;

import net.sf.farrago.syslib.*;

/**
 * Generates a range of integers based on the number of rows passed in,
 * allowing for custom initial values and step values.
 *
 * @author Kevin Secretan
 * @version $Id$
 */
public abstract class GenerateSequenceUdx {
    
    /**
     * Simple range, unpartitioned.
     * @param inputSet - table of rows to count range for.
     * @param startVal - starting value of the count, defaults to -1
     * @param stepVal  - amount to increment the count, defaults to 1,
     *                   cannot be 0.
     * @param resultInserter - Handles the output.
     */
    public static void execute(
            ResultSet inputSet,
            Long startVal,
            Long stepVal,
            PreparedStatement resultInserter
            )
        throws ApplibException
    {
        execute(inputSet, null, startVal, stepVal, resultInserter);
    }

    /**
     * The fully general sequence generator, will start the sequence over
     * if it detects a new key, where a key is a unique column or intersection
     * of columns.
     * @see execute(ResultSet,Long,Long,PreparedStatement)
     * @param columnNames - list of column names composing a unique key.
     */
    public static void execute(
            ResultSet inputSet,
            List<String> columnNames,
            Long startVal,
            Long stepVal,
            PreparedStatement resultInserter
            )
        throws ApplibException
    {
        final long start = (startVal == null) ? 1L : startVal.longValue();
        final long step = (stepVal == null) ? 1L : stepVal.longValue();
        final boolean partition_range = (columnNames != null);
        if (step == 0) {
            throw ApplibResourceObject.get().IncrementByMustNotBeZero.ex();
        }

        ResultSetMetaData meta_data;
        final int columns;
        try {
            meta_data = inputSet.getMetaData();
            columns = meta_data.getColumnCount();
        } catch (SQLException e) {
            throw ApplibResourceObject.get().CannotGetMetaData.ex(e);
        }

        try {

            List<String> names = new ArrayList<String>();
            List<Object> current_partition = null;
            long counter = start - step;
            while (inputSet.next()) {
                int i = 1;
                for (; i <= columns; ++i) {
                    resultInserter.setObject(i, inputSet.getObject(i));
                    if (names.size() < columns) {
                        names.add(meta_data.getColumnName(i));
                    }
                }

                if (partition_range) {
                    if (current_partition == null) {
                        current_partition = getPartitionValues(inputSet,
                                columnNames);
                    }
                    List<Object> partition = getPartitionValues(inputSet,
                            columnNames);
                    int compare = compareKeys(current_partition, partition);
                    if (compare > 0) {
                        // row is out of order
                        throw ApplibResourceObject.get().InputRowsNotSorted.ex(
                                columnNames.toString(),
                                names.toString());
                    } else if (compare < 0) {
                        // new key value
                        counter = start;
                        current_partition = getPartitionValues(inputSet,
                                columnNames);
                    } else {
                        // same
                        counter += step;
                    }
                } else {
                    counter += step;
                }

                resultInserter.setLong(i, counter);
                resultInserter.executeUpdate();
            }

        } catch (SQLException e) {
            throw ApplibResourceObject.get().DatabaseAccessError.ex(
                    e.toString(), e);
        }
    }

    /**
     * Gets all the values in the first row of the set for a list of
     * given column names.
     * @param inputSet - set of values to draw from.
     * @param columnNames - set of column names for the set.
     * @return set of the objects in order of columns.
     */
    private static List<Object> getPartitionValues(
            ResultSet inputSet,
            List<String> columnNames
            )
        throws SQLException
    {
        List<Object> vals = new ArrayList<Object>();
        for (String name : columnNames) {
            vals.add(inputSet.getObject(name));
        }
        return vals;
    }

    /**
     * Compares two equal-length sets of keys key-by-key.
     * @param key_group1 - Left-hand keys
     * @param key_group2 - Right-hand keys
     * @return 0 if the sets are equivalent, -1 if the RHS is ordered
     * before the LHS, 1 if the RHS comes after the LHS.
     */
    private static int compareKeys(
            List<Object> key_group1,
            List<Object> key_group2
            )
    {
        final int len1 = key_group1.size();
        final int len2 = key_group2.size();
        assert (len1 == len2);

        for (int i = 0; i < len1; ++i) {
            int compare = FarragoSyslibUtil.compareKeysUsingGroupBySemantics(
                    key_group1.get(i),
                    key_group2.get(i));
            if (compare != 0) {
                return compare;
            }
        }
        return 0;
    }

}

// End GenerateSequence.java
