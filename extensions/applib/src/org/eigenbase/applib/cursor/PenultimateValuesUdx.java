/*
// $Id$
// Applib is a library of SQL-invocable routines for Eigenbase applications.
// Copyright (C) 2008 The Eigenbase Project
// Copyright (C) 2008 SQLstream, Inc.
// Copyright (C) 2008 DynamoBI Corporation
//
// This library is free software; you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation; either version 2.1 of the License, or (at
// your option) any later version.
//
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public
// License along with this library; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
*/
package org.eigenbase.applib.cursor;

import java.sql.*;

import java.util.*;

import net.sf.farrago.syslib.*;

import org.eigenbase.applib.resource.*;


/**
 * Takes in a sorted input table which has a timestamp column, a designated
 * column, and grouping column(s) among other columns, and returns a table with
 * the same columns as the input table, plus an additional timestamp column. One
 * row per group is outputted - it contains the next to last value change for
 * each column other than the grouping column and the timestamp column. The
 * timestamp column contains the timestamp value taken from the input row for
 * which the next to last value change occurred for the designated column. The
 * additional timestamp column is calculated by looking at the last value change
 * for the designated column and taking the timestamp from that row in the input
 * table.
 *
 * @author Elizabeth Lin
 * @version $Id$
 */
public abstract class PenultimateValuesUdx
{
    //~ Methods ----------------------------------------------------------------

    public static void execute(
        ResultSet inputSet,
        List<String> groupCols,
        List<String> designatedCols,
        PreparedStatement resultInserter)
        throws SQLException, ApplibException
    {
        int nInput = inputSet.getMetaData().getColumnCount();
        int nOutput = resultInserter.getParameterMetaData().getParameterCount();

        // validate the number of output columns is the number of input
        // columns plus one
        if (nOutput != (nInput + 1)) {
            throw ApplibResource.instance().InputOutputColumnError.ex();
        }

        // validate that designatedCols has 2 columns
        if (designatedCols.size() != 2) {
            throw ApplibResource.instance().PenValDesignatedColsMustBeTwoCols
            .ex();
        }

        int tsColIdx = inputSet.findColumn(designatedCols.get(1));

        // validate that 2nd designatedCol is a timestamp
        if (inputSet.getMetaData().getColumnType(tsColIdx)
            != java.sql.Types.TIMESTAMP)
        {
            throw ApplibResource.instance().PenValDesignatedTsColInvalidType.ex(
                designatedCols.get(1));
        }

        // validate that groupCols does not contain any of the columns in
        // designatedCols
        if (groupCols.contains(designatedCols.get(0))
            || groupCols.contains(designatedCols.get(1)))
        {
            throw ApplibResource.instance().PenValGroupColsMustNotContainDesCols
            .ex();
        }

        boolean first = true;
        PenValueKey prevKey = new PenValueKey(groupCols, inputSet);
        PenValueKey currKey = new PenValueKey(groupCols, inputSet);
        Object [] prevValues = new Object[nInput];
        Object [] penRow = new Object[nOutput];

        // grouping and non-grouping input columns
        int [] groupingCols = new int[groupCols.size()];
        int [] nongroupingCols = new int[nInput - groupCols.size()];
        int [] noCheckCols = new int[groupCols.size() + 1];
        int dColIdx = inputSet.findColumn(designatedCols.get(0)) - 1;

        // get indexes for columns which don't need to be checked
        // also get indexes of grouping and non-grouping columns
        for (int i = 0; i < groupCols.size(); i++) {
            int groupIdx = inputSet.findColumn(groupCols.get(i)) - 1;
            groupingCols[i] = groupIdx;
            noCheckCols[i] = groupIdx;
        }
        noCheckCols[noCheckCols.length - 1] = tsColIdx - 1;
        Arrays.sort(noCheckCols);
        Arrays.sort(groupingCols);
        for (int i = 0, j = 0; i < nInput; i++) {
            if (Arrays.binarySearch(groupingCols, i) < 0) {
                nongroupingCols[j] = i;
                j++;
            }
        }

        while (inputSet.next()) {
            // timestamp col value must not be null
            if (inputSet.getTimestamp(tsColIdx) == null) {
                throw ApplibResource.instance().PenValTsColMustNotBeNull.ex(
                    designatedCols.get(1));
            }

            currKey.setValue(inputSet);
            if (first) {
                setToCurrentRowValuesOrNull(
                    inputSet,
                    groupingCols,
                    nongroupingCols,
                    tsColIdx,
                    prevValues,
                    penRow);
                prevKey.setValue(currKey);
                first = false;
            } else {
                int c = currKey.compare(prevKey);
                if (c < 0) {
                    // input table out of order, currKey comes before prevKey
                    throw ApplibResource.instance().InputRowsNotSorted.ex(
                        buildSortColumnString(groupCols, designatedCols),
                        currKey.getValueString() + ", "
                        + inputSet.getTimestamp(tsColIdx).toString());
                } else if (c > 0) {
                    // currKey comes after prevKey
                    emitUdxRow(resultInserter, penRow);
                    setToCurrentRowValuesOrNull(
                        inputSet,
                        groupingCols,
                        nongroupingCols,
                        tsColIdx,
                        prevValues,
                        penRow);
                    prevKey.setValue(currKey);
                } else {
                    // currKey equals prevKey, same group
                    if (inputSet.getTimestamp(
                            tsColIdx).before(
                                    (java.sql.Timestamp)
                                    prevValues[tsColIdx - 1]))
                    {
                        // timestamp out of order
                        throw ApplibResource.instance().InputRowsNotSorted.ex(
                            buildSortColumnString(groupCols, designatedCols),
                            currKey.getValueString() + ", "
                            + inputSet.getTimestamp(tsColIdx).toString());
                    }

                    for (int i = 0; i < nInput; i++) {
                        // for all columns other than grouping columns and
                        // timestamp column
                        if (Arrays.binarySearch(noCheckCols, i) >= 0) {
                            continue;
                        }

                        // column has changed from previous value
                        if (FarragoSyslibUtil.compareKeysUsingGroupBySemantics(
                                prevValues[i],
                                inputSet.getObject(i + 1))
                            != 0)
                        {
                            penRow[i] = prevValues[i];
                            prevValues[i] = inputSet.getObject(i + 1);

                            // column is the designated column
                            if (i == dColIdx) {
                                penRow[tsColIdx - 1] = prevValues[tsColIdx - 1];
                                prevValues[tsColIdx - 1] =
                                    inputSet.getObject(tsColIdx);
                                penRow[nOutput - 1] =
                                    inputSet.getObject(tsColIdx);
                            }
                        }
                    }
                }
            }
        }
        if (!first) {
            emitUdxRow(resultInserter, penRow);
        }
    }

    private static void emitUdxRow(
        PreparedStatement resultInserter,
        Object [] row)
        throws SQLException
    {
        for (int i = 0; i < row.length; i++) {
            resultInserter.setObject(i + 1, row[i]);
        }
        resultInserter.executeUpdate();
    }

    private static void setToCurrentRowValuesOrNull(
        ResultSet inputSet,
        int [] groupingCols,
        int [] nongroupingCols,
        int tsColIdx,
        Object [] prevRow,
        Object [] penRow)
        throws SQLException
    {
        for (int i = 0, n = prevRow.length; i < n; i++) {
            prevRow[i] = inputSet.getObject(i + 1);
        }
        for (int i = 0, n = groupingCols.length; i < n; i++) {
            int idx = groupingCols[i];
            penRow[idx] = inputSet.getObject(idx + 1);
        }
        for (int i = 0, n = nongroupingCols.length; i < n; i++) {
            penRow[nongroupingCols[i]] = null;
        }
        penRow[penRow.length - 1] = inputSet.getTimestamp(tsColIdx);
    }

    private static String buildSortColumnString(
        List<String> groupCols,
        List<String> designatedCols)
    {
        StringBuilder sortCols = new StringBuilder();
        sortCols.append(groupCols);
        sortCols.deleteCharAt(0);
        sortCols.setLength(sortCols.length() - 1);
        sortCols.append(", ");
        sortCols.append(designatedCols.get(1));
        return sortCols.toString();
    }

    //~ Inner Classes ----------------------------------------------------------

    private static class PenValueKey
    {
        private int size;
        private int [] indexes;
        private Object [] value;

        protected PenValueKey(
            List<String> groupCols,
            ResultSet inputSet)
            throws SQLException
        {
            this.size = groupCols.size();
            this.value = new Object[this.size];
            this.indexes = new int[this.size];

            for (int i = 0; i < this.size; i++) {
                this.indexes[i] = inputSet.findColumn(groupCols.get(i));
                this.value[i] = null;
            }
        }

        protected void setValue(ResultSet inputSet)
            throws SQLException
        {
            for (int i = 0; i < this.size; i++) {
                this.value[i] = inputSet.getObject(this.indexes[i]);
            }
        }

        protected void setValue(PenValueKey key)
        {
            for (int i = 0; i < this.size; i++) {
                this.value[i] = key.getValue(i);
            }
        }

        protected int compare(PenValueKey key)
            throws SQLException
        {
            if (this.size != key.getSize()) {
                throw ApplibResource.instance().PenValIncomparableKeys.ex();
            }

            for (int i = 0; i < this.size; i++) {
                int c =
                    FarragoSyslibUtil.compareKeysUsingGroupBySemantics(
                        this.value[i],
                        key.getValue(i));
                if (c != 0) {
                    // objects are not equal, return comparison
                    return c;
                }
            }
            return 0;
        }

        protected Object getValue(int i)
        {
            return this.value[i];
        }

        protected Object [] getValue()
        {
            return this.value;
        }

        protected int getSize()
        {
            return this.size;
        }

        protected String getValueString()
        {
            StringBuilder valueString = new StringBuilder();
            for (int i = 0; i < this.size; i++) {
                if (i != 0) {
                    valueString.append(", ");
                }

                if (this.value[i] != null) {
                    valueString.append(this.value[i].toString());
                } else {
                    valueString.append("(null)");
                }
            }
            return valueString.toString();
        }
    }
}

// End PenultimateValuesUdx.java
