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
package org.eigenbase.applib.cursor;

import java.sql.*;

import java.util.*;

import net.sf.farrago.syslib.*;

import org.eigenbase.applib.resource.*;


/**
 * This UDX takes in an input table that has partitioning column(s), a timestamp
 * column, and a clumping column. The input table must be sorted in ascending
 * order of partitioning column(s) and timestamp column. The UDX output a table
 * that has all input columns plus five extra columns. Within one input
 * partition,adjacent rows having the same value of clumping column are clumped
 * into one output row which has the value of current partition, current clump,
 * timestamps at which the clump starts and ends. The output row also contains
 * information about the value and timestamp of previous and next clump within
 * the partition.
 *
 * @author Elizabeth Lin
 * @author Khanh Vu
 * @version $Id$
 */
public abstract class ContiguousValueIntervalsUdx
{
    //~ Methods ----------------------------------------------------------------

    public static void execute(
        ResultSet inputSet,
        List<String> partitionCols,
        List<String> timestampCol,
        PreparedStatement resultInserter)
        throws SQLException, ApplibException
    {
        int nInput, nOutput;
        try {
            nInput = inputSet.getMetaData().getColumnCount();
            nOutput = resultInserter.getParameterMetaData().getParameterCount();
        } catch (SQLException e) {
            throw ApplibResource.instance().InputOutputColumnError.ex(e);
        }

        // verify number of output column is five plus number of input columns
        if (nOutput != (nInput + 5)) {
            throw ApplibResource.instance().CVIInOutNumColsMismatch.ex();
        }

        // verify that timestampCol contains one column of type TIMESTAMP
        int sqlTSColIdx = inputSet.findColumn(timestampCol.get(0));
        if ((timestampCol.size() != 1)
            || (inputSet.getMetaData().getColumnType(sqlTSColIdx)
                != java.sql.Types.TIMESTAMP))
        {
            throw ApplibResource.instance().CVIInvalidTsCol.ex();
        }

        // verify that timestampCol is not in partitionColsPenValueKey
        if (partitionCols.contains(timestampCol.get(0))) {
            throw ApplibResource.instance().CVITsInPartition.ex();
        }

        // verify number of columns in input set
        int numPartCols = partitionCols.size();
        if (nInput != (numPartCols + 2)) {
            throw ApplibResource.instance().CVIInvalidInputTable.ex();
        }

        // verify that clumping column is of type CHAR or VARCHAR
        int [] sqlPartitionColIdx = new int[numPartCols];
        for (int i = 0; i < numPartCols; i++) {
            sqlPartitionColIdx[i] = inputSet.findColumn(partitionCols.get(i));
        }
        int sqlClumpColIdx = -1;
        for (int i = 1; i < (nInput + 1); i++) {
            if ((i != sqlTSColIdx)
                && (Arrays.binarySearch(sqlPartitionColIdx, i) < 0))
            {
                // sqlClumpColIdx is 1 based column idx in sql
                sqlClumpColIdx = i;
                break;
            }
        }
        if (sqlClumpColIdx == -1) {
            //should never get here
            throw ApplibResource.instance().CVIInvalidInputTable.ex();
        }
        if (!((inputSet.getMetaData().getColumnType(sqlClumpColIdx)
                    == java.sql.Types.VARCHAR)
                || (inputSet.getMetaData().getColumnType(sqlClumpColIdx)
                    == java.sql.Types.CHAR)))
        {
            throw ApplibResource.instance().InvalidColumnDatatype.ex(
                "CLUMPING COLUMN",
                "CHAR or VARCHAR");
        }

        boolean first = true;
        Object [] currPart = new Object[numPartCols];
        Timestamp currTS = null;
        Object [] outputRow = new Object[nOutput];

        final int untilTSColIdx = nInput;
        final int prevClumpColIdx = nInput + 1;
        final int prevFromTSColIdx = nInput + 2;
        final int nextClumpColIdx = nInput + 3;
        final int nextUntilTSColIdx = nInput + 4;
        final int clumpColIdx = sqlClumpColIdx - 1;
        final int tsColIdx = sqlTSColIdx - 1;

        while (inputSet.next()) {
            // timestamp column must not be null
            if (inputSet.getTimestamp(sqlTSColIdx) == null) {
                throw ApplibResource.instance().CVITsMustNotBeNull.ex();
            }

            if (first) {
                copyInRowNResetOutputRow(
                    outputRow,
                    currPart,
                    inputSet,
                    nInput,
                    sqlClumpColIdx,
                    sqlPartitionColIdx);
                currTS = inputSet.getTimestamp(sqlTSColIdx);
                first = false;
            } else {
                int c =
                    comparePartitionColumnsUsingGroupBySemantics(
                        currPart,
                        inputSet,
                        sqlPartitionColIdx);
                if (c > 0) {
                    // partitioning columns out of order
                    String colNames = new String();
                    String colValues = new String();
                    for (int i = 0; i < numPartCols; i++) {
                        colNames =
                            colNames.concat(
                                partitionCols.get(i) + ", ");
                        colValues =
                            colValues.concat(
                                inputSet.getObject(
                                    sqlPartitionColIdx[i]).toString() + ", ");
                    }
                    colNames = colNames.replaceFirst(", $", "");
                    colValues = colValues.replaceFirst(", $", "");
                    throw ApplibResource.instance().InputRowsNotSorted.ex(
                        colNames,
                        colValues);
                } else if (c < 0) {
                    // new partition, need output row
                    currTS = inputSet.getTimestamp(sqlTSColIdx);

                    if (isOutputRowClumpsSame(
                            outputRow[prevClumpColIdx],
                            outputRow[clumpColIdx],
                            outputRow[nextClumpColIdx]))
                    {
                        outputRow[prevClumpColIdx] = null;
                        outputRow[nextClumpColIdx] = null;
                    } else {
                        emitUdxRow(resultInserter, outputRow);

                        outputRow[prevClumpColIdx] = outputRow[clumpColIdx];
                        outputRow[prevFromTSColIdx] = outputRow[tsColIdx];
                        outputRow[clumpColIdx] = outputRow[nextClumpColIdx];
                        outputRow[tsColIdx] = outputRow[untilTSColIdx];
                        outputRow[untilTSColIdx] = null;
                        outputRow[nextClumpColIdx] = null;
                        outputRow[nextUntilTSColIdx] = null;
                    }
                    emitUdxRow(resultInserter, outputRow);

                    copyInRowNResetOutputRow(
                        outputRow,
                        currPart,
                        inputSet,
                        nInput,
                        sqlClumpColIdx,
                        sqlPartitionColIdx);
                } else {
                    // partition not changed
                    if (inputSet.getTimestamp(sqlTSColIdx).before(currTS)) {
                        // timestamp column out of order
                        throw ApplibResource.instance().InputRowsNotSorted.ex(
                            inputSet.getMetaData().getColumnName(sqlTSColIdx),
                            inputSet.getTimestamp(sqlTSColIdx).toString());
                    } else {
                        currTS = inputSet.getTimestamp(sqlTSColIdx);
                    }

                    if (FarragoSyslibUtil.compareKeysUsingGroupBySemantics(
                            inputSet.getObject(sqlClumpColIdx),
                            outputRow[nextClumpColIdx])
                        != 0)
                    {
                        // new clump, need output row
                        if (isOutputRowClumpsSame(
                                outputRow[prevClumpColIdx],
                                outputRow[clumpColIdx],
                                outputRow[nextClumpColIdx]))
                        {
                            outputRow[prevClumpColIdx] = null;
                            outputRow[untilTSColIdx] =
                                inputSet.getObject(sqlTSColIdx);
                            outputRow[nextClumpColIdx] =
                                inputSet.getObject(sqlClumpColIdx);
                            outputRow[nextUntilTSColIdx] = null;
                        } else {
                            outputRow[nextUntilTSColIdx] =
                                inputSet.getObject(sqlTSColIdx);

                            emitUdxRow(resultInserter, outputRow);

                            outputRow[prevClumpColIdx] = outputRow[clumpColIdx];
                            outputRow[prevFromTSColIdx] = outputRow[tsColIdx];
                            outputRow[clumpColIdx] = outputRow[nextClumpColIdx];
                            outputRow[tsColIdx] = outputRow[untilTSColIdx];
                            outputRow[untilTSColIdx] =
                                outputRow[nextUntilTSColIdx];
                            outputRow[nextClumpColIdx] =
                                inputSet.getObject(sqlClumpColIdx);
                            outputRow[nextUntilTSColIdx] = null;
                        }
                    }
                }
            }
        }

        if (!first) {
            if (isOutputRowClumpsSame(
                    outputRow[prevClumpColIdx],
                    outputRow[clumpColIdx],
                    outputRow[nextClumpColIdx]))
            {
                outputRow[prevClumpColIdx] = null;
                outputRow[nextClumpColIdx] = null;
            } else {
                emitUdxRow(resultInserter, outputRow);

                outputRow[prevClumpColIdx] = outputRow[clumpColIdx];
                outputRow[prevFromTSColIdx] = outputRow[tsColIdx];
                outputRow[clumpColIdx] = outputRow[nextClumpColIdx];
                outputRow[tsColIdx] = outputRow[untilTSColIdx];
                outputRow[untilTSColIdx] = null;
                outputRow[nextClumpColIdx] = null;
                outputRow[nextUntilTSColIdx] = null;
            }
            emitUdxRow(resultInserter, outputRow);
        }
    }

    private static void copyInRowNResetOutputRow(
        Object [] outputRow,
        Object [] currentPartition,
        ResultSet inputSet,
        int nInput,
        int sqlClumpColIdx,
        int [] sqlPartitionColIdx)
        throws SQLException
    {
        for (int i = 0; i < nInput; i++) {
            outputRow[i] = inputSet.getObject(i + 1);
        }

        outputRow[nInput] = null;
        outputRow[nInput + 1] = inputSet.getObject(sqlClumpColIdx);
        outputRow[nInput + 2] = null;
        outputRow[nInput + 3] = inputSet.getObject(sqlClumpColIdx);
        outputRow[nInput + 4] = null;

        final int n = sqlPartitionColIdx.length;
        for (int i = 0; i < n; i++) {
            currentPartition[i] = inputSet.getObject(sqlPartitionColIdx[i]);
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

    private static int comparePartitionColumnsUsingGroupBySemantics(
        Object [] currPart,
        ResultSet inputSet,
        int [] sqlPartColIdx)
        throws SQLException
    {
        int n = sqlPartColIdx.length;
        for (int i = 0; i < n; i++) {
            int comp =
                FarragoSyslibUtil.compareKeysUsingGroupBySemantics(
                    currPart[i],
                    inputSet.getObject(sqlPartColIdx[i]));
            if (comp != 0) {
                return comp;
            }
        }
        return 0;
    }

    private static boolean isOutputRowClumpsSame(
        Object prevClump,
        Object thisClump,
        Object nextClump)
        throws SQLException
    {
        return (FarragoSyslibUtil.compareKeysUsingGroupBySemantics(
            prevClump,
            thisClump) == 0)
            && (FarragoSyslibUtil.compareKeysUsingGroupBySemantics(
                    thisClump,
                    nextClump) == 0);
    }
}

// End ContiguousValueIntervalsUdx.java
