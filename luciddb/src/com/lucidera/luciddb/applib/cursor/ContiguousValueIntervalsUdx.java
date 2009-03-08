/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2008-2008 LucidEra, Inc.
// Copyright (C) 2008-2008 The Eigenbase Project
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
 * This UDX takes in an input table that has partitioning column(s),
 * a timestamp column, and a clumping column. The input table must be
 * sorted in ascending order of partitioning column(s) and timestamp column.
 * The UDX output a table that has all input columns plus five extra columns.
 * Within one input partition,adjacent rows having the same value of
 * clumping column are clumped into one output row which has the value of
 * current partition, current clump, timestamps at which the clump starts
 * and ends. The output row also contains information about the value and
 * timestamp of previous and next clump within the partition. 
 *
 * @author Elizabeth Lin
 * @author Khanh Vu
 * @version $Id$
 */
public abstract class ContiguousValueIntervalsUdx
{
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
            nOutput =
                resultInserter.getParameterMetaData().getParameterCount();
        } catch (SQLException e) {
            throw ApplibResourceObject.get().InputOutputColumnError.ex(e);
        }

        // verify number of output column is five plus number of input columns
        if (nOutput != nInput + 5) {
            throw ApplibResourceObject.get().CVIInOutNumColsMismatch.ex();
        }

        // verify that timestampCol contains one column of type TIMESTAMP
        int tsColIdx = inputSet.findColumn(timestampCol.get(0));
        if ((timestampCol.size() != 1) 
            || (inputSet.getMetaData().getColumnType(tsColIdx)
                != java.sql.Types.TIMESTAMP))
        {
            throw ApplibResourceObject.get().CVIInvalidTsCol.ex();
        }

        // verify that timestampCol is not in partitionColsPenValueKey
        if (partitionCols.contains(timestampCol.get(0))) {
            throw ApplibResourceObject.get().CVITsInPartition.ex();
        }

        // verify number of columns in input set
        int numPartCols = partitionCols.size();
        if (nInput != (numPartCols + 2)) {
            throw ApplibResourceObject.get().CVIInvalidInputTable.ex();
        }

        // verify that clumping column is of type CHAR or VARCHAR
        int [] partitionColIdx = new int[numPartCols];
        for (int i = 0; i < numPartCols; i++) {
            partitionColIdx[i] = inputSet.findColumn(partitionCols.get(i));
        }
        int clumpColIdx = -1;
        for (int i = 1; i < nInput+1; i++) {
            if ((i != tsColIdx) && 
                (Arrays.binarySearch(partitionColIdx, i) < 0)) 
            {
                clumpColIdx = i;
                break;
            }
        }
        if (clumpColIdx == -1) {
            //should never get here
            throw ApplibResourceObject.get().CVIInvalidInputTable.ex();
        }
        if (!((inputSet.getMetaData().getColumnType(clumpColIdx) ==
                    java.sql.Types.VARCHAR)
                || (inputSet.getMetaData().getColumnType(clumpColIdx) ==
                    java.sql.Types.CHAR)))
        {
            throw ApplibResourceObject.get().InvalidColumnDatatype.ex(
                "CLUMPING COLUMN", "CHAR or VARCHAR");
        }

        boolean first = true;
        Object [] currPart = new Object[numPartCols];
        Timestamp currTS = null;
        Object [] outputRow = new Object[nOutput];

        // index in outputRow of
        // UNTIL_TIMESATMP = nInput
        // PREV_CLUMP = nInput + 1
        // PREV_FROM_TIMESTAMP = nInput + 2
        // NEXT_CLUMP = nInput + 3
        // NEXT_UNTIL_TIMESTAMP = nInput + 4

        while (inputSet.next()) {
            // timestamp column must not be null
            if (inputSet.getTimestamp(tsColIdx) == null) {
                throw ApplibResourceObject.get().CVITsMustNotBeNull.ex();
            }

            if (first) {
                copyInRowtoOutRow(outputRow, inputSet, nInput);

                outputRow[nInput] = null;
                outputRow[nInput + 1] = inputSet.getObject(clumpColIdx);
                outputRow[nInput + 2] = null;
                outputRow[nInput + 3] = inputSet.getObject(clumpColIdx);
                outputRow[nInput + 4] = null;
                setCurrentPartition(currPart, inputSet, partitionColIdx);
                currTS = inputSet.getTimestamp(tsColIdx);
                first = false;
            } else {
                if(comparePartitionColumnsUsingGroupBySemantics(
                    currPart, inputSet, partitionColIdx) > 0)
                {
                    // partitioning columns out of order
                    String colNames = new String();
                    String colValues = new String();
                    for (int i = 0; i < numPartCols; i++) {
                        colNames = colNames.concat(
                            partitionCols.get(i) + ", ");
                        colValues = colValues.concat(
                            inputSet.getObject(
                                partitionColIdx[i]).toString() + ", ");
                    }
                        colNames = colNames.replaceFirst(", $","");
                        colValues = colValues.replaceFirst(", $","");
                    throw ApplibResourceObject.get().InputRowsNotSorted.ex(
                        colNames,colValues);
                } else if (comparePartitionColumnsUsingGroupBySemantics(
                    currPart, inputSet, partitionColIdx) < 0)
                {
                    // new partition, need output row
                    currTS = inputSet.getTimestamp(tsColIdx);

                    if ((FarragoSyslibUtil.compareKeysUsingGroupBySemantics(
                            outputRow[clumpColIdx - 1],
                            outputRow[nInput + 1]) == 0) &&
                        (FarragoSyslibUtil.compareKeysUsingGroupBySemantics(
                            outputRow[clumpColIdx - 1],
                            outputRow[nInput + 3]) == 0))
                    {
                        outputRow[nInput + 1] = null;
                        outputRow[nInput + 3] = null;
                    } else {
                        emitUdxRow(resultInserter, outputRow);

                        outputRow[nInput + 1] = outputRow[clumpColIdx - 1];
                        outputRow[nInput + 2] = outputRow[tsColIdx  - 1];
                        outputRow[clumpColIdx - 1] = outputRow[nInput + 3];
                        outputRow[tsColIdx - 1] = outputRow[nInput];
                        outputRow[nInput] = null;
                        outputRow[nInput + 3] = null;
                        outputRow[nInput + 4] = null;
                    }
                    emitUdxRow(resultInserter, outputRow);

                    copyInRowtoOutRow(outputRow, inputSet, nInput);
                    outputRow[nInput] = null;
                    outputRow[nInput + 1] = inputSet.getObject(clumpColIdx);
                    outputRow[nInput + 2] = null;
                    outputRow[nInput + 3] = inputSet.getObject(clumpColIdx);
                    outputRow[nInput + 4] = null;
                    setCurrentPartition(currPart, inputSet, partitionColIdx);
                } else {
                    // partition not changed
                    if (inputSet.getTimestamp(tsColIdx).before(currTS)) {
                        // timestamp column out of order
                        throw ApplibResourceObject.get().InputRowsNotSorted.ex(
                            inputSet.getMetaData().getColumnName(tsColIdx),
                            inputSet.getTimestamp(tsColIdx).toString());
                    } else {
                        currTS = inputSet.getTimestamp(tsColIdx);
                    }

                    if (FarragoSyslibUtil.compareKeysUsingGroupBySemantics(
                            inputSet.getObject(clumpColIdx),
                            outputRow[nInput + 3]) != 0)
                    {
                        // new clump, need output row
                        if ((FarragoSyslibUtil.compareKeysUsingGroupBySemantics(
                            outputRow[clumpColIdx - 1],
                            outputRow[nInput + 1]) == 0) &&
                        (FarragoSyslibUtil.compareKeysUsingGroupBySemantics(
                            outputRow[clumpColIdx - 1],
                            outputRow[nInput + 3]) == 0))
                        {
                            outputRow[nInput + 1] = null;
                            outputRow[nInput] =
                                inputSet.getObject(tsColIdx);
                            outputRow[nInput + 3] =
                                inputSet.getObject(clumpColIdx);
                            outputRow[nInput + 4] = null;
                        } else {
                            outputRow[nInput + 4] =
                                inputSet.getObject(tsColIdx);

                            emitUdxRow(resultInserter, outputRow);

                            outputRow[nInput + 1] =
                                outputRow[clumpColIdx - 1];
                            outputRow[nInput + 2] =
                                outputRow[tsColIdx  - 1];
                            outputRow[clumpColIdx - 1] =
                                outputRow[nInput + 3];
                            outputRow[tsColIdx  - 1] =
                                outputRow[nInput];
                            outputRow[nInput] =
                                outputRow[nInput  + 4];
                            outputRow[nInput  + 3] =
                                inputSet.getObject(clumpColIdx);
                            outputRow[nInput  + 4] = null;
                        }
                    }
                }
            }
        }

        if (!first) {
            if ((FarragoSyslibUtil.compareKeysUsingGroupBySemantics(
                outputRow[clumpColIdx - 1],
                outputRow[nInput + 1]) == 0) &&
            (FarragoSyslibUtil.compareKeysUsingGroupBySemantics(
                outputRow[clumpColIdx - 1],
                outputRow[nInput + 3]) == 0))
            {
                outputRow[nInput + 1] = null;
                outputRow[nInput + 3] = null;
            } else {
                emitUdxRow(resultInserter, outputRow);

                outputRow[nInput + 1] = outputRow[clumpColIdx - 1];
                outputRow[nInput + 2] = outputRow[tsColIdx  - 1];
                outputRow[clumpColIdx - 1] = outputRow[nInput + 3];
                outputRow[tsColIdx - 1] = outputRow[nInput];
                outputRow[nInput] = null;
                outputRow[nInput + 3] = null;
                outputRow[nInput + 4] = null;
            }
            emitUdxRow(resultInserter, outputRow);
        }
    }

    private static void copyInRowtoOutRow(
        Object [] outputRow, ResultSet inputSet, int numCol)
        throws SQLException
    {
        for (int i = 0; i < numCol; i++) {
            outputRow[i] = inputSet.getObject(i+1);
        }
    }

    private static void setCurrentPartition(
        Object [] currPart, ResultSet inputSet, int [] partColIdx)
        throws SQLException
    {
        int n = partColIdx.length;
        for (int i = 0; i < n; i++) {
            currPart[i] = inputSet.getObject(partColIdx[i]);
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
        Object [] currPart, ResultSet inputSet, int [] partColIdx)
        throws SQLException
    {
        int n = partColIdx.length;
        for (int i = 0; i < n; i++) {
            int comp = FarragoSyslibUtil.compareKeysUsingGroupBySemantics(
                currPart[i], inputSet.getObject(partColIdx[i]));
            if (comp != 0) {
                return comp;
            }
        }
        return 0;
    }
}

// End ContiguousValueIntervalsUdx.java
