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
package com.lucidera.luciddb.test.udr;

import java.sql.*;

/**
 * Helper UDXs for hashjoin tests
 *
 * Ported from //bb/bb713/Test/hashjoin/
 * @author Elizabeth Lin
 * @version $Id$
 */
public abstract class HashJoinTestUdx
{
    /**
     * Ported from //bb/bb713/Test/hashjoin/testhash1.java
     */
    public static void testhash1(int numIn, PreparedStatement resultInserter)
        throws SQLException
    {
        for (int rowCount=0; rowCount < numIn; rowCount++) {
            resultInserter.setInt(1, rowCount);
            if ((rowCount % 2) == 0) {
                resultInserter.setString(2, "M");
            } else {
                resultInserter.setString(2, "F");
            }
            resultInserter.executeUpdate();
        }
    }

    /**
     * Ported from //bb/bb713/Test/hashjoin/testhash2.java
     */
    public static void testhash2(int numIn, PreparedStatement resultInserter)
        throws SQLException
    {
        for (int rowCount=0; rowCount < numIn; rowCount++) {
            resultInserter.setInt(1, rowCount);

            if ((rowCount % 3) == 0) {
                resultInserter.setString(2, "M");
            } else {
                resultInserter.setString(2, "F");
            }

            resultInserter.executeUpdate();
        }
    }

    /**
     * Ported from //bb/bb713/Test/hashjoin/hashdistrib1.java
     */
    public static void hashdistrib1(PreparedStatement resultInserter) 
        throws SQLException
    {
        for (int rowCount=0; rowCount < 100; rowCount++) {
            resultInserter.setInt(1, rowCount);

            if (rowCount < 50) {
                resultInserter.setInt(2, 1);
            } else if (rowCount < 80) {
                resultInserter.setInt(2, 50);
            } else {
                resultInserter.setInt(2, 100);
            }

            if (rowCount < 75) {
                resultInserter.setString(3, "ann");
            } else {
                resultInserter.setString(3, "tai");
            }

            resultInserter.executeUpdate();
        }
    }

    /**
     * Ported from //bb/bb713/Test/hashjoin/hashdistrib2.java
     */
    public static void hashdistrib2(PreparedStatement resultInserter) 
        throws SQLException
    {
        for (int rowCount=0; rowCount < 100; rowCount++) {
            resultInserter.setInt(1, rowCount);

            if (rowCount < 50) {
                resultInserter.setInt(2, 100);
            } else if (rowCount < 80) {
                resultInserter.setInt(2, 50);
            } else {
                resultInserter.setInt(2, 1);
            }

            if (rowCount < 70) {
                resultInserter.setString(3, "tai");
            } else {
                resultInserter.setString(3, "ann");
            }

            resultInserter.executeUpdate();
        }
    }
}

// End HashJoinTestUdx.java
