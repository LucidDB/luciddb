/*
// $Id$
// LucidDB is a DBMS optimized for business intelligence.
// Copyright (C) 2006-2006 LucidEra, Inc.
// Copyright (C) 2006-2006 The Eigenbase Project
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
package com.lucidera.luciddb.test.udr;

import java.sql.*;

/**
 * Helper UDXs for hashjoin tests
 *
 * Ported from //bb/bb713/Test/hashjoin/
 * @author Elizabeth Lin
 * @version $Id$
 */
class HashJoinTestUdx
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
