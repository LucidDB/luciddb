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
package net.sf.farrago.rng;

import java.sql.*;

import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.rngmodel.*;
import net.sf.farrago.rngmodel.rngschema.*;

import net.sf.farrago.jdbc.FarragoJdbcUtil;
import net.sf.farrago.jdbc.engine.*;
import net.sf.farrago.catalog.*;
import net.sf.farrago.util.*;
import net.sf.farrago.trace.*;
import net.sf.farrago.session.*;

import java.io.*;
import java.util.*;
import java.util.logging.*;

import javax.jmi.reflect.*;

import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.util.*;

/**
 * FarragoRngUDR contains implementations for the user-defined routine
 * portion of the RNG plugin example.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class FarragoRngUDR
{
    private static final Logger tracer =
        FarragoTrace.getClassTracer(FarragoRngUDR.class);

    /**
     * Generates the next pseudo-random integer from a particular RNG.
     *
     * @param rngName name of the RNG (possibly qualified)
     *
     * @param n upper limit on generated nonnegative integer, or -1 for
     * unlimited (including negative)
     */
    public static int rng_next_int(
        String rngName,
        int n)
        throws SQLException
    {
        Connection conn = DriverManager.getConnection(
            "jdbc:default:connection");

        FarragoSession session =
            FarragoJdbcRoutineDriver.getSessionForConnection(conn);

        FarragoSessionStmtValidator stmtValidator =
            session.newStmtValidator();

        FarragoReposTxnContext txn = null;
        try {
            SqlParser sqlParser = new SqlParser(rngName);
            SqlIdentifier rngId = (SqlIdentifier) sqlParser.parseExpression();

            txn = session.getRepos().newTxnContext(true);
            txn.beginReadTxn();
            RngRandomNumberGenerator rng =
                stmtValidator.findSchemaObject(
                    rngId,
                    RngRandomNumberGenerator.class);
            return rng_next_int_internal(n, rngName, getFilename(rng));
        } catch (Throwable ex) {
            throw FarragoJdbcUtil.newSqlException(ex, tracer);
        } finally {
            if (txn != null) {
                txn.commit();
            }
        }

        // NOTE jvs 7-Apr-2005:  no need for cleanup; default connection
        // is cleaned up automatically.
    }

    /**
     * Generates the next pseudo-random integer from a particular RNG.
     *
     * @param n upper limit on generated nonnegative integer, or -1 for
     * unlimited (including negative)
     *
     * @param rngName fully-qualified name of the RNG
     *
     * @param filename name of the RNG's datafile
     *
     * @return psuedo-random integer
     */
    public static int rng_next_int_internal(
        int n,
        String rngName,
        String filename)
        throws SQLException
    {
        File file = new File(filename);
        try {
            Random random = readSerialized(file);
            int value;
            if (n < 0) {
                value = random.nextInt();
            } else {
                value = random.nextInt(n);
            }
            writeSerialized(file, random);
            return value;
        } catch (Throwable ex) {
            throw FarragoJdbcUtil.newSqlException(ex, tracer);
        }
    }

    public static RngmodelPackage getRngModelPackage(FarragoRepos repos)
    {
        return (RngmodelPackage)
            repos.getFarragoPackage().refPackage("RNGModel");
    }

    // TODO:  file lock

    public static void writeSerialized(
        File file,
        Random random)
        throws IOException
    {
        FileOutputStream fos = null;
        ObjectOutputStream oos = null;
        try {
            fos = new FileOutputStream(file);
            oos = new ObjectOutputStream(fos);
            oos.writeObject(random);
            oos.close();
            oos = null;
            fos.close();
            fos = null;
        } finally {
            Util.squelchStream(oos);
            Util.squelchStream(fos);
        }
    }

    public static Random readSerialized(
        File file)
        throws Exception
    {
        FileInputStream fis = null;
        ObjectInputStream ois = null;
        try {
            fis = new FileInputStream(file);
            ois = new ObjectInputStream(fis);
            return (Random) ois.readObject();
        } finally {
            Util.squelchStream(ois);
            Util.squelchStream(fis);
        }
    }

    static String getFilename(RngRandomNumberGenerator rng)
    {
        return FarragoProperties.instance().expandProperties(
            rng.getSerializedFile());
    }
}

// End FarragoRngUDR.java
