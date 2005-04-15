/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2004-2005 John V. Sichi
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
package net.sf.farrago.rng;

import java.sql.*;

import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.rngmodel.*;
import net.sf.farrago.rngmodel.rngschema.*;

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
     * @param n upper limit on generated integer
     *
     * @return psuedo-random integer in the range [0, n)
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

        try {
            SqlParser sqlParser = new SqlParser(rngName);
            SqlIdentifier rngId = (SqlIdentifier) sqlParser.parseExpression();
            
            RngmodelPackage rngPkg =
                getRngModelPackage(session.getRepos());
            RefClass refRngClass =
                rngPkg.getRngschema().getRngRandomNumberGenerator();
            RngRandomNumberGenerator rng = (RngRandomNumberGenerator)
                stmtValidator.findSchemaObject(
                    rngId,
                    refRngClass);
            Random random = readSerialized(rng);
            int value = random.nextInt(n);
            writeSerialized(rng, random);
            return value;
        } catch (Throwable ex) {
            throw FarragoUtil.newSqlException(ex, tracer);
        }

        // NOTE jvs 7-Apr-2005:  no need for cleanup; default connection
        // is cleaned up automatically.
    }

    // TODO:  share with parser
    private static RngmodelPackage getRngModelPackage(FarragoRepos repos)
    {
        return (RngmodelPackage)
            repos.getFarragoPackage().refPackage("RNGModel");
    }

    // TODO:  file lock
    
    public static void writeSerialized(
        RngRandomNumberGenerator rng,
        Random random)
        throws IOException
    {
        File file = getFile(rng);
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
        RngRandomNumberGenerator rng)
        throws Exception
    {
        File file = getFile(rng);
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

    private static File getFile(RngRandomNumberGenerator rng)
    {
        return new File(
            FarragoProperties.instance().expandProperties(
                rng.getSerializedFile()));
    }
}

// End FarragoRngUDR.java
