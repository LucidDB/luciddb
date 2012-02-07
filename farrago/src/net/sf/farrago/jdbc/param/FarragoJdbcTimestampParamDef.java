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
package net.sf.farrago.jdbc.param;

import java.sql.Timestamp;

import java.util.Calendar;

import org.eigenbase.util14.*;


/**
 * FarragoJdbcEngineTimestampParamDef defines a Timestamp parameter.
 *
 * <p>This class is JDK 1.4 compatible.
 *
 * @author Julian Hyde
 * @version $Id$
 */
class FarragoJdbcTimestampParamDef
    extends FarragoJdbcParamDef
{
    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a FarragoJdbcEngineTimestampParamDef.
     *
     * @param paramName Name
     * @param paramMetaData Meta data
     */
    FarragoJdbcTimestampParamDef(
        String paramName,
        FarragoParamFieldMetaData paramMetaData)
    {
        super(paramName, paramMetaData);
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoSessionStmtParamDef
    public Object scrubValue(Object x)
    {
        return scrubValue(
            x,
            Calendar.getInstance());
    }

    // implement FarragoSessionStmtParamDef
    // Converts parameters from timezone in calendar into gmt time.
    public Object scrubValue(Object x, Calendar cal)
    {
        if (x == null) {
            checkNullable();
            return x;
        }

        if (x instanceof String) {
            String s = ((String) x).trim();
            ZonelessTimestamp ts = ZonelessTimestamp.parse(s);
            if (ts == null) {
                throw newInvalidFormat(x);
            }
            return ts;
        }

        // Of the subtypes of java.util.Date,
        // only java.sql.Date and java.sql.Timestamp are OK.
        // java.sql.Time is not okay (no date information).
        if ((x instanceof Timestamp) || (x instanceof java.sql.Date)) {
            java.util.Date timestamp = (java.util.Date) x;
            ZonelessTimestamp zt = new ZonelessTimestamp();
            zt.setZonedTime(timestamp.getTime(), DateTimeUtil.getTimeZone(cal));
            return zt;
        }

        // ZonelessDatetime is not required by JDBC, but we allow it because
        // it is a convenient format to serialize values over RMI.
        // We disallow ZonelessTime for the same reasons we disallow
        // java.sql.Time above.
        if (x instanceof ZonelessTimestamp) {
            // Do not shift time - value has already been shifted.
            return x;
        } else if (x instanceof ZonelessDate) {
            long time = ((ZonelessDatetime) x).getTime();
            ZonelessTimestamp zt = new ZonelessTimestamp();
            zt.setZonelessTime(time);
            return zt;
        }

        throw newInvalidType(x);
    }
}

// End FarragoJdbcTimestampParamDef.java
