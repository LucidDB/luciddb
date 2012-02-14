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
import java.util.*;

/**
 * Returns the current local date/time as a timestamp
 *
 * @author Elizabeth Lin
 * @version $Id$
 */
public abstract class GetCurrentTimeUdf
{
    public static Timestamp execute()
    {
        Calendar cal = new GregorianCalendar();
        // TODO: We're ahead by 7 hours always regardless of what time zone is 
        // being set   
        cal.roll(Calendar.HOUR, -7);
        return new Timestamp(cal.getTimeInMillis());
    }
}

// End GetCurrentTimeUdf.java
