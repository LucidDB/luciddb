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
package net.sf.farrago.fennel;

import java.nio.*;


/**
 * FennelJavaErrorTarget represents a class of java objects that can handle row
 * errors arising from Fennel streams.
 *
 * @author John Pham
 * @version $Id$
 */
public interface FennelJavaErrorTarget
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Handles a Fennel row exception
     *
     * @param source the unique Fennel stream name
     * @param isWarning true if the exception is only a warning
     * @param msg the exception string
     * @param byteBuffer the Fennel format byte buffer containing an error
     * record for the row that failed. The error record must conform to the row
     * type specified for the source with {@link
     * net.sf.farrago.query.FennelRelImplementor#setErrorRecordType}
     * @param index position of the column whose processing caused the exception
     * to occur. -1 indicates that no column was culpable. 0 indicates that a
     * filter condition was being processed. Otherwise this parameter should be
     * a 1-indexed column position.
     */
    public Object handleRowError(
        String source,
        boolean isWarning,
        String msg,
        ByteBuffer byteBuffer,
        int index);
}

// End FennelJavaErrorTarget.java
