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

/**
 * FennelJavaStreamMap is needed when a Fennel TupleStream's definition includes
 * calls to JavaTupleStreams.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FennelJavaStreamMap
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Looks up the handle of a JavaTupleStream by its ID. This is called by
     * native code when a TupleStream is opened. The ID is a placeholder in the
     * TupleStream definition; each open may result in a different handle.
     *
     * @param streamId ID of stream to find
     *
     * @return JavaTupleStream handle
     */
    public long getJavaStreamHandle(int streamId);

    /**
     * Looks up the root PageId of an index. This is called by native code when
     * a TupleStream accessing a temporary BTree is opened.
     *
     * @param pageOwnerId the identifier for the index
     */
    public long getIndexRoot(long pageOwnerId);
}

// End FennelJavaStreamMap.java
