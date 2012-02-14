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
package net.sf.farrago.fennel.calc;

/**
 * Enumeration of aggregate operations to be performed on aggregation buckets
 * for windowed or streaming aggregation.
 *
 * @author Jack Hahn
 * @version $Id$
 * @since Feb 5, 2004
 */
public enum AggOp
{
    None,

    /**
     * Initialize bucket to zero or null values
     */
    Init,

    /**
     * Update bucket in response to new row.
     */
    Add,

    /**
     * Update bucket in response to row leaving window.
     */
    Drop,

    /**
     * Initialize bucket and update for new row.
     */
    InitAdd
}

// End AggOp.java
