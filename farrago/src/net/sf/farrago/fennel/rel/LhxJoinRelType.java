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
package net.sf.farrago.fennel.rel;

import org.eigenbase.rel.*;


/**
 * Enumeration of LucidDB Hash Join types.
 *
 * @author Rushan Chen
 * @version $Id$
 */
public enum LhxJoinRelType
{
    INNER(JoinRelType.INNER), LEFT(JoinRelType.LEFT), RIGHT(JoinRelType.RIGHT),
    FULL(JoinRelType.FULL), LEFTSEMI(null), RIGHTSEMI(null), RIGHTANTI(null);

    private final JoinRelType logicalJoinType;

    LhxJoinRelType(JoinRelType logicalJoinType)
    {
        this.logicalJoinType = logicalJoinType;
    }

    public JoinRelType getLogicalJoinType()
    {
        return logicalJoinType;
    }

    public static LhxJoinRelType getLhxJoinType(JoinRelType logicalJoinType)
    {
        if (logicalJoinType == JoinRelType.FULL) {
            return FULL;
        } else if (logicalJoinType == JoinRelType.LEFT) {
            return LEFT;
        } else if (logicalJoinType == JoinRelType.RIGHT) {
            return RIGHT;
        } else {
            assert (logicalJoinType == JoinRelType.INNER);
            return INNER;
        }
    }
}

// End LhxJoinRelType.java
