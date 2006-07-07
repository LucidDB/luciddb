/*
// $Id$
// Farrago is an extensible data management system.
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
package com.lucidera.opt;

import org.eigenbase.rel.JoinRelType;

/**
 * Enumeration of LucidDB Hash Join types.
 *
 * @author Rushan Chen
 * @version $Id$
 */
public enum LhxJoinRelType
{
    INNER(JoinRelType.INNER),
    LEFT(JoinRelType.LEFT),
    RIGHT(JoinRelType.RIGHT),
    FULL(JoinRelType.FULL),
    LEFTSEMI(null),
    RIGHTANTI(null);

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
