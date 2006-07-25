/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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
package org.eigenbase.sql;

import java.util.*;

import org.eigenbase.util.*;


/**
 * A class that describes how many operands an operator can take.
 *
 * @author Wael Chatila
 * @version $Id$
 */
public class SqlOperandCountRange
{

    //~ Static fields/initializers ---------------------------------------------

    // common usage instances
    public static final SqlOperandCountRange Variadic =
        new SqlOperandCountRange();
    public static final SqlOperandCountRange Zero = new SqlOperandCountRange(0);
    public static final SqlOperandCountRange ZeroOrOne =
        new SqlOperandCountRange(0, 1);
    public static final SqlOperandCountRange One = new SqlOperandCountRange(1);
    public static final SqlOperandCountRange OneOrTwo =
        new SqlOperandCountRange(1, 2);
    public static final SqlOperandCountRange Two = new SqlOperandCountRange(2);
    public static final SqlOperandCountRange TwoOrThree =
        new SqlOperandCountRange(2, 3);
    public static final SqlOperandCountRange Three =
        new SqlOperandCountRange(3);
    public static final SqlOperandCountRange ThreeOrFour =
        new SqlOperandCountRange(3, 4);
    public static final SqlOperandCountRange Four = new SqlOperandCountRange(4);

    //~ Instance fields --------------------------------------------------------

    private List<Integer> possibleList;
    private boolean isVariadic;

    //~ Constructors -----------------------------------------------------------

    /**
     * This constructor should only be called internally from this class and
     * only when creating a variadic count descriptor
     */
    private SqlOperandCountRange()
    {
        possibleList = null;
        isVariadic = true;
    }

    private SqlOperandCountRange(Integer [] possibleCounts)
    {
        this(Arrays.asList(possibleCounts));
    }

    public SqlOperandCountRange(int count)
    {
        this(new Integer[] { new Integer(count) });
    }

    public SqlOperandCountRange(List<Integer> list)
    {
        possibleList = Collections.unmodifiableList(list);
        isVariadic = false;
    }

    public SqlOperandCountRange(
        int count1,
        int count2)
    {
        this(new Integer[] { new Integer(count1), new Integer(count2) });
    }

    public SqlOperandCountRange(
        int count1,
        int count2,
        int count3)
    {
        this(
            new Integer[] {
                new Integer(count1),
            new Integer(count2),
            new Integer(count3)
            });
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Returns a list of allowed operand counts for a non-variadic operator.
     *
     * @return unmodifiable list of Integer
     *
     * @pre !isVariadic()
     */
    public List<Integer> getAllowedList()
    {
        Util.pre(!isVariadic, "!isVariadic");
        return possibleList;
    }

    /**
     * @return true if any number of operands is allowed
     */
    public boolean isVariadic()
    {
        return isVariadic;
    }
}

// End SqlOperandCountRange.java
