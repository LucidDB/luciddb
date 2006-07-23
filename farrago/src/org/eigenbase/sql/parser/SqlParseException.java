/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2004-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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
package org.eigenbase.sql.parser;

import java.util.*;

import org.eigenbase.util14.*;


/**
 * SqlParseException defines a checked exception corresponding to {@link
 * SqlParser}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class SqlParseException
    extends Exception
    implements EigenbaseParserException
{

    //~ Instance fields --------------------------------------------------------

    private final SqlParserPos pos;
    private final int [][] expectedTokenSequences;
    private final String [] tokenImages;

    /**
     * The original exception thrown by the generated parser. Unfortunately,
     * each generated parser throws exceptions of a different class. So, we keep
     * the exception for forensic purposes, but don't print it publicly.
     */
    private final Throwable parserException;

    //~ Constructors -----------------------------------------------------------

    public SqlParseException(
        String message,
        SqlParserPos pos,
        int [][] expectedTokenSequences,
        String [] tokenImages,
        Throwable parserException)
    {
        super(message, parserException);
        this.pos = pos;
        this.expectedTokenSequences = expectedTokenSequences;
        this.tokenImages = tokenImages;
        this.parserException = parserException;
    }

    //~ Methods ----------------------------------------------------------------

    public SqlParserPos getPos()
    {
        return pos;
    }

    /**
     * Returns a list of the token names which could have legally occurred at
     * this point.
     */
    public Collection getExpectedTokenNames()
    {
        final TreeSet set = new TreeSet();
        for (int i = 0; i < expectedTokenSequences.length; i++) {
            int [] expectedTokenSequence = expectedTokenSequences[i];
            set.add(tokenImages[expectedTokenSequence[0]]);
        }
        return set;
    }

    public String [] getTokenImages()
    {
        return tokenImages;
    }

    public int [][] getExpectedTokenSequences()
    {
        return expectedTokenSequences;
    }
}

// End SqlParseException.java
