/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
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
package org.eigenbase.util;

import junit.framework.ComparisonFailure;
import java.util.regex.Pattern;

/**
 * Static utilities for JUnit tests.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class TestUtil
{
    public static void assertEqualsVerbose(
        String expected,
        String actual)
    {
        if ((expected == null) && (actual == null)) {
            return;
        }
        if ((expected != null) && expected.equals(actual)) {
            return;
        }
        String s = actual;

        // Convert [string with "quotes" split
        // across lines]
        // into ["string with \"quotes\" split" + NL +
        // "across lines
        //
        //
        s = Util.replace(s, "\"", "\\\"");
        final String lineBreak = "\" + NL + " + Util.lineSeparator + "\"";
        s = Pattern.compile("\r\n|\r|\n").matcher(s).replaceAll(lineBreak);
        s = "\"" + s + "\"";
        final String spurious = " + " + Util.lineSeparator + "\"\"";
        if (s.endsWith(spurious)) {
            s = s.substring(0, s.length() - spurious.length());
        }
        String message =
            "Expected:" + Util.lineSeparator + expected + Util.lineSeparator
            + "Actual: " + Util.lineSeparator + actual + Util.lineSeparator
            + "Actual java: " + Util.lineSeparator + s + Util.lineSeparator;
        throw new ComparisonFailure(message, expected, actual);
    }
}

// End TestUtil.java
