/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2006 The Eigenbase Project
// Copyright (C) 2002-2006 Disruptive Tech
// Copyright (C) 2005-2006 LucidEra, Inc.
// Portions Copyright (C) 2003-2006 John V. Sichi
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

package org.eigenbase.util14;

import java.text.DecimalFormat;
import java.text.NumberFormat;

/**
 * Utility functions for converting from one type to another
*
 * @author angel
 * @version $Id$
 * @since Jan 22, 2006
 */
public class ConversionUtil {

    //~ Methods ---------------------------------------------------------------

    /**
     * Converts a byte array into a bit string or a hex string.
     *
     * <p>For example,
     * <code>toStringFromByteArray(new byte[] {0xAB, 0xCD}, 16)</code> returns
     * <code>ABCD</code>.
     */
    public static String toStringFromByteArray(
        byte [] value,
        int radix)
    {
        assert (2 == radix) || (16 == radix) : "Make sure that the algorithm below works for your radix";
        if (0 == value.length) {
            return "";
        }

        int trick = radix * radix;
        StringBuffer ret = new StringBuffer();
        for (int i = 0; i < value.length; i++) {
            ret.append(
                Integer.toString(trick | (0x0ff & value[i]), radix).substring(1));
        }

        return ret.toString().toUpperCase();
    }

    /**
     * Converts an approximate value into a string, following the SQL 2003
     * standard.
     */
    public static String toStringFromApprox(double d, boolean isFloat)
    {
        NumberFormat nf = NumberUtil.getApproxFormatter(isFloat);
        return nf.format(d);
    }


}

// End ConversionUtil.java