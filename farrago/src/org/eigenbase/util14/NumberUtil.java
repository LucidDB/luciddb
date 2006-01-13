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

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Utility functions for working with numbers
 *
 * @author angel
 * @version $Id$
 * @since Jan 9, 2006
 */
public class NumberUtil {

    public static final BigInteger getMaxUnscaled(int precision)
    {
        if (precision < 19) {
            BigInteger temp = BigInteger.TEN.pow(precision);
            return temp.subtract(BigInteger.ONE);
        } else {
            return BigInteger.valueOf(Long.MAX_VALUE);
        }
    }

    public static final BigInteger getMinUnscaled(int precision)
    {
        if (precision < 19) {
            BigInteger temp = BigInteger.TEN.pow(precision);
            return temp.subtract(BigInteger.ONE).negate();
        } else {
            return BigInteger.valueOf(Long.MIN_VALUE);
        }
    }

    public static final BigDecimal rescaleBigDecimal(BigDecimal bd, int scale)
    {
        if (bd != null) {
            bd = bd.setScale(scale, BigDecimal.ROUND_HALF_UP);
        }
        return bd;
    }

    public static final BigDecimal toBigDecimal(Number number, int scale)
    {
        BigDecimal bd = toBigDecimal(number);
        return rescaleBigDecimal(bd, scale);
    }

    public static final BigDecimal toBigDecimal(Number number)
    {
        if (number == null) {
            return null;
        } if (number instanceof BigDecimal) {
            return (BigDecimal) number;
        } else if ((number instanceof Double)
                || (number instanceof Float)) {
            return BigDecimal.valueOf(((Number) number).doubleValue());
        } else if (number instanceof BigInteger) {
            return new BigDecimal((BigInteger) number);
        } else {
            return new BigDecimal(((Number) number).longValue());
        }
    }


}

// End NumberUtil.java