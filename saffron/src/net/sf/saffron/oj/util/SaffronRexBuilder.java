/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2003-2003 Disruptive Technologies, Inc.
// You must accept the terms in LICENSE.html to use this software.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/
package net.sf.saffron.oj.util;

import net.sf.saffron.core.SaffronTypeFactory;
import net.sf.saffron.rex.RexLiteral;
import net.sf.saffron.sql.type.SqlTypeName;
import net.sf.saffron.util.NlsString;

import java.math.BigDecimal;

/**
 * Extension to {@link JavaRexBuilder} where everything is created with a Java
 * type.
 *
 * @author Julian Hyde, Wael Chatila 
 * @since June 2nd, 2004
 * @version $Id$
 */
public class SaffronRexBuilder extends JavaRexBuilder {
    /**
     * Creates a SaffronRexBuilder
     */
    public SaffronRexBuilder(SaffronTypeFactory typeFactory) {
        super(typeFactory);
    }

    public RexLiteral makeLiteral(boolean b) {
        return makeLiteral(b ? Boolean.TRUE : Boolean.FALSE,
                _typeFactory.createJavaType(boolean.class),
                SqlTypeName.Boolean);
    }

    public RexLiteral makeExactLiteral(BigDecimal bd) {
        // Use int for an integer (e.g. -52),
        // long for an integer larger than maxint,
        // double for a fraction (e.g. 3.14).
        Class clazz;
        if (bd.scale() > 0) {
            clazz = double.class;
        } else {
            long l = bd.longValue();
            if (l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE) {
                clazz = int.class;
            } else {
                clazz = long.class;
            }
        }
        return makeLiteral(bd,
                _typeFactory.createJavaType(clazz),
                SqlTypeName.Decimal);
    }

    public RexLiteral makeApproxLiteral(BigDecimal bd) {
        return makeLiteral(bd,
                _typeFactory.createJavaType(double.class),
                SqlTypeName.Double);
    }

    public RexLiteral makeLiteral(String s) {
        return makeLiteral(new NlsString(s, null, null),
                _typeFactory.createJavaType(String.class),
                SqlTypeName.Char);
    }

}
