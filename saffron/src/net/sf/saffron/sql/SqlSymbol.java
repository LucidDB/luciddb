/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2004-2004 Disruptive Technologies, Inc.
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
package net.sf.saffron.sql;

import net.sf.saffron.util.EnumeratedValues;
import net.sf.saffron.util.Util;
import net.sf.saffron.sql.SqlLiteral;
import net.sf.saffron.sql.parser.ParserPosition;
import net.sf.saffron.sql.type.SqlTypeName;

/**
 * Represents a value of an enumerated type which is exposed as a SQL keyword.
 *
 * <p>Consider the TRIM function, whose syntax is
 * <code>TRIM([LEADING | TRAILING | BOTH] char FROM string)</code>. You can
 * represent the values LEADING, TRAILING and BOTH as values of an enumeration,
 * and you can also associate a literal value for when these values need to
 * appear in a parse tree.</p>
 *
 * <p>Derived classes should generally follow the pattern set by
 * {@link net.sf.saffron.sql.fun.SqlTrimFunction.Flag}:<ul>
 * <li>a <code>static final</code> member for each value;</li>
 * <li>a <code>static final</code> member 'EnumeratedValues enumeration';</li>
 * <li><code>static</code> lookup methods 'get(String)' and 'get(int)'.</li>
 * </ul>
 *
 * @author jhyde
 * @since May 28, 2004
 * @version $Id$
 **/
public class SqlSymbol extends SqlLiteral
{
    public final String _name;

    public SqlSymbol(String name, ParserPosition parserPosition) {
        super(name, SqlTypeName.Symbol, parserPosition);
        this._name = name;
    }

    public String getDescription() {
        return null;
    }

    public String getName() {
        return _name;
    }


    /**
     * Returns the value's name.
     */
    public String toString() {
        return _name;
    }

    public Error unexpected() {
        return Util.newInternal(
                "Value " + _name + " of class " + getClass()
                + " unexpected here");
    }

    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.print(_name.toUpperCase());
    }
}

// End SqlSymbol.java