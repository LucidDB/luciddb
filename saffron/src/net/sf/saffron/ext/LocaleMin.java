/*
// Saffron preprocessor and data engine.
// Copyright (C) 2002-2004 Disruptive Tech
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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

package net.sf.saffron.ext;

import java.text.Collator;
import java.util.Locale;

import net.sf.saffron.core.AggregationExtender;


/**
 * <code>LocaleMin</code> is an example of a custom aggregation. It evaluates
 * the minimum of a set of values, according to a given locale.
 *
 * @author jhyde
 * @version $Id$
 *
 * @since 10 February, 2002
 */
public class LocaleMin implements AggregationExtender
{
    private Collator collator;

    public LocaleMin(Locale locale)
    {
        this.collator = Collator.getInstance(locale);
    }

    // the following methods fulfill the 'AggregationExtender' contract for
    // 'Object' values
    public String aggregate(String v)
    {
        throw new UnsupportedOperationException();
    }

    public Object merge(
        String v,
        Object accumulator0,
        Object accumulator1)
    {
        return lesser((String) accumulator0, (String) accumulator1);
    }

    public Object next(
        String v,
        Object accumulator)
    {
        return lesser(v, (String) accumulator);
    }

    public String result(
        String v,
        Object accumulator)
    {
        return (String) accumulator;
    }

    public Object start(String v)
    {
        return null;
    }

    private final String lesser(
        String s,
        String t)
    {
        if (s != null) {
            if ((t == null) || (collator.compare(s, t) < 0)) {
                return s;
            }
        }
        return t;
    }
}


// End LocaleMin.java
