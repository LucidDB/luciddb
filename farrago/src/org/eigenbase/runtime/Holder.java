/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// (C) Copyright 2003-2004 John V. Sichi
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

package org.eigenbase.runtime;

/**
 * A set of classes for holding primitive objects.
 *
 * @author jhyde
 * @version $Id$
 *
 * @since 8 February, 2002
 */
public class Holder
{
    //~ Inner Classes ---------------------------------------------------------

    public static final class int_Holder
    {
        public int value;

        public int_Holder(int value)
        {
            this.value = value;
        }

        void setGreater(int other)
        {
            if (other > value) {
                value = other;
            }
        }

        void setLesser(int other)
        {
            if (other < value) {
                value = other;
            }
        }
    }
}


// End Holder.java
