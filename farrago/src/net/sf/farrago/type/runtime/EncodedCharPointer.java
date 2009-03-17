/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
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
package net.sf.farrago.type.runtime;

import java.io.*;

import org.eigenbase.util.*;


/**
 * EncodedCharPointer specializes BytePointer to interpret its bytes as
 * characters encoded via a given charset.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class EncodedCharPointer
    extends BytePointer
{
    //~ Methods ----------------------------------------------------------------

    // TODO:  preallocate a CharsetDecoder
    public String toString()
    {
        if (buf == null) {
            return null;
        }
        try {
            return new String(
                buf,
                pos,
                count - pos,
                getCharsetName());
        } catch (UnsupportedEncodingException ex) {
            throw Util.newInternal(ex);
        }
    }

    // refine BytePointer to make this method abstract so that subclasses
    // are forced to override it
    protected abstract String getCharsetName();

    // implement BytePointer
    protected byte [] getBytesForString(String string)
    {
        try {
            return string.getBytes(getCharsetName());
        } catch (UnsupportedEncodingException ex) {
            throw Util.newInternal(ex);
        }
    }

    // override BytePointer
    public Object getNullableData()
    {
        return toString();
    }
}

// End EncodedCharPointer.java
