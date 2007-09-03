/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2007-2007 The Eigenbase Project
// Copyright (C) 2007-2007 Disruptive Tech
// Copyright (C) 2007-2007 LucidEra, Inc.
// Portions Copyright (C) 2007-2007 John V. Sichi
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

import java.nio.charset.Charset;
import java.io.*;

/**
 * Serializable wrapper around a {@link Charset}.
 *
 * <p>It serializes itself by writing out the name of the character set, for
 * example "ISO-8859-1". On the other side, it deserializes itself by looking
 * for a charset with the same name.
 *
 * <p>A SerializableCharset is immutable.
 *
 * @version $Id$
 * @author jhyde
 */
public class SerializableCharset implements Serializable
{
    private Charset charset;
    private String charsetName;

    /**
     * Creates a SerializableCharset. External users should call
     * {@link #forCharset(Charset)}.
     *
     * @param charset Character set; must not be null
     */
    private SerializableCharset(Charset charset) {
        assert charset != null;
        this.charset = charset;
        this.charsetName = charset.name();
    }

    /**
     * Per {@link Serializable}.
     */
    private void writeObject(ObjectOutputStream out) throws IOException
    {
        out.writeObject(charset.name());
    }

    /**
     * Per {@link Serializable}.
     */
    private void readObject(ObjectInputStream in)
        throws IOException, ClassNotFoundException
    {
        charsetName = (String) in.readObject();
        charset = Charset.availableCharsets().get(this.charsetName);
    }

    /**
     * Returns the wrapped {@link Charset}.
     *
     * @return the wrapped Charset
     */
    public Charset getCharset()
    {
        return charset;
    }

    /**
     * Returns a SerializableCharset wrapping the given Charset, or null
     * if the <coded>charset</code> is null.
     *
     * @param charset Character set to wrap, or null
     * @return Wrapped charset
     */
    public static SerializableCharset forCharset(Charset charset)
    {
        if (charset == null) {
            return null;
        }
        return new SerializableCharset(charset);
    }
}

// End SerializableCharset.java
