/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
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
package net.sf.farrago.fennel;

import java.util.Arrays;
import org.eigenbase.util.Util;

/**
 * FennelPseudoUuid provides access to the Fennel PseudoUuid class.  Fennel's
 * PseudoUuid represents universal unique identifiers (UUIDs) in a way that
 * abstracts away OS and hardware dependencies.
 *
 * <p>Depends on Fennel's libfarrago.
 *
 * @author Stephan Zuercher
 * @version $Id$
 */
public class FennelPseudoUuid
{
    //~ Static fields/initializers --------------------------------------------
    private static final int UUID_LENGTH = 16;

    static {
        Util.loadLibrary("farrago");
    }

    //~ Instance fields -------------------------------------------------------
    private final byte[] uuid;

    //~ Constructors ----------------------------------------------------------

    /**
     * Private constructor.  Creates an empty FennelPseudoUuid.  Use
     * {@link #generate()} or {@link #generateInvalid()} to create a UUID.
     */
    private FennelPseudoUuid(byte[] bytes)
    {
        uuid = bytes;
        validate();
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * @return a valid UUID object
     */
    public static FennelPseudoUuid generate()
    {
        return new FennelPseudoUuid(nativeGenerate());
    }

    /**
     * @return an invalid UUID object (UUID's actual value is constant)
     */
    public static FennelPseudoUuid generateInvalid()
    {
        return new FennelPseudoUuid(nativeGenerateInvalid());
    }

    /**
     * Parses the given string and returns the UUID it represents.  See
     * {@link #toString()} for details on the format of the UUID string.
     *
     * @return a UUID object
     * @throws IllegalArgumentException if the String is not in the correct
     *                                  format.
     */
    public static FennelPseudoUuid parse(String uuid)
    {
        if (uuid.length() != 36) {
            throw new IllegalArgumentException("invalid uuid format");
        }

        byte[] bytes = new byte[UUID_LENGTH];

        try {
            // one 4 byte int
            for(int i = 0; i < 4; i++) {
                int b = Integer.parseInt(uuid.substring(i * 2, i * 2 + 2), 16);

                bytes[i] = (byte)b;
            }
            if (uuid.charAt(8) != '-') {
                throw new IllegalArgumentException("invalid uuid format");
            }

            // three 2 byte ints
            for(int i = 0; i < 3; i++) {
                int start = 9 + i * 5;

                for(int j = 0; j < 2; j++) {
                    int b =
                        Integer.parseInt(
                            uuid.substring(
                                start + j * 2, start + j *  2 + 2), 16);
                    bytes[4 + i * 2 + j] = (byte)b;
                }

                if (uuid.charAt(start + 4) != '-') {
                    throw new IllegalArgumentException("invalid uuid format");
                }
            }

            // one 6-byte int
            for(int i = 0; i < 6; i++) {
                int b = Integer.parseInt(
                    uuid.substring(24 + i * 2, 24 + i * 2 + 2), 16);

                bytes[10 + i] = (byte)b;
            }
        } catch(NumberFormatException e) {
            IllegalArgumentException iae =
                new IllegalArgumentException("invalid uuid format");
            iae.initCause(e);
            throw iae;
        }

        return new FennelPseudoUuid(bytes);
    }

    /**
     * Convert a sequence of 16 bytes into a UUID object.
     *
     * @param bytes the bytes of a UUID
     * @return a UUID object
     */
    public static FennelPseudoUuid parse(byte[] bytes)
    {
        assert(bytes.length == UUID_LENGTH);

        // clone byte array so changes can't affect us
        return new FennelPseudoUuid((byte[])bytes.clone());
    }

    /**
     * @return UUID in xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx format
     */
    public String toString()
    {
        StringBuffer buffer = new StringBuffer(36);

        for(int i = 0; i < uuid.length; i++) {
            if (i == 4 || i == 6 || i == 8 || i == 10) {
                buffer.append('-');
            }

            String digits = Integer.toHexString((int)uuid[i] & 0xFF);
            if (digits.length() == 1) {
                buffer.append('0');
            }
            buffer.append(digits);
        }

        return buffer.toString();
    }

    /**
     * Compares two UUID objects for equality by value.
     * @param obj another FennelPseudoUuid object
     * @return true if the UUIDs are the same, false otherwise
     * @throws java.lang.ClassCastException if <code>obj</code> is not a FennelPseudoUuid
     */
    public boolean equals(Object obj)
    {
        FennelPseudoUuid other = (FennelPseudoUuid)obj;

        return Arrays.equals(this.uuid, other.uuid);
    }

    public int hashCode()
    {
        if (uuid == null) {
            return 0;
        }

        // REVIEW: SWZ 12/1/2004: This wants a better algorithm.
        int hash = 0;
        int i;
        for(i = 0; i < uuid.length; i += 4) {
            hash = hash ^ ((uuid[i] << 24) | (uuid[i + 1] << 16) | (uuid[i + 2] << 8) | uuid[i + 3]);
        }
        if (i < uuid.length) {
            int last = 0;
            for( ; i < uuid.length; i++) {
                last = last | uuid[i] << ((3 - i) * 8);
            }
            hash = hash ^ last;
        }

        return hash;
    }

    private void validate()
    {
        assert(uuid != null);
        assert(uuid.length == UUID_LENGTH);
    }

    private static native byte[] nativeGenerate();

    private static native byte[] nativeGenerateInvalid();
}
