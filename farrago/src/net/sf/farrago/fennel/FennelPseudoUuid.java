/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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
package net.sf.farrago.fennel;

import java.util.Arrays;

/**
 * FennelPseudoUuid represents universal unique identifiers (UUIDs).  UUIDs
 * are generated via Fennel (via {@link FennelPseudoUuidGenerator}) in a way
 * that abstracts away OS and hardware dependencies.
 *
 * <p>Depends on Fennel's libfarrago.
 *
 * @author Stephan Zuercher
 * @version $Id$
 */
public class FennelPseudoUuid implements java.io.Serializable
{
    //~ Static fields/initializers --------------------------------------------
    public static final int UUID_LENGTH = 16;

    //~ Instance fields -------------------------------------------------------
    private final byte[] uuid;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a FennelPseudoUuid with the given bytes.  Use
     * {@link FennelPseudoUuidGenerator#validUuid()} or
     * {@link FennelPseudoUuidGenerator#invalidUuid()} to create a UUID.
     */
    public FennelPseudoUuid(byte[] bytes)
    {
        this.uuid = (byte[])bytes.clone();
        validate();
    }

    public FennelPseudoUuid(String uuidStr)
    {
        this.uuid = parse(uuidStr);
        validate();
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * @return a copy of the byte array that backs this FennelPseudoUuid.
     */
    public byte[] getBytes()
    {
        byte[] copy = new byte[UUID_LENGTH];
        for(int i = 0; i < UUID_LENGTH; i++) {
            copy[i] = uuid[i];
        }
        return copy;
    }

    /**
     * Returns the byte value at a particular position within the UUID
     * represented by this instance.
     *
     * @param index must be greater than or equal to 0 and less than
     *        {@link #UUID_LENGTH}.
     * @return the byte value at a particular possition
     * @throws ArrayIndexOutOfBoundsException
     *         if index is less than 0 or greater than or equal to
     *         {@link #UUID_LENGTH}.
     */
    public byte getByte(int index)
    {
        return uuid[index];
    }

    /**
     * Parses the given string and returns the UUID it represents.  See
     * {@link #toString()} for details on the format of the UUID string.
     *
     * @return a byte-array representing the UUID's contents, suitable for
     *         use by {@link #FennelPseudoUuid(byte[])}
     * @throws IllegalArgumentException if the String is not in the correct
     *                                  format.
     */
    private byte[] parse(String uuid)
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

        return bytes;
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
     *
     * @param obj another FennelPseudoUuid object
     * @return true if the UUIDs are the same, false otherwise
     * @throws ClassCastException if <code>obj</code> is not a FennelPseudoUuid
     */
    public boolean equals(Object obj)
    {
        FennelPseudoUuid other = (FennelPseudoUuid)obj;

        return Arrays.equals(this.uuid, other.uuid);
    }

    public int hashCode()
    {
        // REVIEW: SWZ 12/1/2004: This may want a better algorithm.  As long
        // as Fennel's UUIDs are random numbers, this provides a nearly random
        // distribution of hash code values -- if the UUIDs aren't random (for
        // instance if they're based on time, MAC address, etc.), that may not
        // be the case.
        return
            ((int)(uuid[0] ^ uuid[4] ^ uuid[8] ^ uuid[12]) & 0xFF) << 24 |
            ((int)(uuid[1] ^ uuid[5] ^ uuid[9] ^ uuid[13]) & 0xFF) << 16 |
            ((int)(uuid[2] ^ uuid[6] ^ uuid[10] ^ uuid[14]) & 0xFF) << 8 |
            ((int)(uuid[3] ^ uuid[7] ^ uuid[11] ^ uuid[15]) & 0xFF);
    }

    private void validate()
    {
        assert(uuid != null);
        assert(uuid.length == UUID_LENGTH);
    }
}
