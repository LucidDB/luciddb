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
package net.sf.saffron.util;

import java.math.BigInteger;

/**
 * String of bits
 *
 * @author Wael Chatila
 * @since May 28, 2004
 * @version $Id$
 **/
public class BitString {
    //~ Member variables -----------
    private String _bits;
    private int _bitCount;

    //~ Methods -----------
    /**
     * Creates a BitString representation out of a Hex String.
     * Initial zeros are be preserved.
     * Hex String is defined in the SQL standard to be a string with odd number
     * of hex digits.
     * An even number of hex digits is in the standard a Binary String.
     */
    public static BitString createFromHexString(String s) {
        int bitCount = s.length() * 4;
        String bits = bitCount == 0 ? "" :
                new BigInteger(s, 16).toString(2);
        return new BitString(bits, bitCount);
    }

    /**
     * Creates a BitString representation out of a Bit String.
     * Initial zeros are be preserved.
     */
    public static BitString createFromBitString(String s){
        return new BitString(s, s.length());
    }

    protected BitString(String bits, int bitCount) {
        assert bits.replaceAll("1","").replaceAll("0","").length() == 0 :
                "bit string '" + bits + "' contains digits other than {0, 1}";
        _bits = bits;
        _bitCount = bitCount;
    }

    public String toString() {
        return toBitString();
    }

    public int getBitCount() {
        return _bitCount;
    }

    public byte[] getAsByteArray() {
        return toByteArrayFromBitString(_bits, _bitCount);
    }

    /**
     * Returns this bit string as a bit string, such as "10110".
     */
    public String toBitString() {
        return _bits;
    }

    /**
     * Converts this bit string to a hex string, such as "7AB".
     */
    public String toHexString() {
        byte[] bytes = getAsByteArray();
        String s = Util.toStringFromByteArray(bytes, 16);
        switch (_bitCount % 8) {
        case 1: // B'1' -> X'1'
        case 2: // B'10' -> X'2'
        case 3: // B'100' -> X'4'
        case 4: // B'1000' -> X'8'
            return s.substring(1);
        case 5: // B'10000' -> X'10'
        case 6: // B'100000' -> X'20'
        case 7: // B'1000000' -> X'40'
        case 0: // B'10000000' -> X'80', and B'' -> X''
            return s;
        }
        if (_bitCount % 8 == 4) {
            return s.substring(1);
        } else {
            return s;
        }
    }

    /**
     * Converts a bit string to an array of bytes.
     *
     * @post return.length = (bitCount + 7) / 8
     */
    public static byte[] toByteArrayFromBitString(String bits, int bitCount)
    {
        if (bitCount < 0) {
            return new byte[0];
        }
        int byteCount = (bitCount + 7) / 8;
        byte[] srcBytes;
        if (bits.length() > 0) {
            BigInteger bigInt = new BigInteger(bits, 2);
            srcBytes = bigInt.toByteArray();
        } else {
            srcBytes = new byte[0];
        }
        byte[] dest = new byte[byteCount];
        // If the number started with 0s, the array won't be very long. Assume
        // that ret is already initialized to 0s, and just copy into the
        // RHS of it.
        int bytesToCopy = Math.min(byteCount, srcBytes.length);
        System.arraycopy(srcBytes, srcBytes.length - bytesToCopy,
                dest, dest.length - bytesToCopy, bytesToCopy);
        return dest;
    }


}

// End BitString.java