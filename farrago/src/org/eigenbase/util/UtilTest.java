/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
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

package org.eigenbase.util;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;

import junit.framework.TestCase;
import junit.framework.Test;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

import org.eigenbase.runtime.*;

/**
 * Unit test for {@link Util} and other classes in this package.
 *
 * @author jhyde
 * @since Jul 12, 2004
 * @version $Id$
 **/
public class UtilTest extends TestCase
{
    //~ Constructors ----------------------------------------------------------

    public UtilTest(String name)
    {
        super(name);
    }

    //~ Methods ---------------------------------------------------------------

    public static Test suite()
        throws Exception
    {
        TestSuite suite = new TestSuite();
        suite.addTestSuite(UtilTest.class);
        suite.addTestSuite(BinaryHeap.BinaryHeapTestCase.class);
        suite.addTestSuite(ThreadIterator.Test.class);
        suite.addTestSuite(TimeoutIteratorTest.class);
        return suite;
    }

    public void testPrintEquals()
    {
        assertPrintEquals("\"x\"", "x", true);
    }

    public void testPrintEquals2()
    {
        assertPrintEquals("\"x\"", "x", false);
    }

    public void testPrintEquals3()
    {
        assertPrintEquals("null", null, true);
    }

    public void testPrintEquals4()
    {
        assertPrintEquals("", null, false);
    }

    public void testPrintEquals5()
    {
        assertPrintEquals("\"\\\\\\\"\\r\\n\"", "\\\"\r\n", true);
    }

    public void testScientificNotation()
    {
        BigDecimal bd;

        bd = new BigDecimal("0.001234");
        TestUtil.assertEqualsVerbose(
            "1.234E-3",
            Util.toScientificNotation(bd));
        bd = new BigDecimal("0.001");
        TestUtil.assertEqualsVerbose(
            "1E-3",
            Util.toScientificNotation(bd));
        bd = new BigDecimal("-0.001");
        TestUtil.assertEqualsVerbose(
            "-1E-3",
            Util.toScientificNotation(bd));
        bd = new BigDecimal("1");
        TestUtil.assertEqualsVerbose(
            "1E0",
            Util.toScientificNotation(bd));
        bd = new BigDecimal("-1");
        TestUtil.assertEqualsVerbose(
            "-1E0",
            Util.toScientificNotation(bd));
        bd = new BigDecimal("1.0");
        TestUtil.assertEqualsVerbose(
            "1.0E0",
            Util.toScientificNotation(bd));
        bd = new BigDecimal("12345");
        TestUtil.assertEqualsVerbose(
            "1.2345E4",
            Util.toScientificNotation(bd));
        bd = new BigDecimal("12345.00");
        TestUtil.assertEqualsVerbose(
            "1.234500E4",
            Util.toScientificNotation(bd));
        bd = new BigDecimal("12345.001");
        TestUtil.assertEqualsVerbose(
            "1.2345001E4",
            Util.toScientificNotation(bd));

        //test truncate
        bd = new BigDecimal("1.23456789012345678901");
        TestUtil.assertEqualsVerbose(
            "1.2345678901234567890E0",
            Util.toScientificNotation(bd));
        bd = new BigDecimal("-1.23456789012345678901");
        TestUtil.assertEqualsVerbose(
            "-1.2345678901234567890E0",
            Util.toScientificNotation(bd));
    }

    public void testToJavaId()
        throws UnsupportedEncodingException
    {
        assertEquals(
            "ID$0$foo",
            Util.toJavaId("foo", 0));
        assertEquals(
            "ID$0$foo_20_bar",
            Util.toJavaId("foo bar", 0));
        assertEquals(
            "ID$0$foo__bar",
            Util.toJavaId("foo_bar", 0));
        assertEquals(
            "ID$100$_30_bar",
            Util.toJavaId("0bar", 100));
        assertEquals(
            "ID$0$foo0bar",
            Util.toJavaId("foo0bar", 0));
        assertEquals(
            "ID$0$it_27_s_20_a_20_bird_2c__20_it_27_s_20_a_20_plane_21_",
            Util.toJavaId("it's a bird, it's a plane!", 0));

        // Try some funny non-ASCII charsets
        assertEquals(
            "ID$0$_f6__cb__c4__ca__ae__c1__f9__cb_",
            Util.toJavaId("\u00f6\u00cb\u00c4\u00ca\u00ae\u00c1\u00f9\u00cb", 0));
        assertEquals(
            "ID$0$_f6cb__c4ca__aec1__f9cb_",
            Util.toJavaId("\uf6cb\uc4ca\uaec1\uf9cb", 0));
        byte [] bytes1 =
        { 3, 12, 54, 23, 33, 23, 45, 21, 127, -34, -92, -113 };
        assertEquals(
            "ID$0$_3__c_6_17__21__17__2d__15__7f__6cd9__fffd_",
            Util.toJavaId(
                new String(bytes1, "EUC-JP"),
                0));
        byte [] bytes2 =
        { 64, 32, 43, -45, -23, 0, 43, 54, 119, -32, -56, -34 };
        assertEquals(
            "ID$0$_30c__3617__2117__2d15__7fde__a48f_",
            Util.toJavaId(
                new String(bytes1, "UTF-16"),
                0));
    }

    private void assertPrintEquals(
        String expect,
        String in,
        boolean nullMeansNull)
    {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        Util.printJavaString(pw, in, nullMeansNull);
        pw.flush();
        String out = sw.toString();
        assertEquals(expect, out);
    }

    /**
     * Unit-test for {@link BitString}.
     */
    public void testBitString()
    {
        // Powers of two, minimal length.
        final BitString b0 = new BitString("", 0);
        final BitString b1 = new BitString("1", 1);
        final BitString b2 = new BitString("10", 2);
        final BitString b4 = new BitString("100", 3);
        final BitString b8 = new BitString("1000", 4);
        final BitString b16 = new BitString("10000", 5);
        final BitString b32 = new BitString("100000", 6);
        final BitString b64 = new BitString("1000000", 7);
        final BitString b128 = new BitString("10000000", 8);
        final BitString b256 = new BitString("100000000", 9);

        // other strings
        final BitString b0_1 = new BitString("", 1);
        final BitString b0_12 = new BitString("", 12);

        // conversion to hex strings
        assertEquals(
            "",
            b0.toHexString());
        assertEquals(
            "1",
            b1.toHexString());
        assertEquals(
            "2",
            b2.toHexString());
        assertEquals(
            "4",
            b4.toHexString());
        assertEquals(
            "8",
            b8.toHexString());
        assertEquals(
            "10",
            b16.toHexString());
        assertEquals(
            "20",
            b32.toHexString());
        assertEquals(
            "40",
            b64.toHexString());
        assertEquals(
            "80",
            b128.toHexString());
        assertEquals(
            "100",
            b256.toHexString());
        assertEquals(
            "0",
            b0_1.toHexString());
        assertEquals(
            "000",
            b0_12.toHexString());

        // to byte array
        assertByteArray("01", "1", 1);
        assertByteArray("01", "1", 5);
        assertByteArray("01", "1", 8);
        assertByteArray("00, 01", "1", 9);
        assertByteArray("", "", 0);
        assertByteArray("00", "0", 1);
        assertByteArray("00", "0000", 2); // bit count less than string
        assertByteArray("00", "000", 5); // bit count larger than string
        assertByteArray("00", "0", 8); // precisely 1 byte
        assertByteArray("00, 00", "00", 9); // just over 1 byte

        // from hex string
        assertReversible("");
        assertReversible("1");
        assertReversible("10");
        assertReversible("100");
        assertReversible("1000");
        assertReversible("10000");
        assertReversible("100000");
        assertReversible("1000000");
        assertReversible("10000000");
        assertReversible("100000000");
        assertReversible("01");
        assertReversible("001010");
        assertReversible("000000000100");
    }

    private static void assertReversible(String s)
    {
        assertEquals(
            s,
            BitString.createFromBitString(s).toBitString(),
            s);
        assertEquals(
            s,
            BitString.createFromHexString(s).toHexString());
    }

    private void assertByteArray(
        String expected,
        String bits,
        int bitCount)
    {
        byte [] bytes = BitString.toByteArrayFromBitString(bits, bitCount);
        final String s = toString(bytes);
        assertEquals(expected, s);
    }

    /**
     * Converts a byte array to a hex string like "AB, CD".
     */
    private String toString(byte [] bytes)
    {
        StringBuffer buf = new StringBuffer();
        for (int i = 0; i < bytes.length; i++) {
            byte b = bytes[i];
            if (i > 0) {
                buf.append(", ");
            }
            String s = Integer.toString(b, 16);
            buf.append((b < 16) ? ("0" + s) : s);
        }
        return buf.toString();
    }
    
    /**
     * Runs the test suite.
     */
    public static void main(String [] args)
        throws Exception
    {
        TestRunner.run(suite());
    }
}


// End UtilTest.java
