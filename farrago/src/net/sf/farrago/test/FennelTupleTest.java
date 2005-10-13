/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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

package net.sf.farrago.test;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import java.nio.ByteBuffer;
import java.util.Iterator;

import net.sf.farrago.fennel.tuple.*;
import org.eigenbase.util.Util;


/**
 * Set of unit tests which exercise the Java Tuple Library.
 *
 * @author mbennett
 * @since Dec 29, 2004
 * @version $Id$
 **/
public class FennelTupleTest extends TestCase
{
    //~ Methods ---------------------------------------------------------------

    private FennelTupleDescriptor buildDescriptor(
        FennelStandardTypeDescriptor[] types,
        boolean[] nullable,
        int[] sizes)
    {
        FennelTupleDescriptor d = new FennelTupleDescriptor();
        for (int i = 0; i < types.length; ++i) {
            d.add(
                new FennelTupleAttributeDescriptor(
                    types[i], nullable[i], sizes[i]));
        }
        return d;
    }

    private FennelTupleDescriptor buildDescriptor(
        FennelStandardTypeDescriptor[] types)
    {
        FennelTupleDescriptor d = new FennelTupleDescriptor();
        for (int i = 0; i < types.length; ++i) {
            d.add(
                new FennelTupleAttributeDescriptor(
                    types[i], false, 0));
        }
        return d;
    }

    private boolean compareObjects(Object[] before, Object[] after)
    {
        //System.out.println("comparing " + before.length + " values ");
        assertTrue(before.length == after.length);
        for (int i = 0; i < before.length; ++i) {
            Object objBefore = before[i];
            Object objAfter = after[i];

            if (objBefore == null && objAfter == null) {
                continue;
            }
            /*
             * due to an artifact of the way we unmarshall fixed length
             * arrays into Strings, it's possible to get a much larger
             * string out of an unmarshall than we put into a marshall.
             * If it's a string, just make sure the output starts with
             * the input.
             */
            if ( objBefore instanceof String ) {
                assertTrue(
                    "string " + i + " values should match,",
                    ((String) objAfter).startsWith((String) objBefore));
            } else {
                assertEquals(
                    "object " + i + " values should match,",
                    objBefore, objAfter);
                // integer promotion might cause false-positive above,
                // so check classes, too
                assertEquals(
                    "object " + i + " classes should match,",
                    objBefore.getClass(), objAfter.getClass());
            }
        }
        return true;
    }

    public ByteBuffer marshallValues(
        FennelTupleDescriptor desc,
        Object[] objs,
        ByteBuffer iBuff,
        FennelTupleAccessor a,
        FennelTupleData d )
        throws NullPointerException
    {
        for (int i = 0; i < desc.getAttrCount(); ++i) {
            if (objs[i] == null) {
                continue;
            }
            switch(desc.getAttr(i).typeDescriptor.getOrdinal()) {
                case FennelStandardTypeDescriptor.INT_8_ORDINAL:
                    d.getDatum(i).setByte(((Byte) objs[i]).byteValue());
                    break;
                case FennelStandardTypeDescriptor.UINT_8_ORDINAL:
                    d.getDatum(i).setUnsignedByte(
                        ((Short) objs[i]).shortValue());
                    break;
                case FennelStandardTypeDescriptor.INT_16_ORDINAL:
                    d.getDatum(i).setShort(((Short) objs[i]).shortValue());
                    break;
                case FennelStandardTypeDescriptor.UINT_16_ORDINAL:
                    d.getDatum(i).setUnsignedShort(
                        ((Integer) objs[i]).intValue());
                    break;
                case FennelStandardTypeDescriptor.INT_32_ORDINAL:
                    d.getDatum(i).setInt(((Integer) objs[i]).intValue());
                    break;
                case FennelStandardTypeDescriptor.UINT_32_ORDINAL:
                    d.getDatum(i).setUnsignedInt(((Long) objs[i]).longValue());
                    break;
                case FennelStandardTypeDescriptor.INT_64_ORDINAL:
                    d.getDatum(i).setLong(((Long) objs[i]).longValue());
                    break;
                case FennelStandardTypeDescriptor.UINT_64_ORDINAL:
                    d.getDatum(i).setUnsignedLong(
                        ((Long) objs[i]).longValue());
                    break;
                case FennelStandardTypeDescriptor.BOOL_ORDINAL:
                    d.getDatum(i).setBoolean(
                        ((Boolean) objs[i]).booleanValue());
                    break;
                case FennelStandardTypeDescriptor.REAL_ORDINAL:
                    d.getDatum(i).setFloat(((Float) objs[i]).floatValue());
                    break;
                case FennelStandardTypeDescriptor.DOUBLE_ORDINAL:
                    d.getDatum(i).setDouble(((Double) objs[i]).doubleValue());
                    break;
                case FennelStandardTypeDescriptor.CHAR_ORDINAL:
                    d.getDatum(i).setString(((String) objs[i]));
                    break;
                case FennelStandardTypeDescriptor.VARCHAR_ORDINAL:
                    d.getDatum(i).setString(((String) objs[i]));
                    break;
                case FennelStandardTypeDescriptor.BINARY_ORDINAL:
                    d.getDatum(i).setBytes(((String) objs[i]).getBytes());
                    break;
                case FennelStandardTypeDescriptor.VARBINARY_ORDINAL:
                    d.getDatum(i).setBytes(((String) objs[i]).getBytes());
                    break;
                default:
                    assertTrue(false);
            }
        }
        a.marshal(d, iBuff);
        assert(iBuff.position() > 0);

        return iBuff;
    }

    public ByteBuffer marshallValues(FennelTupleDescriptor desc, Object[] objs)
        throws NullPointerException
    {
        ByteBuffer iBuff = ByteBuffer.allocate(50000);
        FennelTupleAccessor a = new FennelTupleAccessor();
        a.compute(desc);
        assertTrue(desc.getAttrCount() == a.size());

        FennelTupleData d = new FennelTupleData(desc);
        assertTrue(d.getDatumCount() == a.size());

        return marshallValues( desc, objs, iBuff, a, d );
    }

    public Object[] unmarshallValues(
        FennelTupleDescriptor desc,
        ByteBuffer iBuff,
        FennelTupleAccessor a,
        FennelTupleData d )
    {
        assertTrue(desc.getAttrCount() == a.size());
        assertTrue(d.getDatumCount() == a.size());

        // unmarshall
        a.setCurrentTupleBuf(iBuff);
        a.unmarshal(d);

        // build the unmarshalled object holders
        Object[] o = new Object[desc.getAttrCount()];

        for (int i = 0; i < desc.getAttrCount(); ++i) {
            if (! d.getDatum(i).isPresent()) {
                continue;
            }
            
            switch (desc.getAttr(i).typeDescriptor.getOrdinal()) {
                case FennelStandardTypeDescriptor.INT_8_ORDINAL:
                    o[i] = new Byte(d.getDatum(i).getByte());
                    break;
                case FennelStandardTypeDescriptor.UINT_8_ORDINAL:
                    o[i] = new Short(d.getDatum(i).getUnsignedByte());
                    break;
                case FennelStandardTypeDescriptor.INT_16_ORDINAL:
                    o[i] = new Short(d.getDatum(i).getShort());
                    break;
                case FennelStandardTypeDescriptor.UINT_16_ORDINAL:
                    o[i] = new Integer(d.getDatum(i).getUnsignedShort());
                    break;
                case FennelStandardTypeDescriptor.INT_32_ORDINAL:
                    o[i] = new Integer(d.getDatum(i).getInt());
                    break;
                case FennelStandardTypeDescriptor.UINT_32_ORDINAL:
                    o[i] = new Long(d.getDatum(i).getUnsignedInt());
                    break;
                case FennelStandardTypeDescriptor.INT_64_ORDINAL:
                    o[i] = new Long(d.getDatum(i).getLong());
                    break;
                case FennelStandardTypeDescriptor.UINT_64_ORDINAL:
                    o[i] = new Long(d.getDatum(i).getUnsignedLong());
                    break;
                case FennelStandardTypeDescriptor.BOOL_ORDINAL:
                    o[i] = new Boolean(d.getDatum(i).getBoolean());
                    break;
                case FennelStandardTypeDescriptor.REAL_ORDINAL:
                    o[i] = new Float(d.getDatum(i).getFloat());
                    break;
                case FennelStandardTypeDescriptor.DOUBLE_ORDINAL:
                    o[i] = new Double(d.getDatum(i).getDouble());
                    break;
                case FennelStandardTypeDescriptor.CHAR_ORDINAL:
                    o[i] = new String(
                        d.getDatum(i).getBytes(),
                        0,
                        d.getDatum(i).getLength());
                    break;
                case FennelStandardTypeDescriptor.VARCHAR_ORDINAL:
                    o[i] = new String(
                        d.getDatum(i).getBytes(),
                        0,
                        d.getDatum(i).getLength());
                    break;
                case FennelStandardTypeDescriptor.BINARY_ORDINAL:
                    o[i] = new String(
                        d.getDatum(i).getBytes(),
                        0,
                        d.getDatum(i).getLength());
                    break;
                case FennelStandardTypeDescriptor.VARBINARY_ORDINAL:
                    o[i] = new String(
                        d.getDatum(i).getBytes(),
                        0,
                        d.getDatum(i).getLength());
                    break;
                default:
                    assertTrue(false);
            }
        }
        return o;
    }

    public Object[] unmarshallValues(
        FennelTupleDescriptor desc,
        ByteBuffer iBuff)
    {
        FennelTupleAccessor a = new FennelTupleAccessor();
        a.compute(desc);

        FennelTupleData d = new FennelTupleData(desc);

        return unmarshallValues( desc, iBuff, a, d );
    }

    public void testTupleAlignment()
    {
        FennelTupleAccessor def = new FennelTupleAccessor();
        FennelTupleAccessor by4 =
            new FennelTupleAccessor(FennelTupleAccessor.TUPLE_ALIGN4);
        FennelTupleAccessor by8 =
            new FennelTupleAccessor(FennelTupleAccessor.TUPLE_ALIGN8);
//        FennelTupleAccessor by5 = new FennelTupleAccessor(5);

        FennelTupleDescriptor desc = new FennelTupleDescriptor();
        desc.add(
            new FennelTupleAttributeDescriptor(
                FennelStandardTypeDescriptor.INT_16, false, 0));
        desc.add(
            new FennelTupleAttributeDescriptor(
                // len=32 requires padding for 8-byte alignment
                FennelStandardTypeDescriptor.VARCHAR, false, 32));

        def.compute(desc);
        by4.compute(desc);
        by8.compute(desc);

        int defsize = def.getMaxByteCount();
        int by4size = by4.getMaxByteCount();
        int by8size = by8.getMaxByteCount();
//        System.out.println(
//            "def=" +defsize +" by4=" +by4size +" by8=" +by8size);

        assertEquals("4-byte alignment is default,", by4size, defsize);
        assertTrue("4-byte alignment, size=" +by4size, by4size % 4 == 0);
        assertTrue("8-byte alignment, size=" +by8size, by8size % 8 == 0);
    }

    public void testMinimal()
    {
        FennelStandardTypeDescriptor[] o1 = {
            FennelStandardTypeDescriptor.INT_32,
            FennelStandardTypeDescriptor.INT_16,
            FennelStandardTypeDescriptor.UINT_32,
            FennelStandardTypeDescriptor.INT_8,
            FennelStandardTypeDescriptor.UINT_8 };
        FennelTupleDescriptor desc = buildDescriptor(o1);

        Object[] before = {
            new Integer(5555),
            new Short((short) -25),
            new Long(555555l),
            new Byte((byte) 120),
            new Short((short) 128) };

        ByteBuffer b1 = marshallValues(desc, before);
        b1.flip();
        Object[] after = unmarshallValues(desc, b1);
        assertTrue(compareObjects(before, after));
    }

    public void testNumericMaximums()
    {
        FennelStandardTypeDescriptor[] o1 = {
            FennelStandardTypeDescriptor.INT_32,
            FennelStandardTypeDescriptor.UINT_32,
            FennelStandardTypeDescriptor.INT_64,
            FennelStandardTypeDescriptor.UINT_64,
            FennelStandardTypeDescriptor.INT_16,
            FennelStandardTypeDescriptor.UINT_16,
            FennelStandardTypeDescriptor.INT_8,
            FennelStandardTypeDescriptor.UINT_8,
            FennelStandardTypeDescriptor.BOOL,
            FennelStandardTypeDescriptor.REAL,
            FennelStandardTypeDescriptor.DOUBLE };
        FennelTupleDescriptor desc = buildDescriptor(o1);

        Object[] maxVals = {
            new Integer(Integer.MAX_VALUE),
            new Long((((long)(Integer.MAX_VALUE))<<1)-1),
            new Long(Long.MAX_VALUE),
            new Long(Long.MAX_VALUE),  // FIXME
            new Short(Short.MAX_VALUE),
            new Integer((((int)(Short.MAX_VALUE))<<1)-1),
            new Byte(Byte.MAX_VALUE),
            new Short((short) 255),
            new Boolean(true),
            new Float(Float.MAX_VALUE),
            new Double(Double.MAX_VALUE) };

        ByteBuffer buff = marshallValues(desc, maxVals);
        /*
        System.out.println("testing maximum values, post-marshall position is " +
                            buff.position());
                            */
        buff.flip();
        Object[] after = unmarshallValues(desc, buff);
        assertTrue(compareObjects(maxVals, after));
    }

    public void testNumericMinimums()
    {
        FennelStandardTypeDescriptor[] o1 = {
            FennelStandardTypeDescriptor.INT_32,
            FennelStandardTypeDescriptor.UINT_32,
            FennelStandardTypeDescriptor.INT_64,
            FennelStandardTypeDescriptor.UINT_64,
            FennelStandardTypeDescriptor.INT_16,
            FennelStandardTypeDescriptor.UINT_16,
            FennelStandardTypeDescriptor.INT_8,
            FennelStandardTypeDescriptor.UINT_8,
            FennelStandardTypeDescriptor.BOOL,
            FennelStandardTypeDescriptor.REAL,
            FennelStandardTypeDescriptor.DOUBLE };
        FennelTupleDescriptor desc = buildDescriptor(o1);

        Object[] minVals = {
            new Integer(Integer.MIN_VALUE),
            new Long(0L),
            new Long(Long.MIN_VALUE),
            new Long(0L),  // FIXME
            new Short(Short.MIN_VALUE),
            new Integer(0),
            new Byte(Byte.MIN_VALUE),
            new Short((short) 0),
            new Boolean(false),
            new Float(Float.MAX_VALUE),
            new Double(Double.MAX_VALUE) };
        ByteBuffer buff = marshallValues(desc, minVals);
        /*
        System.out.println("testing minimum values, post-marshall position is " +
                            buff.position());
        */
        buff.flip();
        Object[] after = unmarshallValues(desc, buff);
        assertTrue(compareObjects(minVals, after));
    }

    public void testNullables()
    {
        FennelStandardTypeDescriptor[] o1 = {
            FennelStandardTypeDescriptor.INT_32,
            FennelStandardTypeDescriptor.UINT_32,
            FennelStandardTypeDescriptor.BOOL,
            FennelStandardTypeDescriptor.UINT_64,
            FennelStandardTypeDescriptor.REAL,
            FennelStandardTypeDescriptor.UINT_16,
            FennelStandardTypeDescriptor.INT_8,
            FennelStandardTypeDescriptor.UINT_8,
            FennelStandardTypeDescriptor.BOOL };
        FennelTupleDescriptor desc = buildDescriptor(o1,
                new boolean[] { true, true, true, true, true,
                                true, true, true, true},
                new int[] {0,0,0,0,0,0,0,0,0});

        Object[] nullVals = { null, null, null, null, null,
                              null, null, null, null };
        ByteBuffer buff = marshallValues(desc, nullVals);
        /*
        System.out.println("testing nullable values, post-marshall position is " +
                            buff.position());
                            */
        buff.flip();
        Object[] after = unmarshallValues(desc, buff);
        assertTrue(compareObjects(nullVals, after));
    }

    public void testStrings()
    {
        FennelStandardTypeDescriptor[] o1 = {
            FennelStandardTypeDescriptor.CHAR,
            FennelStandardTypeDescriptor.VARCHAR,
            FennelStandardTypeDescriptor.VARCHAR,
            FennelStandardTypeDescriptor.CHAR };
        FennelTupleDescriptor desc = buildDescriptor(o1,
                new boolean[] { false, false, true, true },
                new int[] {10,20,12,8});

        Object[] stringVals = {
            new String("10bytes   "),
            new String("hi"),
            null,
            new String("6bytes  ") };

        ByteBuffer buff = marshallValues(desc, stringVals);
        /*
        System.out.println("testing string values, post-marshall position is " +
                            buff.position());
                            */
        buff.flip();
        Object[] after = unmarshallValues(desc, buff);
        /*
        for ( int i = 0; i < 4; i++ ) {
            if ( stringVals[i] == null ) {
                continue;
            }
            System.out.println( "obj" + i + 
                                ": before size is: " + ((String) stringVals[i]).length() +
                                ", after size is: " + ((String)after[i]).length() );
        }
        */
        assertTrue(compareObjects(stringVals, after));
    }

    public void testBinaries()
    {
        FennelStandardTypeDescriptor[] o1 = {
            FennelStandardTypeDescriptor.BINARY,
            FennelStandardTypeDescriptor.VARBINARY,
            FennelStandardTypeDescriptor.VARBINARY,
            FennelStandardTypeDescriptor.BINARY };
        FennelTupleDescriptor desc = buildDescriptor(o1,
                new boolean[] { false, false, true, true },
                new int[] {10,20,12,8});

        Object[] binaryVals = {
            new String("10bytes   "),
            new String("hi"),
            null,
            new String("6bytes  ") };

        ByteBuffer buff = marshallValues(desc, binaryVals);
        /*
        System.out.println("testing binary values, post-marshall position is " +
                            buff.position());
                            */
        buff.flip();
        Object[] after = unmarshallValues(desc, buff);
        assertTrue(compareObjects(binaryVals, after));
    }

    public void __testUnsuppliedThrows()
    {
        /* note - this doesn't work, due to an assert in the
         * FennelTupleAccessor.marshal() method (which is also in
         * fennel). I think we should be throwing an exception
         * rather than an assert in the tuple library, but that
         * needs to be argued with the fennel library
         */
        for (Iterator iterator = FennelStandardTypeDescriptor.enumeration.iterator(); iterator.hasNext();) {
            FennelStandardTypeDescriptor fennelStandardTypeDescriptor = (FennelStandardTypeDescriptor) iterator.next();
            FennelTupleDescriptor desc =
                buildDescriptor( new FennelStandardTypeDescriptor[] {fennelStandardTypeDescriptor} );
            try {
                ByteBuffer buff = marshallValues(desc, new Object[]{null} );
                Util.discard(buff);
                fail("should have thrown an exception");
            } catch (NullPointerException e ){
            }
        }
    }

    /**
     * test the ability to marshall multiple times using the same
     * tupledescriptor, accessor and bytebuffer
     */
    public void testMultipleMarshalling()
    {
        FennelStandardTypeDescriptor[] o1 = {
            FennelStandardTypeDescriptor.VARCHAR,
            FennelStandardTypeDescriptor.INT_32,
            FennelStandardTypeDescriptor.VARCHAR,
            FennelStandardTypeDescriptor.UINT_32 };
        FennelTupleDescriptor desc = buildDescriptor(o1,
                new boolean[] { false, false, false, true },
                new int[] {40,0,120,0} );

        final int COUNT = 20;
        Object[] initialVals = new Object[COUNT];
        for (int i = 0; i < COUNT; ++i) {
            initialVals[i] = new Object[] {
                new String("Value " + i),
                new Integer(i),
                new String("this is very cool data - iteration " + i ),
                new Long((long)i * 20) };
        }

        ByteBuffer iBuff = ByteBuffer.allocate(50000);
        FennelTupleAccessor a = new FennelTupleAccessor();
        a.compute(desc);
        assertTrue(desc.getAttrCount() == a.size());

        FennelTupleData d = new FennelTupleData(desc);
        assertTrue(d.getDatumCount() == a.size());

        int iters;
        for ( iters = 0; iters < COUNT; ++iters ) {
            ByteBuffer sliceBuff = marshallValues(
                desc,
                (Object []) initialVals[iters],
                iBuff.slice(),
                a,
                d );
            int newPosition = iBuff.position() + sliceBuff.position();
            while ((newPosition & 0x3) != 0) {
                newPosition++;
            }
            iBuff.position( newPosition );
            /*
            System.out.println("Multi-marshall iter " + (iters+1) + 
                               ": buffer position is now " + iBuff.position());
                               */
        }
        /*
        System.out.println("Multi-marshall: after " + COUNT + " marshalls, the buffer is now " +
                           iBuff.position() + " bytes in size");
                           */

        iBuff.flip();
        for ( iters = 0; iters < COUNT; ++iters ) {
            ByteBuffer uBuff = iBuff.slice();
            Object[] objs = unmarshallValues(desc, uBuff, a, d );
            assertTrue(compareObjects( (Object[]) initialVals[iters], objs));
            int newPosition = iBuff.position() + uBuff.position();
            while ((newPosition & 0x3) != 0) {
                newPosition++;
            }
            iBuff.position( newPosition );
            /*
            System.out.println("Multi-unmarshall iter " + (iters+1) + 
                               ": buffer position is now " + iBuff.position());
                               */
        }
        /*
        System.out.println("Multi-marshall: after " + COUNT + 
                           " unmarshalls, the buffer is now at position " + iBuff.position() + 
                           " of the limit " + iBuff.limit() );
                           */
        assertTrue( iBuff.position() == iBuff.limit() );
    }

    public void testLargeVarBuffers()
    {
        FennelStandardTypeDescriptor[] o1 = {
            FennelStandardTypeDescriptor.CHAR,
            FennelStandardTypeDescriptor.VARCHAR,
            FennelStandardTypeDescriptor.VARBINARY };
        FennelTupleDescriptor desc = buildDescriptor(o1,
                new boolean[] { false, false, false },
                new int[] {40000,2000,5000});

        Object[] stringVals = {
            new String("10bytes   "),
            new String("hi"),
            new String("howdy") };

        ByteBuffer buff = marshallValues(desc, stringVals);
        /*
        System.out.println("testing string values, post-marshall position is " +
                            buff.position());
                            */
        buff.flip();
        Object[] after = unmarshallValues(desc, buff);
        /*
        for ( int i = 0; i < 3; i++ ) {
            if ( stringVals[i] == null ) {
                continue;
            }
            System.out.println( "obj" + i + 
                                ": before size is: " + ((String) stringVals[i]).length() +
                                ", after size is: " + ((String)after[i]).length() );
        }
        */
        assertTrue(compareObjects(stringVals, after));
    }

    public static Test suite()
    {
        TestSuite ts = new TestSuite();

        ts.addTestSuite(FennelTupleTest.class);
        return ts;
    }

    public static void main(String args[])
    {
        junit.textui.TestRunner.run(suite());
    }
}

// End FennelTupleTest.java
