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
import net.sf.farrago.fennel.tuple.*;


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
        int[] types,
        boolean[] nullable,
        int[] sizes)
    {
        FennelStoredTypeDescriptorFactory typeFactory =
            FennelStandardTypeDescriptorFactory.getInstance();
        FennelTupleDescriptor d = new FennelTupleDescriptor();

        int i;
        for (i = 0; i < types.length; ++i) {
            d.add(new FennelTupleAttributeDescriptor(
                typeFactory.newDataType(types[i]),
                nullable[i],
                sizes[i]));
        }
        return d;
    }

    private FennelTupleDescriptor buildDescriptor(int[] types)
    {
        FennelStoredTypeDescriptorFactory typeFactory =
            FennelStandardTypeDescriptorFactory.getInstance();
        FennelTupleDescriptor d = new FennelTupleDescriptor();

        int i;
        for (i = 0; i < types.length; ++i) {
            d.add(new FennelTupleAttributeDescriptor(
                typeFactory.newDataType(types[i]), false, 0));
        }
        return d;
    }

    private boolean compareObjects(Object[] before, Object[] after)
    {
        //System.out.println("comparing " + before.length + " values ");
        assertTrue(before.length == after.length);
        for (int i = 0; i < before.length; ++i) {
            if (before[i] == null && after[i] == null) {
                continue;
            }
            /*
             * due to an artifact of the way we unmarshall fixed length
             * arrays into Strings, it's possible to get a much larger
             * string out of an unmarshall than we put into a marshall.
             * If it's a string, just make sure the output starts with
             * the input.
             */
            if ( before[i] instanceof String ) {
                assertTrue(
                    "string " + i + " should match",
                    ((String) after[i]).startsWith((String) before[i]));
            } else {
                assertEquals(
                    "object " + i + " should match",
                    before[i], after[i]);
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
                case FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_INT_8:
                    d.getDatum(i).setByte(((Byte) objs[i]).byteValue());
                    break;
                case FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_UINT_8:
                    d.getDatum(i).setUnsignedByte(
                        ((Short) objs[i]).shortValue());
                    break;
                case FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_INT_16:
                    d.getDatum(i).setShort(((Short) objs[i]).shortValue());
                    break;
                case FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_UINT_16:
                    d.getDatum(i).setUnsignedShort(
                        ((Integer) objs[i]).intValue());
                    break;
                case FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_INT_32:
                    d.getDatum(i).setInt(((Integer) objs[i]).intValue());
                    break;
                case FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_UINT_32:
                    d.getDatum(i).setUnsignedInt(((Long) objs[i]).longValue());
                    break;
                case FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_INT_64:
                    d.getDatum(i).setLong(((Long) objs[i]).longValue());
                    break;
                case FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_UINT_64:
                    d.getDatum(i).setUnsignedLong(
                        ((Long) objs[i]).longValue());
                    break;
                case FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_BOOL:
                    d.getDatum(i).setBoolean(
                        ((Boolean) objs[i]).booleanValue());
                    break;
                case FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_REAL:
                    d.getDatum(i).setFloat(((Float) objs[i]).floatValue());
                    break;
                case FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_DOUBLE:
                    d.getDatum(i).setDouble(((Double) objs[i]).doubleValue());
                    break;
                case FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_CHAR:
                    d.getDatum(i).setString(((String) objs[i]));
                    break;
                case FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_VARCHAR:
                    d.getDatum(i).setString(((String) objs[i]));
                    break;
                case FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_BINARY:
                    d.getDatum(i).setBytes(((String) objs[i]).getBytes());
                    break;
                case FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_VARBINARY:
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
            
            switch(desc.getAttr(i).typeDescriptor.getOrdinal()) {
                case FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_INT_8:
                    o[i] = new Byte(d.getDatum(i).getByte());
                    break;
                case FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_UINT_8:
                    o[i] = new Short(d.getDatum(i).getUnsignedByte());
                    break;
                case FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_INT_16:
                    o[i] = new Short(d.getDatum(i).getShort());
                    break;
                case FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_UINT_16:
                    o[i] = new Integer(d.getDatum(i).getUnsignedShort());
                    break;
                case FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_INT_32:
                    o[i] = new Integer(d.getDatum(i).getInt());
                    break;
                case FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_UINT_32:
                    o[i] = new Long(d.getDatum(i).getUnsignedInt());
                    break;
                case FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_INT_64:
                    o[i] = new Long(d.getDatum(i).getLong());
                    break;
                case FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_UINT_64:
                    o[i] = new Long(d.getDatum(i).getUnsignedLong());
                    break;
                case FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_BOOL:
                    o[i] = new Boolean(d.getDatum(i).getBoolean());
                    break;
                case FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_REAL:
                    o[i] = new Float(d.getDatum(i).getFloat());
                    break;
                case FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_DOUBLE:
                    o[i] = new Double(d.getDatum(i).getDouble());
                    break;
                case FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_CHAR:
                    o[i] = new String(
                        d.getDatum(i).getBytes(),
                        0,
                        d.getDatum(i).getLength());
                    break;
                case FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_VARCHAR:
                    o[i] = new String(
                        d.getDatum(i).getBytes(),
                        0,
                        d.getDatum(i).getLength());
                    break;
                case FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_BINARY:
                    o[i] = new String(
                        d.getDatum(i).getBytes(),
                        0,
                        d.getDatum(i).getLength());
                    break;
                case FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_VARBINARY:
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

    public void testMinimal()
    {
        int[] o1 = {
            FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_INT_32,
            FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_INT_16,
            FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_UINT_32,
            FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_INT_8,
            FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_UINT_8 };
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
        int[] o1 = {
            FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_INT_32,
            FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_UINT_32,
            FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_INT_64,
            FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_UINT_64,
            FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_INT_16,
            FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_UINT_16,
            FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_INT_8,
            FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_UINT_8,
            FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_BOOL,
            FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_REAL,
            FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_DOUBLE };
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
        int[] o1 = {
            FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_INT_32,
            FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_UINT_32,
            FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_INT_64,
            FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_UINT_64,
            FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_INT_16,
            FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_UINT_16,
            FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_INT_8,
            FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_UINT_8,
            FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_BOOL,
            FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_REAL,
            FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_DOUBLE };
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
        int[] o1 = {
            FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_INT_32,
            FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_UINT_32,
            FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_BOOL,
            FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_UINT_64,
            FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_REAL,
            FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_UINT_16,
            FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_INT_8,
            FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_UINT_8,
            FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_BOOL };
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
        int[] o1 = {
            FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_CHAR,
            FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_VARCHAR,
            FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_VARCHAR,
            FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_CHAR };
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
        int[] o1 = {
            FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_BINARY,
            FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_VARBINARY,
            FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_VARBINARY,
            FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_BINARY };
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
        int ordinal;
        for (ordinal = FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_MIN;
             ordinal < FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_END;
             ++ordinal) {
            FennelTupleDescriptor desc =
                buildDescriptor( new int[] {ordinal} );
            try {
                ByteBuffer buff = marshallValues(desc, new Object[]{null} );
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
        int[] o1 = {
            FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_VARCHAR,
            FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_INT_32,
            FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_VARCHAR,
            FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_UINT_32 };
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
        int[] o1 = {
            FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_CHAR,
            FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_VARCHAR,
            FennelStandardTypeDescriptorOrdinal.STANDARD_TYPE_VARBINARY };
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
