package net.sf.farrago.test;

import net.sf.farrago.fennel.FennelPseudoUuid;
import org.eigenbase.util.Util;

/**
 * FennelPseudoUuidTest tests the FennelPseudoUuid class.
 *
 * @author Stephan Zuercher
 * @version $Id$
 */
public class FenndelPseudoUuidTest
    extends FarragoTestCase
{
    static {
         Util.loadLibrary("farrago");
    }

    public FennelPseudoUuidTest(String name) throws Exception
    {
        super(name);
    }

    public void testValidPseudoUuid() throws Exception
    {
        FennelPseudoUuid uuid1 = FennelPseudoUuid.generate();

        assertEquals(uuid1, uuid1);
        assertNotNull(uuid1.toString());
        assertTrue(uuid1.toString().length() > 0);
    }

    public void testInvalidPseudoUuid() throws Exception
    {
        FennelPseudoUuid uuid1 = FennelPseudoUuid.generate();
        FennelPseudoUuid uuid2 = FennelPseudoUuid.generateInvalid();

        assertFalse(uuid1.equals(uuid2));
        assertNotNull(uuid2.toString());
        assertTrue(uuid2.toString().length() > 0);
    }

    public void testPseudoUuidSymmetry() throws Exception
    {
        FennelPseudoUuid uuid1 = FennelPseudoUuid.generate();
        FennelPseudoUuid uuid2 = FennelPseudoUuid.parse(uuid1.toString());
        assertEquals(uuid1, uuid2);

        String uuidStr = "01234567-89ab-cdef-0123-456789abcdef";
        FennelPseudoUuid uuid3 = FennelPseudoUuid.parse(uuidStr);

        byte[] uuidBytes = new byte[16];
        byte val = 0;
        for(int i = 0; i < 16; i++) {
            if (i == 0 || i == 8) {
                val = 0x01;
            }

            uuidBytes[i] = val;
            val += 0x22;
        }
        FennelPseudoUuid uuid4 = FennelPseudoUuid.parse(uuidBytes);

        assertEquals(uuid3, uuid4);
    }
}
