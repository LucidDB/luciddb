package net.sf.farrago.fennel;

import java.util.Arrays;

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

    public static FennelPseudoUuid parse(String uuid)
    {
        assert(uuid.length() == 36);

        byte[] bytes = new byte[UUID_LENGTH];

        try {
            // one 4 byte int
            for(int i = 0; i < 4; i++) {
                int b = Integer.parseInt(uuid.substring(i * 2, i * 2 + 2), 16);

                bytes[i] = (byte)b;
            }
            assert(uuid.charAt(8) == '-');

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

                assert(uuid.charAt(start + 4) == '-');
            }

            // one 6-byte int
            for(int i = 0; i < 6; i++) {
                int b = Integer.parseInt(
                    uuid.substring(24 + i * 2, 24 + i * 2 + 2), 16);

                bytes[10 + i] = (byte)b;
            }
        } catch(NumberFormatException e) {
            assert(false): e.getMessage();
        }

        return new FennelPseudoUuid(bytes);
    }

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
