/*
 * OJMember.java
 *
 * comments here.
 *
 * @author   Michiaki Tatsubori
 * @version  %VERSION% %DATE%
 * @see      java.lang.Object
 *
 * COPYRIGHT 1999 by Michiaki Tatsubori, ALL RIGHTS RESERVED.
 */
package openjava.mop;


/**
 * The class <code>OJMember</code> is equivalent to Member
 *
 *
 * @author   Michiaki Tatsubori
 * @version  1.0
 * @since    %SOFTWARE% 1.0
 * @see	java.lang.reflect.Member
 */
public interface OJMember {
    public static final int PUBLIC = 0;
    public static final int DECLARED = 1;

    public OJClass getDeclaringClass();
    public String getName();
    public OJModifier getModifiers();
    public Signature signature();
    public Environment getEnvironment();
}
