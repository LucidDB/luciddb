/*
 * OJPrimitive.java
 *
 * Now deprecated and replaced with openjava.mop.OJSystem.
 *
 * @author   Michiaki Tatsubori
 * @version  %VERSION% %DATE%
 * @see      java.lang.Object
 *
 * COPYRIGHT 1999 by Michiaki Tatsubori, ALL RIGHTS RESERVED.
 */
package openjava.mop;


import java.lang.Object;


/**
 * Defines primitive types.
 *
 * @deprecated Replaced by {@link openjava.mop.OJSystem}
 * @author   Michiaki Tatsubori
 * @version  1.0
 * @since    %SOFTWARE% 1.0
 * @see openjava.mop.OJSystem
 */
public abstract class OJPrimitive
{
    public static final OJClass VOID = OJClass.forClass( void . class );
    public static final OJClass BYTE = OJClass.forClass( byte . class );
    public static final OJClass CHAR = OJClass.forClass( char . class );
    public static final OJClass INT = OJClass.forClass( int . class );
    public static final OJClass LONG = OJClass.forClass( long . class );
    public static final OJClass FLOAT = OJClass.forClass( float . class );
    public static final OJClass DOUBLE = OJClass.forClass( double . class );
    public static final OJClass STRING = OJClass.forClass( String . class );
    public static final OJClass OBJECT = OJClass.forClass( Object . class );

}
