/*
 * OJModifier.java
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


import java.lang.reflect.Modifier;
import openjava.ptree.*;


/**
 * The class <code>OJModifier</code> extends
 * <code>java.lang.relfect.Modifier</code> to support user defined
 * keywords.
 *
 * @author   Michiaki Tatsubori
 * @version  1.0
 * @since    %SOFTWARE% 1.0
 * @see java.lang.reflect.Modifier
 **/
public class OJModifier
{
    /*
     * Access modifier flag constants from <em>The Java Virtual
     * Machine Specification</em>, Table 4.1.
     */
    public static final int PUBLIC       = Modifier.PUBLIC;
    public static final int PRIVATE      = Modifier.PRIVATE;
    public static final int PROTECTED    = Modifier.PROTECTED;
    public static final int STATIC       = Modifier.STATIC;
    public static final int FINAL        = Modifier.FINAL;
    public static final int SYNCHRONIZED = Modifier.SYNCHRONIZED;
    public static final int VOLATILE     = Modifier.VOLATILE;
    public static final int TRANSIENT    = Modifier.TRANSIENT;
    public static final int NATIVE       = Modifier.NATIVE;
    public static final int INTERFACE    = Modifier.INTERFACE;
    public static final int ABSTRACT     = Modifier.ABSTRACT;

    private int javaModifier = 0;
    private String userModifiers[] = null;

    private static OJModifier _constantEmpty = null;

    OJModifier( int mod ) {
	javaModifier = mod;
	userModifiers = new String[0];
    }

    OJModifier( int mod, String[] user_modifs ) {
	javaModifier = mod;
	userModifiers = user_modifs;
    }

    public static final OJModifier constantEmpty() {
	if (_constantEmpty == null)  _constantEmpty = new OJModifier( 0 );
	return _constantEmpty;
    }

    /**
     * Returns the specifier int for modifiers in regular Java.
     *
     * @return  integer specifier
     * @see java.lang.reflect.Modifier
     */
    public int toModifier() {
	return javaModifier;
    }

    public static OJModifier forModifier( int mod ) {
	return new OJModifier( mod );
    }

    public static OJModifier forParseTree( ModifierList ptree ) {
	int regular_modifs = ptree.getRegular();
	String[] oj_modifs = new String[ptree.size()];
	for (int i = 0; i < ptree.size(); ++i) {
	    oj_modifs[i] = ptree.get( i );
	}
	return new OJModifier( regular_modifs, oj_modifs );
    }

    /**
     * Returns true if this modifier includes the <tt>public</tt>
     * modifier.
     */
    public final boolean isPublic() {
        return ((javaModifier & PUBLIC) != 0);
    }

    /**
     * Returns true if this modifier includes the <tt>private</tt>
     * modifier.
     */
    public final boolean isPrivate() {
        return ((javaModifier & PRIVATE) != 0);
    }

    /**
     * Returns true if this modifier includes the <tt>protected</tt>
     * modifier.
     */
    public final boolean isProtected() {
        return ((javaModifier & PROTECTED) != 0);
    }

    /**
     * Returns true if this modifier includes the <tt>static</tt>
     * modifier.
     */
    public final boolean isStatic() {
        return ((javaModifier & STATIC) != 0);
    }

    /**
     * Returns true if this modifier includes the <tt>final</tt>
     * modifier.
     */
    public final boolean isFinal() {
        return ((javaModifier & FINAL) != 0);
    }

    /**
     * Returns true if this modifier includes the <tt>synchronized</tt>
     * modifier.
     */
    public final boolean isSynchronized() {
        return ((javaModifier & SYNCHRONIZED) != 0);
    }

    /**
     * Returns true if this modifier includes the <tt>volatile</tt>
     * modifier.
     */
    public final boolean isVolatile() {
        return ((javaModifier & VOLATILE) != 0);
    }

    /**
     * Returns true if this modifier includes the <tt>transient</tt>
     * modifier.
     */
    public final boolean isTransient() {
        return ((javaModifier & TRANSIENT) != 0);
    }

    /**
     * Returns true if this modifier includes the <tt>native</tt>
     * modifier.
     */
    public final boolean isNative() {
        return ((javaModifier & NATIVE) != 0);
    }

    /**
     * Returns true if this modifier includes the <tt>interface</tt>
     * modifier.
     */
    public final boolean isInterface() {
        return ((javaModifier & INTERFACE) != 0);
    }

    /**
     * Returns true if this modifier includes the <tt>abstract</tt>
     * modifier.
     */
    public final boolean isAbstract() {
        return ((javaModifier & ABSTRACT) != 0);
    }

    /**
     *
     */
    public final boolean has( String str ) {
	for (int i = 0; i < userModifiers.length; ++i) {
	    if (userModifiers[i].equals( str ))  return true;
	}
	return false;
    }

    /**
     * Generates a string describing the access modifier flags
     * without user modifiers.
     * For example:
     * <pre>
     *    public final synchronized
     *    private transient volatile
     * </pre>
     * The modifier names are return in canonical order, as
     * specified by <em>The Java Language Specification<em>.
     */
    public String toString() {
	return Modifier.toString( javaModifier );
    }

    private static final int ACCESS = PUBLIC | PROTECTED | PRIVATE;
    private static final int INHERIT = ABSTRACT | FINAL;

    public OJModifier add( int mods ) {
        if ((mods & ACCESS) != 0) {
	    return new OJModifier( removedModifier( ACCESS ) | mods );
	}
        return new OJModifier( toModifier() | mods );
    }

    public OJModifier remove( int mods ) {
        return new OJModifier( removedModifier( mods ) );
    }

    private final int removedModifier( int mods ) {
        int toBeRemoved = this.toModifier() & mods;
        return this.toModifier() - toBeRemoved;
    }

    public OJModifier setPublic() {
	return new OJModifier( removedModifier( ACCESS ) | PUBLIC );
    }

    public OJModifier setProtected() {
	return new OJModifier( removedModifier( ACCESS ) | PROTECTED );
    }

    public OJModifier setPrivate() {
	return new OJModifier( removedModifier( ACCESS ) | PRIVATE );
    }

    public OJModifier setPackaged() {
	return new OJModifier( removedModifier( ACCESS ) );
    }

    public OJModifier setAbstract() {
	return new OJModifier( removedModifier( INHERIT ) | ABSTRACT );
    }

    public OJModifier setFinal() {
	return new OJModifier( removedModifier( INHERIT ) | FINAL );
    }

}
