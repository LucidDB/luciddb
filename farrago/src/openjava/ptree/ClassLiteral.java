/*
 * ClassLiteral.java 1.0
 *
 *
 * Jun 20, 1997
 * Oct 10, 1997
 *
 * @see openjava.ptree.ParseTree
 * @version 1.0 last updated:  Sep 29, 1997
 * @author  Michiaki Tatsubori
 */
package openjava.ptree;


import openjava.ptree.util.*;
import openjava.mop.*;
import java.io.OutputStream;



/**
 * The <code>ClassLiteral</code> class represents
 * an expression as a object of <code>Class</code> class,
 * which is suppoted since JDK 1.1.
 * This is like :
 * <br><blockquote><pre>
 *     String.class
 * </pre></blockquote><br>
 * or :
 * <br><blockquote><pre>
 *     int.class
 * </pre></blockquote><br>
 * 
 * @see java.lang.Class
 * @see openjava.ptree.Leaf
 * @see openjava.ptree.Expression
 * @see openjava.ptree.TypeName
 */
public class ClassLiteral extends NonLeaf
    implements Expression
{

    /**
     * Allocates a new object.
     *
     */
    public ClassLiteral( TypeName type ) {
	super();
	set(type);
    }

    public ClassLiteral( OJClass type ) {
	this( TypeName.forOJClass( type ) );
    }

    /**
     * Gets the type name of this class literal.
     *
     * @return  the type name.
     */
    public TypeName getTypeName() {
	return (TypeName) elementAt(0);
    }

    /**
     * Sets the type name of this class literal.
     *
     * @param type  the type name.
     */
    public void setTypeName(TypeName type) {
	set(type);
    }

    public OJClass getType( Environment env )
	throws Exception
    {
	return OJClass.forClass( Class . class );
    }

    public void accept( ParseTreeVisitor v ) throws ParseTreeException {
        v.visit( this );
    }

}
