/*
 * ArrayInitializer.java 1.0
 * 
 * Jun 11, 1997 by mich
 * Oct 10, 1997 by mich
 *
 * @see openjava.ptree.ParseTree
 * @version last updated:  Oct 10, 1997
 * @author  Michiaki Tatsubori
 */
package openjava.ptree;


import openjava.ptree.util.*;


/**
 * The ArrayInitializer class presents initializer list of array elements.
 *
 * @see openjava.ptree.ParseTree
 * @see openjava.ptree.List
 * @see openjava.ptree.VariableInitializer
 */
public class ArrayInitializer extends List
    implements VariableInitializer
{

    private boolean _isRemainderOmitted = false;

    /**
     * Allocates a new ArrayInitializer.
     *
     * @param arrayinit prototype object
     */
    public ArrayInitializer() {
	super( ", " );
    }

    public ArrayInitializer( VariableInitializer vi ) {
        this();
	add( vi );
    }

    public ArrayInitializer( ExpressionList exprs ) {
	this();
	int len = exprs.size();
	for (int i = 0; i < len; ++i) {
	    add( exprs.get( i ) );
	}
    }

    public void writeCode() {
	writeDebugL();

	out.print( "{ " );
	super.writeCode();
	if (isRemainderOmitted())  out.print( "," );
	out.print( " }" );

	writeDebugR();
    }

    public void omitRemainder( boolean is_omitted ) {
	this._isRemainderOmitted = is_omitted;
    }
    public void omitRemainder() {
	this._isRemainderOmitted = true;
    }
    public boolean isRemainderOmitted() {
	return this._isRemainderOmitted;
    }

    /**
     * Gets the specified element at the index.
     *
     * @param  n  index
     */
    public VariableInitializer get( int n ) {
	return (VariableInitializer) contents_elementAt( n );
    }

    /**
     * Adds the specified element after the list
     * This causes side-effect.
     *
     * @param  p  VariableInitializer to be inserted into the list
     */
    public void add( VariableInitializer p ) {
	contents_addElement( p );
    }

    /**
     * Adds the specified element after the list
     * This causes side-effect.
     *
     * @param  p  VariableInitializer to be inserted into the list
     */
    public void set( int index, VariableInitializer p ) {
	contents_setElementAt( p, index );
    }

    /**
     * Removes the element at the specified position in this Vector.
     * shifts any subsequent elements to the left (subtracts one from their
     * indices).  Returns the element that was removed from the Vector.
     *
     * @exception ArrayIndexOutOfBoundsException index out of range (index
     *            &lt; 0 || index &gt;= size()).
     * @param index the index of the element to removed.
     * @since JDK1.2
     */
    public VariableInitializer remove( int index ) {
	VariableInitializer removed
	    = (VariableInitializer) contents_elementAt( index );
	contents_removeElementAt( index );
	return removed;
    }


    /**
     * Inserts the specified element into the list
     * before the specified element of the list.
     * This causes side-effect.
     *
     * @param  p  the element to be inserted into the list
     * @param  n  number of the element before which insertion ocuurs
     */
    public void insertElementAt( VariableInitializer p, int n ) {
	contents_insertElementAt( p, n );
    }

    /**
     * Appends a list after this list.
     *
     * @param  lst  a list to be appended
     */
    public void addAll( ArrayInitializer lst ) {
	for( int i = 0, len = lst.size(); i < len; i++ ){
	    contents_addElement( lst.get( i ) );
	}
    }

    /**
     * Returns a view of the portion of this List between fromIndex,
     * inclusive, and toIndex, exclusive.  The returned List is backed by this
     * List, but changes in the returned List are not reflected in this List.
     * <p>
     *
     * @param fromIndex low endpoint (inclusive) of the subList.
     * @param toKey high endpoint (exclusive) of the subList.
     * @return a view of the specified range within this List.
     * @exception IndexOutOfBoundsException Illegal endpoint index value
     *     (fromIndex &lt; 0 || toIndex &gt; size || fromIndex &gt; toIndex).
     */
    public ArrayInitializer subList( int from_index, int to_index ) {
	ArrayInitializer result = new ArrayInitializer();
	for (int i = from_index; i < to_index; ++i) {
	    result.add( this.get( i ) );
	}
	return result;
    }

    public void accept( ParseTreeVisitor v ) throws ParseTreeException {
        v.visit( this );
    }
}
