/*
 * MemberDeclarationList.java 1.0
 *  
 * @see openjava.ptree.ParseTree
 * @version last updated: 06/11/97
 * @author  Michiaki Tatsubori
 */
package openjava.ptree;


import openjava.ptree.util.*;
import openjava.mop.*;
import java.io.OutputStream;


/**
 * The MemberDeclarationList class presents for the node of parse tree
 * of MemberDeclaration
 *
 */
public class MemberDeclarationList extends List
{
    private static final String LNLN = ParseTreeObject.LN + ParseTreeObject.LN;

    public MemberDeclarationList() {
	super( LNLN );
    }

    public MemberDeclarationList( MemberDeclaration e0 ) {
	super( LNLN, (ParseTree) e0 );
    }

    public MemberDeclarationList( MemberDeclaration e0, MemberDeclaration e1 ) {
	this( e0 );	add( e1 );
    }

    /**
     * Gets the specified element at the index.
     *
     * @param  n  index
     */
    public MemberDeclaration get( int n ) {
	return (MemberDeclaration) contents_elementAt( n );
    }

    /**
     * Adds the specified element after the list
     * This causes side-effect.
     *
     * @param  p  MemberDeclaration to be inserted into the list
     */
    public void add( MemberDeclaration p ) {
	contents_addElement( p );
    }

    /**
     * Adds the specified element after the list
     * This causes side-effect.
     *
     * @param  p  MemberDeclaration to be inserted into the list
     */
    public void set( int index, MemberDeclaration p ) {
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
    public MemberDeclaration remove( int index ) {
	MemberDeclaration removed
	    = (MemberDeclaration) contents_elementAt( index );
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
    public void insertElementAt( MemberDeclaration p, int n ) {
	contents_insertElementAt( p, n );
    }

    /**
     * Appends a list after this list.
     *
     * @param  lst  a list to be appended
     */
    public void addAll( MemberDeclarationList lst ) {
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
    public MemberDeclarationList subList( int from_index, int to_index ) {
	MemberDeclarationList result = new MemberDeclarationList();
	for (int i = from_index; i < to_index; ++i) {
	    result.add( this.get( i ) );
	}
	return result;
    }

    public void accept( ParseTreeVisitor v ) throws ParseTreeException {
        v.visit( this );
    }
}
