/*
 * CaseGroupList.java 1.0
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
 * The CaseGroupList class presents for the node of parse tree
 * of CaseGroup
 *
 */
public class CaseGroupList extends List
{
    private static final String LNLN = ParseTreeObject.LN + ParseTreeObject.LN;

    public CaseGroupList() {
	super( LNLN );
    }

    public CaseGroupList( CaseGroup e0 ) {
	super( LNLN, (ParseTree) e0 );
    }

    /**
     * Gets the specified element at the index.
     *
     * @param  n  index
     */
    public CaseGroup get( int n ) {
	return (CaseGroup) contents_elementAt( n );
    }

    /**
     * Adds the specified element after the list
     * This causes side-effect.
     *
     * @param  p  CaseGroup to be inserted into the list
     */
    public void add( CaseGroup p ) {
	contents_addElement( p );
    }

    /**
     * Adds the specified element after the list
     * This causes side-effect.
     *
     * @param  p  CaseGroup to be inserted into the list
     */
    public void set( int index, CaseGroup p ) {
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
    public CaseGroup remove( int index ) {
	CaseGroup removed
	    = (CaseGroup) contents_elementAt( index );
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
    public void insertElementAt( CaseGroup p, int n ) {
	contents_insertElementAt( p, n );
    }

    /**
     * Appends a list after this list.
     *
     * @param  lst  a list to be appended
     */
    public void addAll( CaseGroupList lst ) {
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
    public CaseGroupList subList( int from_index, int to_index ) {
	CaseGroupList result = new CaseGroupList();
	for (int i = from_index; i < to_index; ++i) {
	    result.add( this.get( i ) );
	}
	return result;
    }

    public void accept( ParseTreeVisitor v ) throws ParseTreeException {
        v.visit( this );
    }
}
