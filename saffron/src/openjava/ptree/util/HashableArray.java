/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// You must accept the terms in LICENSE.html to use this software.
*/

package openjava.ptree.util;

/**
 * <code>HashableArray</code> provides a <code>Object[]</code> with a {@link
 * #hashCode} and an {@link #equals} function, so it can be used as a key in a
 * {@link java.util.Hashtable}.
 */
class HashableArray
{
    Object[] a;

    HashableArray(Object[] a) {
	this.a = a;
    }
    // override Object
    public int hashCode() {
	return arrayHashCode(a);
    }
    // override Object
    public boolean equals(Object o) {
	return o instanceof HashableArray &&
	    arraysAreEqual(this.a, ((HashableArray) o).a);
    }

    static int arrayHashCode(Object[] a) {
	// hash algorithm borrowed from java.lang.String
	int h = 0;
	for (int i = 0; i < a.length; i++) {
	    h = 31 * h + a[i].hashCode();
	}
	return h;
    }

    /** Return whether two arrays are equal (shallow compare). */
    static boolean arraysAreEqual(Object[] a1, Object[] a2) {
	if (a1.length != a2.length) {
	    return false;
	}
	for (int i = 0; i < a1.length; i++) {
	    // we don't need to use 'equals', cuz synthetic classes are always
	    // made of real classes
	    if (!a1[i].equals(a2[i])) {
		return false;
	    }
	}
	return true;
    }
};


// End HashableArray.java
