/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// (C) Copyright 2003-2004 John V. Sichi
// You must accept the terms in LICENSE.html to use this software.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

package net.sf.saffron.runtime;

/**
 * An array of <code>VarDecl</code>s is returned from the <code>dummy()</code>
 * method which is generated to implement a variable declaration, or a list
 * of statements which contain variable declarations.
 */
public class VarDecl
{
    //~ Instance fields -------------------------------------------------------

    public Class clazz;
    public Object value;
    public String name;

    //~ Constructors ----------------------------------------------------------

    public VarDecl(String name,Class clazz,Object value)
    {
        this.name = name;
        this.clazz = clazz;
        this.value = value;
    }
}


// End VarDecl.java
