/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
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

package net.sf.saffron.rel;

import net.sf.saffron.opt.VolcanoCluster;


/**
 * todo:
 *
 * @author jhyde
 * @version $Id$
 *
 * @since 23 September, 2001
 */
public class IntersectRel extends UnionRel
{
    //~ Constructors ----------------------------------------------------------

    public IntersectRel(
        VolcanoCluster cluster,
        SaffronRel left,
        SaffronRel right)
    {
        super(cluster,new SaffronRel [] { left,right },false);
    }
}


// End IntersectRel.java
