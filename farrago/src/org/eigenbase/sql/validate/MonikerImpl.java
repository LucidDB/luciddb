/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2004-2005 The Eigenbase Project
// Copyright (C) 2004-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
//
// This program is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by the Free
// Software Foundation; either version 2 of the License, or (at your option)
// any later version approved by The Eigenbase Project.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/

package org.eigenbase.sql.validate;

/**
 * A generic implementation of {@link Moniker} 
 *
 * @author tleung
 * @since May 31, 2005
 * @version $Id$
 **/
public class MonikerImpl implements Moniker
{   
    String[] names;
    MonikerType type;

    public MonikerImpl(String[] names, MonikerType type) {
        this.names = names;
        this.type = type;
    }
    
    public MonikerImpl(String name, MonikerType type) {
        this.names = new String[] {name};
        this.type = type;
    }

    public MonikerType getType() {
        return type;
    }

    public String[] getFullyQualifiedNames() {
        return names;
    }

    public String getShortName() {
        return names[names.length-1];
    }

    public String toString() {
        StringBuffer result = new StringBuffer();;
        for (int i = 0; i < names.length; i++) {
            result.append(names[i]);
            if (i < names.length-1) {
                result.append(".");
            }
        }
        return result.toString();
    }
}
