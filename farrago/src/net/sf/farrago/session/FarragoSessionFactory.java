/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
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

package net.sf.farrago.session;

import net.sf.farrago.parser.*;
import net.sf.farrago.catalog.*;

import java.util.*;

/**
 * FarragoSessionFactory defines an interface with factory methods used
 * to create sessions and related objects.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FarragoSessionFactory
{
    /**
     * Creates a new FarragoSession.
     *
     * @param url (same as for JDBC connect)
     *
     * @param info (same as for JDBC connect)
     *
     * @return new session
     */
    public FarragoSession newFarragoSession(
        String url,
        Properties info);

    /**
     * Creates a new parser which reads input from a string.
     *
     *<p>
     *
     * REVIEW jvs 19-Mar-2004:  It would be nice to define a
     * FarragoSessionParser interface.  However, it's a little tricky
     * since some of the parser interface relies on generated classes.
     *
     * @param catalog catalog containing metadata affected by parsing
     *
     * @param sql string to be parsed
     */
    public FarragoParser newFarragoParser(
        FarragoCatalog catalog,
        String sql);
}

// End FarragoSessionFactory.java
