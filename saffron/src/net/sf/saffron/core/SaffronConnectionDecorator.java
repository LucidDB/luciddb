/*
// Saffron preprocessor and data engine.
// Copyright (C) 2002-2004 Disruptive Tech
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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

package net.sf.saffron.core;

import org.eigenbase.relopt.*;


/**
 * Base for a class which wraps a {@link RelOptConnection} and extends its
 * functionality.
 *
 * <p>See {@link org.eigenbase.util.Glossary#DecoratorPattern the Decorator
 * Pattern}.
 *
 * @author jhyde
 * @since Dec 9, 2003
 * @version $Id$
 **/
public abstract class SaffronConnectionDecorator implements RelOptConnection
{
    /**
     * The underlying {@link RelOptConnection}.
     */
    protected final RelOptConnection connection;

    protected SaffronConnectionDecorator(RelOptConnection connection)
    {
        this.connection = connection;
    }

    public RelOptSchema getRelOptSchema()
    {
        return connection.getRelOptSchema();
    }

    public Object contentsAsArray(
        String qualifier,
        String tableName)
    {
        return connection.contentsAsArray(qualifier, tableName);
    }
}


// End SaffronConnectionDecorator.java
