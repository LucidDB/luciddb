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

package net.sf.farrago.ddl;

/**
 * DdlStoredElement defines an interface which should be implemented by any
 * catalog objects which correspond to physical storage.  Note that this
 * interface is only used after all validation has been performed.
 * Implementations may update the catalog (e.g. to record the physical ID of
 * a created object), but these updates do not result in further validation
 * triggers.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface DdlStoredElement extends DdlValidatedElement
{
    //~ Methods ---------------------------------------------------------------

    /**
     * Called to execute a DDL CREATE statement.  The implementation should
     * execute any storage management required to create the physical object.
     */
    public void createStorage(DdlValidator validator);

    /**
     * Called to execute a DDL DROP statement.  The implementation should
     * execute any storage management required to delete the physical object.
     */
    public void deleteStorage(DdlValidator validator);

    /**
     * Called to execute a DDL TRUNCATE statement.  The implementation should
     * execute any storage management required to truncate the physical
     * object.
     */
    public void truncateStorage(DdlValidator validator);
}


// End DdlStoredElement.java
