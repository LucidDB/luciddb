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
package net.sf.farrago.cwm.relational;

import net.sf.farrago.catalog.*;
import net.sf.farrago.ddl.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.util.*;

import org.netbeans.mdr.handlers.*;
import org.netbeans.mdr.storagemodel.*;


// TODO: refine the Farrago package dependency diagram to show that the Impl
// portion of cwm is above type, fennel, catalog, etc.  Also, in the fullness
// of time, fem should depend on cwm.

/**
 * CwmSqlindexImpl is a custom implementation for CwmSqlindex.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class CwmSqlindexImpl extends InstanceHandler
    implements CwmSqlindex,
        DdlStoredElement
{
    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new CwmSqlindexImpl object.
     *
     * @param storable .
     */
    protected CwmSqlindexImpl(StorableObject storable)
    {
        super(storable);
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * .
     *
     * @return the table which this index spans
     */
    public CwmTable getTable()
    {
        return (CwmTable) getSpannedClass();
    }

    // implement DdlStoredElement
    public void createStorage(DdlValidator validator)
    {
        if (getTable().isTemporary()) {
            // definition of a temporary table should't create any real storage
            return;
        }

        validator.getIndexMap().createIndexStorage(
            validator.getDataWrapperCache(), this);

        // TODO:  index existing rows; for now, creating an index on a
        // non-empty table will leave the index (incorrectly) empty
    }

    // implement DdlStoredElement
    public void deleteStorage(DdlValidator validator)
    {
        // TODO: For a temporary table, need to drop storage for ALL sessions.
        // For now, storage from other sessions becomes garbage which will be
        // collected as those sessions close (or on recovery in case of a
        // crash).
        validator.getIndexMap().dropIndexStorage(
            validator.getDataWrapperCache(), this, false);
    }

    // implement DdlStoredElement
    public void truncateStorage(DdlValidator validator)
    {
        validator.getIndexMap().dropIndexStorage(
            validator.getDataWrapperCache(), this, true);
    }

    // implement DdlValidatedElement
    public void validateDefinition(
        DdlValidator validator,
        boolean creation)
    {
        // indexes are never modified after creation
        assert (creation);

        if (getTable().isTemporary()) {
            if (!validator.isCreatedObject(getTable())) {
                // REVIEW: support this?  What to do about instances of the
                // same temporary table in other sessions?
                throw validator.res.newValidatorIndexOnExistingTempTable(
                    validator.getRepos().getLocalizedObjectName(this, null),
                    validator.getRepos().getLocalizedObjectName(
                        getTable(),
                        null));
            }
        }

        // TODO:  verify columns distinct, total width acceptable, and all
        // columns indexable types
        setSorted(true);
        if (getNamespace() != null) {
            assert (getNamespace().equals(getTable().getNamespace()));
        } else {
            setNamespace(getTable().getNamespace());
        }

        setFilterCondition("TRUE");
    }

    // implement DdlValidatedElement
    public void validateDeletion(
        DdlValidator validator,
        boolean truncation)
    {
        if (truncation) {
            return;
        }

        if (validator.isDeletedObject(getTable())) {
            // This index is being deleted together with its containing table,
            // which is always OK.
            return;
        }

        if (validator.getRepos().isClustered(this)) {
            throw validator.res.newValidatorDropClusteredIndex(
                validator.getRepos().getLocalizedObjectName(this, null),
                validator.getParserPosString(this));
        }

        if (getTable().isTemporary()) {
            // REVIEW: support this?  What to do about instances of the
            // same temporary table in other sessions?
            throw validator.res.newValidatorIndexOnExistingTempTable(
                validator.getRepos().getLocalizedObjectName(this, null),
                validator.getRepos().getLocalizedObjectName(
                    getTable(),
                    null));
        }
    }
}


// End CwmSqlindexImpl.java
