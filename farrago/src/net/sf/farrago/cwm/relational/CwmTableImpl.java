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
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.resource.*;

import org.netbeans.mdr.handlers.*;
import org.netbeans.mdr.storagemodel.*;

import java.util.*;


/**
 * CwmTableImpl is a custom implementation for CWM Table.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class CwmTableImpl extends InstanceHandler implements CwmTable,
    DdlValidatedElement
{
    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new CwmTableImpl object.
     *
     * @param storable .
     */
    protected CwmTableImpl(StorableObject storable)
    {
        super(storable);
    }

    //~ Methods ---------------------------------------------------------------

    // implement DdlValidatedElement
    public void validateDefinition(DdlValidator validator,boolean creation)
    {
        validator.validateUniqueNames(this,getFeature(),false);

        Collection indexes = validator.getCatalog().getIndexes(this);

        // NOTE:  don't need to validate index name uniqueness since indexes
        // live in same schema as table, so enforcement will take place at
        // schema level
        Iterator indexIter = indexes.iterator();
        int nClustered = 0;
        while (indexIter.hasNext()) {
            CwmSqlindex index = (CwmSqlindex) indexIter.next();
            if (validator.getCatalog().isClustered(index)) {
                nClustered++;
            }
        }
        if (nClustered > 1) {
            throw validator.res.newValidatorDuplicateClusteredIndex(
                getName());
        }

        CwmPrimaryKey primaryKey = null;
        Iterator constraintIter = getOwnedElement().iterator();
        while (constraintIter.hasNext()) {
            CwmUniqueConstraint constraint =
                (CwmUniqueConstraint) constraintIter.next();
            if (constraint instanceof CwmPrimaryKey) {
                if (primaryKey != null) {
                    throw validator.res.newValidatorMultiplePrimaryKeys(
                        getName());
                }
                primaryKey = (CwmPrimaryKey) constraint;
            }
            if (creation) {
                // Implement constraints via system-owned indexes.
                CwmSqlindex index =
                    createUniqueConstraintIndex(validator,constraint);
                if ((constraint == primaryKey) && (nClustered == 0)) {
                    // If no clustered index was specified, make the primary
                    // key's index clustered.
                    validator.getCatalog().setTagValue(
                        index,"clusteredIndex",null);
                }
            }
        }

        if (primaryKey == null) {
            // TODO:  This is not SQL-standard.  Fixing it requires the
            // introduction of a system-managed surrogate key.
            throw validator.res.newValidatorNoPrimaryKey(
                getName());
        }

        // NOTE:  do this after PRIMARY KEY uniqueness validation to get a
        // better error message in the case of generated constraint names
        validator.validateUniqueNames(this,getOwnedElement(),false);
    }

    // implement DdlValidatedElement
    public void validateDeletion(DdlValidator validator,boolean truncation)
    {
        if (truncation) {
            Collection indexes = validator.getCatalog().getIndexes(this);
            Iterator indexIter = indexes.iterator();
            while (indexIter.hasNext()) {
                CwmSqlindex index = (CwmSqlindex) indexIter.next();
                validator.scheduleTruncation(index);
            }
        }
    }

    private CwmSqlindex createUniqueConstraintIndex(
        DdlValidator validator,
        CwmUniqueConstraint constraint)
    {
        // TODO:  make index SYSTEM-owned so that it can't be
        // dropped explicitly
        FarragoCatalog catalog = validator.getCatalog();
        CwmSqlindex index = catalog.newCwmSqlindex();
        catalog.generateConstraintIndexName(constraint,index);
        catalog.indexPackage.getIndexSpansClass().add(this,index);

        // REVIEW:  same as DDL; why is this necessary?
        index.setSpannedClass(this);
        index.setUnique(true);

        Iterator columnIter = constraint.getFeature().iterator();
        while (columnIter.hasNext()) {
            CwmColumn column = (CwmColumn) columnIter.next();
            CwmSqlindexColumn indexColumn =
                catalog.newCwmSqlindexColumn();
            indexColumn.setAscending(Boolean.TRUE);
            indexColumn.setFeature(column);
            indexColumn.setIndex(index);
        }
        return index;
    }
}


// End CwmTableImpl.java
