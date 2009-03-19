/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2004-2009 John V. Sichi
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
package net.sf.farrago.ddl;

import java.io.*;

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.cwm.relational.enumerations.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.util.*;
import net.sf.farrago.plugin.*;
import net.sf.farrago.query.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.session.*;
import net.sf.farrago.type.*;
import net.sf.farrago.util.*;

import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;


/**
 * DdlMedHandler defines DDL handler methods for SQL/MED objects.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class DdlMedHandler
    extends DdlHandler
{
    //~ Constructors -----------------------------------------------------------

    public DdlMedHandler(FarragoSessionDdlValidator validator)
    {
        super(validator);
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoSessionDdlHandler
    public void validateDefinition(FemForeignTable foreignTable)
    {
        validateForeignColumnSetDefinition(foreignTable);
    }

    public void validateForeignColumnSetDefinition(
        FemBaseColumnSet columnSet)
    {
        FemDataServer dataServer = columnSet.getServer();
        FemDataWrapper dataWrapper = dataServer.getWrapper();

        if (!dataWrapper.isForeign()) {
            throw res.ValidatorForeignTableButLocalWrapper.ex(
                repos.getLocalizedObjectName(columnSet),
                repos.getLocalizedObjectName(dataWrapper));
        }

        validateBaseColumnSet(columnSet);

        FarragoMedColumnSet medColumnSet = validateMedColumnSet(columnSet);

        List<CwmFeature> columnList = columnSet.getFeature();
        if (columnList.isEmpty()) {
            // derive column information
            RelDataType rowType = medColumnSet.getRowType();
            RelDataTypeField [] fields = rowType.getFields();
            for (int i = 0; i < fields.length; ++i) {
                FemStoredColumn column = repos.newFemStoredColumn();
                columnList.add(column);
                convertFieldToCwmColumn(fields[i], column, columnSet);
                validateAttribute(column);
            }
        }

        SqlAccessType allowedAccess = medColumnSet.getAllowedAccess();
        if (columnSet.getAllowedAccess() == null) {
            columnSet.setAllowedAccess(allowedAccess.toString());
        } else {
            Util.permAssert(
                columnSet.getAllowedAccess().equals(
                    medColumnSet.getAllowedAccess().toString()),
                "Catalog allowed access doesn't match MED allowed access");
        }
    }

    // implement FarragoSessionDdlHandler
    public void validateDefinition(FemDataServer femServer)
    {
        // since servers are in the same namespace with CWM catalogs,
        // need a special name uniquness check here
        validator.validateUniqueNames(
            repos.getCatalog(FarragoCatalogInit.SYSBOOT_CATALOG_NAME),
            repos.allOfType(CwmCatalog.class),
            false);

        // See http://issues.eigenbase.org/browse/FRG-276 for
        // an enhancement related to providing more control here.
        // For now, we avoid failing CREATE OR REPLACE FOREIGN WRAPPER
        // just because a dependent foreign server can't be
        // accessed.

        // FIXME jvs 21-Jun-2007:  promote methods up to
        // FarragoSessionDdlValidator level instead of downcasting.
        DdlValidator ddlValidator = (DdlValidator) validator;

        if (!ddlValidator.isReplace()
            || ddlValidator.isReplacingType(femServer))
        {
            try {
                // validate that we can successfully initialize the server
                validator.getDataWrapperCache().loadServerFromCatalog(
                    femServer);
            } catch (Throwable ex) {
                throw res.ValidatorDefinitionInvalid.ex(
                    repos.getLocalizedObjectName(femServer),
                    ex);
            }
        }

        // REVIEW jvs 18-April-2004:  This uses default charset/collation
        // info from local catalog, but should really allow foreign
        // servers to override.
        FarragoCatalogUtil.initializeCatalog(repos, femServer);

        // REVIEW jvs 18-April-2004:  Query the plugin for these?
        if (femServer.getType() == null) {
            femServer.setType("UNKNOWN");
        }
        if (femServer.getVersion() == null) {
            femServer.setVersion("UNKNOWN");
        }

        validator.createDependency(
            femServer,
            Collections.singleton(femServer.getWrapper()));
    }

    // implement FarragoSessionDdlHandler
    public void validateDefinition(FemDataWrapper femWrapper)
    {
        FarragoMedDataWrapper wrapper;
        try {
            if (!FarragoPluginClassLoader.isLibraryClass(
                    femWrapper.getLibraryFile()))
            {
                // convert library filename to absolute path, if necessary
                String libraryFile = femWrapper.getLibraryFile();

                String expandedLibraryFile =
                    FarragoProperties.instance().expandProperties(libraryFile);

                // REVIEW: SZ: 7/20/2004: Maybe the library should
                // always be an absolute path?  (e.g. Always report an
                // error if the path given by the user is relative.)
                // If a user installs a thirdparty Data Wrapper we
                // probably don't want them using relative paths to
                // call out its location.
                if (libraryFile.equals(expandedLibraryFile)) {
                    // No properties were expanded, so make the path
                    // absolute if it isn't already absolute.
                    File file = new File(libraryFile);
                    femWrapper.setLibraryFile(file.getAbsolutePath());
                } else {
                    // Test that the expanded library file is an
                    // absolute path.  We don't set the absolute path
                    // because we want to keep the property in the
                    // library name.
                    File file = new File(expandedLibraryFile);
                    if (!file.isAbsolute()) {
                        throw new IOException(
                            libraryFile
                            + " does not evaluate to an absolute path");
                    }
                }
            }

            // validate that we can successfully initialize the wrapper
            wrapper =
                validator.getDataWrapperCache().loadWrapperFromCatalog(
                    femWrapper);
        } catch (Throwable ex) {
            throw res.ValidatorDefinitionInvalid.ex(
                repos.getLocalizedObjectName(femWrapper),
                ex);
        }

        if (femWrapper.isForeign()) {
            if (!wrapper.isForeign()) {
                throw res.ValidatorForeignWrapperHasLocalImpl.ex(
                    repos.getLocalizedObjectName(femWrapper));
            }
        } else {
            if (wrapper.isForeign()) {
                throw res.ValidatorLocalWrapperHasForeignImpl.ex(
                    repos.getLocalizedObjectName(femWrapper));
            }
        }
    }

    // implement FarragoSessionDdlHandler
    public void executeDrop(FemDataServer server)
    {
        validator.discardDataWrapper(server);
    }

    // implement FarragoSessionDdlHandler
    public void executeDrop(FemDataWrapper wrapper)
    {
        validator.discardDataWrapper(wrapper);
    }

    public FarragoMedColumnSet validateMedColumnSet(
        FemBaseColumnSet femColumnSet)
    {
        FarragoMedColumnSet medColumnSet;

        try {
            // validate that we can successfully initialize the table
            medColumnSet =
                validator.getDataWrapperCache().loadColumnSetFromCatalog(
                    femColumnSet,
                    validator.getTypeFactory());
        } catch (Throwable ex) {
            throw res.ValidatorDataServerTableInvalid.ex(
                repos.getLocalizedObjectName(femColumnSet),
                ex);
        }

        validator.createDependency(
            femColumnSet,
            Collections.singleton(femColumnSet.getServer()));

        return medColumnSet;
    }
}

// End DdlMedHandler.java
