/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
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
package net.sf.farrago.ddl;

import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.fem.med.*;

import net.sf.farrago.resource.*;
import net.sf.farrago.session.*;
import net.sf.farrago.type.*;
import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.util.*;

import org.eigenbase.sql.*;
import org.eigenbase.reltype.*;

import java.util.*;
import java.sql.*;

/**
 * DdlImportForeignSchemaStmt represents a DDL IMPORT FOREIGN SCHEMA
 * statement.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class DdlImportForeignSchemaStmt extends DdlStmt
{
    private final FemLocalSchema localSchema;
    private final FemDataServer femServer;
    private final SqlIdentifier foreignSchemaName;
    private final boolean exclude;
    private final List roster;
    private final String pattern;

    /**
     * Creates a new import statement.  It is illegal for both roster and
     * pattern to be specified (both may be null).
     *
     * @param localSchema target local schema
     *
     * @param femServer source foreign server
     *
     * @param foreignSchemaName name of source foreign schema
     *
     * @param exclude whether to exclude roster/pattern
     *
     * @param roster if non-null, a list of table names (as SqlIdentifiers)
     * to include or exclude
     *
     * @param pattern if non-null, a table name pattern to include or exclude
     */
    public DdlImportForeignSchemaStmt(
        FemLocalSchema localSchema,
        FemDataServer femServer,
        SqlIdentifier foreignSchemaName,
        boolean exclude,
        List roster,
        String pattern)
    {
        super(localSchema);

        this.localSchema = localSchema;
        this.femServer = femServer;
        this.foreignSchemaName = foreignSchemaName;
        this.exclude = exclude;
        this.roster = roster;
        this.pattern = pattern;
    }

    // implement DdlStmt
    public void visit(DdlVisitor visitor)
    {
        visitor.visit(this);
    }
    
    // implement FarragoSessionDdlStmt
    public void preValidate(FarragoSessionDdlValidator ddlValidator)
    {
        if (!femServer.getWrapper().isForeign()) {
            throw FarragoResource.instance().newValidatorImportMustBeForeign(
                ddlValidator.getRepos().getLocalizedObjectName(femServer));
        }
        FarragoMedDataServer medServer = 
            ddlValidator.getDataWrapperCache().loadServerFromCatalog(femServer);
        try {
            FarragoMedNameDirectory catalogDir = medServer.getNameDirectory();
            if (catalogDir == null) {
                throw FarragoResource.instance().newValidatorImportUnsupported(
                    ddlValidator.getRepos().getLocalizedObjectName(femServer));
            }
            assert(foreignSchemaName.isSimple());
            FarragoMedNameDirectory schemaDir =
                catalogDir.lookupSubdirectory(foreignSchemaName.getSimple());
            if (schemaDir == null) {
                throw FarragoResource.instance().newValidatorImportUnknown(
                    ddlValidator.getRepos().getLocalizedObjectName(
                        foreignSchemaName.getSimple()));
            }

            Set requiredSet = null;

            // Create a metadata query to retrieve column descriptors, using
            // any table name filters requested.
            FarragoMedMetadataQuery query = new MedMetadataQueryImpl();
            query.getResultObjectTypes().add(
                FarragoMedMetadataQuery.OTN_TABLE);
            query.getResultObjectTypes().add(
                FarragoMedMetadataQuery.OTN_COLUMN);
            if ((roster != null) || (pattern != null)) {
                requiredSet = convertRoster();
                FarragoMedMetadataFilter filter =
                    new MedMetadataFilterImpl(
                        exclude,
                        requiredSet,
                        pattern);
                query.getFilterMap().put(
                    FarragoMedMetadataQuery.OTN_TABLE,
                    filter);
                if (exclude) {
                    requiredSet = null;
                }
            }

            // Create a sink to receive query results
            ImportSink sink = new ImportSink(
                ddlValidator, query, schemaDir);
            
            if (!schemaDir.queryMetadata(query, sink)) {
                throw FarragoResource.instance().newValidatorImportUnsupported(
                    ddlValidator.getRepos().getLocalizedObjectName(femServer));
            }

            if (requiredSet != null) {
                requiredSet.removeAll(sink.getImportedTableNames());
                if (!requiredSet.isEmpty()) {
                    throw FarragoResource.instance().newValidatorImportMissing(
                        ddlValidator.getRepos().getLocalizedObjectName(
                            foreignSchemaName.getSimple()),
                        requiredSet.toString());
                }
            }

            // TODO:  error handling for duplicate table/column names
            sink.dropStragglers();

        } catch (SQLException ex) {
            throw FarragoResource.instance().newValidatorImportFailed(
                ddlValidator.getRepos().getLocalizedObjectName(
                    foreignSchemaName.getSimple()),
                ddlValidator.getRepos().getLocalizedObjectName(femServer),
                ex);
        }
    }

    private Set convertRoster()
    {
        if (roster == null) {
            return null;
        }
        Set set = new HashSet();
        Iterator iter = roster.iterator();
        while (iter.hasNext()) {
            SqlIdentifier id = (SqlIdentifier) iter.next();
            set.add(id.getSimple());
        }
        return set;
    }

    // TODO:  passive abort checking
    
    /**
     * ImportSink implements {@link FarragoMedMetadataSink} by creating 
     * catalog descriptors for imported foreign tables and columns.
     */
    private class ImportSink extends MedAbstractMetadataSink
    {
        private final FarragoSessionDdlValidator ddlValidator;
        
        private final Map tableMap;

        private DdlMedHandler medHandler;

        private FarragoMedNameDirectory directory;
        
        ImportSink(
            FarragoSessionDdlValidator ddlValidator,
            FarragoMedMetadataQuery query,
            FarragoMedNameDirectory directory)
        {
            super(query, ddlValidator.getTypeFactory());

            this.ddlValidator = ddlValidator;
            this.directory = directory;
            
            tableMap = new HashMap();
            medHandler = new DdlMedHandler(ddlValidator);
        }

        // implement FarragoMedMetadataSink
        public boolean writeObjectDescriptor(
            String name,
            String typeName,
            String remarks,
            Map properties)
        {
            if (!shouldInclude(name, typeName, false)) {
                return false;
            }
            
            FemBaseColumnSet table = createTable(name);
            setStorageOptions(table, properties);
            return true;
        }
        
        // implement FarragoMedMetadataSink
        public boolean writeColumnDescriptor(
            String tableName,
            String columnName,
            int ordinal,
            RelDataType type,
            String remarks,
            String defaultValue,
            Map properties)
        {
            if (!shouldInclude(
                    tableName, FarragoMedMetadataQuery.OTN_TABLE, true))
            {
                return false;
            }
            if (!shouldInclude(
                    columnName, FarragoMedMetadataQuery.OTN_COLUMN, false))
            {
                return false;
            }

            FemBaseColumnSet table = (FemBaseColumnSet) tableMap.get(tableName);
            if (table == null) {
                return false;
            }

            FemStoredColumn column =
                ddlValidator.getRepos().newFemStoredColumn();
            column.setName(columnName);
            column.setOrdinal(ordinal);
            RelDataTypeField field = new RelDataTypeFieldImpl(
                columnName, ordinal, type);
            medHandler.convertFieldToCwmColumn(field, column);

            setStorageOptions(column, properties);
            
            // TODO:  real excn
            assert(ordinal == table.getFeature().size());
            table.getFeature().add(column);

            // TODO:  set column remarks, default value

            return true;
        }

        Set getImportedTableNames()
        {
            return tableMap.keySet();
        }

        void dropStragglers()
        {
            // Drop all tables with no columns.
            Iterator iter = tableMap.values().iterator();
            while (iter.hasNext()) {
                FemBaseColumnSet table = (FemBaseColumnSet) iter.next();
                if (table.getFeature().isEmpty()) {
                    table.refDelete();
                }
            }
        }

        private FemBaseColumnSet createTable(String tableName)
        {
            FemBaseColumnSet table = directory.newImportedColumnSet(
                ddlValidator.getRepos(), tableName);
            table.setName(tableName);
            table.setServer(femServer);
            localSchema.getOwnedElement().add(table);
            tableMap.put(tableName, table);
            return table;
        }

        private void setStorageOptions(
            FemElementWithStorageOptions element,
            Map props)
        {
            Iterator entryIter = props.entrySet().iterator();
            while (entryIter.hasNext()) {
                Map.Entry entry = (Map.Entry) entryIter.next();
                FemStorageOption opt =
                    ddlValidator.getRepos().newFemStorageOption();
                opt.setName(entry.getKey().toString());
                opt.setValue(entry.getValue().toString());
                element.getStorageOptions().add(opt);
            }
        }
    }
}

// End DdlImportForeignSchemaStmt.java
