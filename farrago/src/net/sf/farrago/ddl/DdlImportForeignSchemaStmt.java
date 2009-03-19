/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 SQLstream, Inc.
// Copyright (C) 2005-2007 LucidEra, Inc.
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

import java.sql.*;

import java.util.*;

import net.sf.farrago.fem.med.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.util.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.session.*;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.util.*;


/**
 * DdlImportForeignSchemaStmt represents a DDL IMPORT FOREIGN SCHEMA statement.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class DdlImportForeignSchemaStmt
    extends DdlStmt
{
    //~ Instance fields --------------------------------------------------------

    private final FemLocalSchema localSchema;
    private final FemDataServer femServer;
    private final SqlIdentifier foreignSchemaName;
    private final boolean exclude;
    private final List<SqlIdentifier> roster;
    private final String pattern;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new import statement. It is illegal for both roster and pattern
     * to be specified (both may be null).
     *
     * @param localSchema target local schema
     * @param femServer source foreign server
     * @param foreignSchemaName name of source foreign schema
     * @param exclude whether to exclude roster/pattern
     * @param roster if non-null, a list of table names (as SqlIdentifiers) to
     * include or exclude
     * @param pattern if non-null, a table name pattern to include or exclude
     */
    public DdlImportForeignSchemaStmt(
        FemLocalSchema localSchema,
        FemDataServer femServer,
        SqlIdentifier foreignSchemaName,
        boolean exclude,
        List<SqlIdentifier> roster,
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

    //~ Methods ----------------------------------------------------------------

    // implement DdlStmt
    public void visit(DdlVisitor visitor)
    {
        visitor.visit(this);
    }

    // implement FarragoSessionDdlStmt
    public void preValidate(FarragoSessionDdlValidator ddlValidator)
    {
        if (!femServer.getWrapper().isForeign()) {
            throw FarragoResource.instance().ValidatorImportMustBeForeign.ex(
                ddlValidator.getRepos().getLocalizedObjectName(femServer));
        }
        FarragoMedDataServer medServer =
            ddlValidator.getDataWrapperCache().loadServerFromCatalog(femServer);
        try {
            FarragoMedNameDirectory catalogDir = medServer.getNameDirectory();
            if (catalogDir == null) {
                throw FarragoResource.instance().ValidatorImportUnsupported.ex(
                    ddlValidator.getRepos().getLocalizedObjectName(femServer));
            }
            assert (foreignSchemaName.isSimple());
            FarragoMedNameDirectory schemaDir =
                catalogDir.lookupSubdirectory(foreignSchemaName.getSimple());
            if (schemaDir == null) {
                throw FarragoResource.instance().ValidatorImportUnknown.ex(
                    ddlValidator.getRepos().getLocalizedObjectName(
                        foreignSchemaName.getSimple()));
            }

            Set<String> requiredSet = null;

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
            ImportSink sink =
                new ImportSink(
                    ddlValidator,
                    query,
                    schemaDir);

            if (!schemaDir.queryMetadata(query, sink)) {
                throw FarragoResource.instance().ValidatorImportUnsupported.ex(
                    ddlValidator.getRepos().getLocalizedObjectName(femServer));
            }

            if (requiredSet != null) {
                requiredSet.removeAll(sink.getImportedTableNames());
                if (!requiredSet.isEmpty()) {
                    throw FarragoResource.instance().ValidatorImportMissing.ex(
                        ddlValidator.getRepos().getLocalizedObjectName(
                            foreignSchemaName.getSimple()),
                        requiredSet.toString());
                }
            }

            // TODO:  error handling for duplicate table/column names
            sink.dropStragglers();
        } catch (SQLException ex) {
            throw FarragoResource.instance().ValidatorImportFailed.ex(
                ddlValidator.getRepos().getLocalizedObjectName(
                    foreignSchemaName.getSimple()),
                ddlValidator.getRepos().getLocalizedObjectName(femServer),
                ex);
        }
    }

    private Set<String> convertRoster()
    {
        if (roster == null) {
            return null;
        }

        // keep roster as a sorted set so can report any missing
        // entries in a deterministic order (esp. for unit tests)
        Set<String> set = new TreeSet<String>();
        for (SqlIdentifier id : roster) {
            set.add(id.getSimple());
        }
        return set;
    }

    // TODO:  passive abort checking

    //~ Inner Classes ----------------------------------------------------------

    /**
     * ImportSink implements {@link FarragoMedMetadataSink} by creating catalog
     * descriptors for imported foreign tables and columns.
     */
    private class ImportSink
        extends MedAbstractMetadataSink
    {
        private final FarragoSessionDdlValidator ddlValidator;

        private final Map<String, FemBaseColumnSet> tableMap;

        private DdlMedHandler medHandler;

        private FarragoMedNameDirectory directory;

        ImportSink(
            FarragoSessionDdlValidator ddlValidator,
            FarragoMedMetadataQuery query,
            FarragoMedNameDirectory directory)
        {
            super(
                query,
                ddlValidator.getTypeFactory());

            this.ddlValidator = ddlValidator;
            this.directory = directory;

            tableMap = new HashMap<String, FemBaseColumnSet>();
            medHandler = new DdlMedHandler(ddlValidator);
        }

        // implement FarragoMedMetadataSink
        public boolean writeObjectDescriptor(
            String name,
            String typeName,
            String remarks,
            Properties properties)
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
            Properties properties)
        {
            if (!shouldInclude(
                    tableName,
                    FarragoMedMetadataQuery.OTN_TABLE,
                    true))
            {
                return false;
            }
            if (!shouldInclude(
                    columnName,
                    FarragoMedMetadataQuery.OTN_COLUMN,
                    false))
            {
                return false;
            }

            FemBaseColumnSet table = tableMap.get(tableName);
            if (table == null) {
                return false;
            }

            FemStoredColumn column =
                ddlValidator.getRepos().newFemStoredColumn();
            column.setName(columnName);
            column.setOrdinal(ordinal);
            RelDataTypeField field =
                new RelDataTypeFieldImpl(
                    columnName,
                    ordinal,
                    type);
            medHandler.convertFieldToCwmColumn(field, column, table);

            setStorageOptions(column, properties);

            // TODO:  real excn
            assert (ordinal == table.getFeature().size());
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
            for (FemBaseColumnSet table : tableMap.values()) {
                if (table.getFeature().isEmpty()) {
                    table.refDelete();
                }
            }
        }

        private FemBaseColumnSet createTable(String tableName)
        {
            FemBaseColumnSet table =
                directory.newImportedColumnSet(
                    ddlValidator.getRepos(),
                    tableName);
            table.setName(tableName);
            table.setServer(femServer);
            localSchema.getOwnedElement().add(table);
            tableMap.put(tableName, table);
            return table;
        }

        private void setStorageOptions(
            FemElementWithStorageOptions element,
            Properties props)
        {
            for (Map.Entry<String, String> entry : Util.toMap(props).entrySet()) {
                FemStorageOption opt =
                    ddlValidator.getRepos().newFemStorageOption();
                opt.setName(entry.getKey());
                opt.setValue(entry.getValue());
                element.getStorageOptions().add(opt);
            }
        }
    }
}

// End DdlImportForeignSchemaStmt.java
