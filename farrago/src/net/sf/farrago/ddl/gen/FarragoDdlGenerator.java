/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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
package net.sf.farrago.ddl.gen;

import java.util.*;

import net.sf.farrago.cwm.behavioral.*;
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.cwm.relational.enumerations.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fem.sql2003.*;

import org.eigenbase.sql.type.*;


/**
 * Generates DDL statements from catalog objects.
 *
 * @author Jason Ouellette
 * @version $Id$
 */
public class FarragoDdlGenerator
    extends DdlGenerator
{

    //~ Constructors -----------------------------------------------------------

    public FarragoDdlGenerator()
    {
    }

    //~ Methods ----------------------------------------------------------------

    protected void createHeader(
        StringBuilder sb,
        String typeName,
        GeneratedDdlStmt stmt)
    {
        createHeader(
            sb,
            typeName,
            stmt.isReplace(),
            stmt.getNewName());
    }

    protected void createHeader(
        StringBuilder sb,
        String typeName,
        boolean replace,
        String newName)
    {
        sb.append("CREATE ");
        if (replace) {
            sb.append("OR REPLACE ");
        }
        if (newName != null) {
            sb.append("RENAME TO ");
            sb.append(quote(newName));
            sb.append(" ");
        }
        sb.append(typeName);
        sb.append(" ");
    }

    public void create(
        FemLocalView view,
        GeneratedDdlStmt stmt)
    {
        StringBuilder sb = new StringBuilder();
        createHeader(sb, "VIEW", stmt);

        sb.append(quote(view.getName()));
        addDescription(sb, (FemAnnotatedElement) view);
        sb.append(" AS ");
        stmt.addStmt(sb.toString());

        sb = new StringBuilder();
        sb.append(view.getOriginalDefinition());
        stmt.addStmt(sb.toString());
    }

    public void create(
        CwmSchema schema,
        GeneratedDdlStmt stmt)
    {
        StringBuilder sb = new StringBuilder();
        createHeader(sb, "SCHEMA", stmt);

        sb.append(quote(schema.getName()));
        stmt.addStmt(sb.toString());
    }

    public void create(
        FemLocalTable table,
        GeneratedDdlStmt stmt)
    {
        StringBuilder sb = new StringBuilder();
        createHeader(sb, "TABLE", false, null);

        sb.append(quote(table.getName()));
        stmt.addStmt(sb.toString());
        sb = new StringBuilder();
        addColumns(
            sb,
            table.getFeature().iterator());
        addOptions(
            sb,
            ((FemElementWithStorageOptions) table).getStorageOptions());
        addDescription(sb, (FemAnnotatedElement) table);
        stmt.addStmt(sb.toString());
    }

    public void create(
        FemForeignTable table,
        GeneratedDdlStmt stmt)
    {
        StringBuilder sb = new StringBuilder();
        createHeader(sb, "FOREIGN TABLE", false, null);

        sb.append(quote(table.getName()));
        stmt.addStmt(sb.toString());
        sb = new StringBuilder();
        addColumns(
            sb,
            table.getFeature().iterator());
        sb.append(NL);
        sb.append(" SERVER ");
        FemDataServer server = (FemDataServer) table.getServer();
        if (server != null) {
            sb.append(quote(server.getName()));
        }
        addOptions(
            sb,
            ((FemElementWithStorageOptions) table).getStorageOptions());
        addDescription(sb, (FemAnnotatedElement) table);
        stmt.addStmt(sb.toString());
    }

    public void create(
        FemDataWrapper wrapper,
        GeneratedDdlStmt stmt)
    {
        StringBuilder sb = new StringBuilder();
        createHeader(sb, "FOREIGN DATA WRAPPER", stmt);

        sb.append(quote(wrapper.getName()));
        stmt.addStmt(sb.toString());
        sb = new StringBuilder();
        sb.append(" LIBRARY ");
        sb.append(literal(wrapper.getLibraryFile()));
        sb.append(NL);
        sb.append(" LANGUAGE ");
        sb.append(wrapper.getLanguage());
        addOptions(
            sb,
            wrapper.getStorageOptions());
        addDescription(sb, (FemAnnotatedElement) wrapper);
        stmt.addStmt(sb.toString());
    }

    public void create(
        FemRoutine routine,
        GeneratedDdlStmt stmt)
    {
        StringBuilder sb = new StringBuilder();
        if (routine.getType().equals(ProcedureTypeEnum.FUNCTION)) {
            createHeader(sb, "FUNCTION", false, null);
        } else if (routine.getType().equals(ProcedureTypeEnum.PROCEDURE)) {
            createHeader(sb, "PROCEDURE", false, null);
        }

        sb.append(quote(routine.getName()));
        stmt.addStmt(sb.toString());
        sb = new StringBuilder();

        sb.append(" (");
        sb.append(NL);
        List parameters = routine.getParameter();
        Iterator pi = parameters.iterator();
        if (routine.getType().equals(ProcedureTypeEnum.PROCEDURE)) {
            while (pi.hasNext()) {
                CwmParameter p = (CwmParameter) pi.next();
                if (p.getKind().equals(ParameterDirectionKindEnum.PDK_IN)) {
                    sb.append("  IN ");
                } else if (p.getKind().equals(
                        ParameterDirectionKindEnum.PDK_INOUT)) {
                    sb.append("  INOUT ");
                } else if (p.getKind().equals(
                        ParameterDirectionKindEnum.PDK_OUT)) {
                    sb.append("  OUT ");
                }

                //REVIEW: procedures don't have RETURNS, right?
                sb.append(quote(p.getName()));
                sb.append(" ");
                CwmSqldataType type = (CwmSqldataType) p.getType();
                SqlTypeName stn = getSqlTypeName(type);
                sb.append(type.getName());

                //REVIEW: how to get type length, precision?
                if (pi.hasNext()) {
                    sb.append(", ");
                    sb.append(NL);
                }
            }
            sb.append(NL);
            sb.append(" )");
            sb.append(NL);
        } else {
            List<CwmParameter> returns = new ArrayList<CwmParameter>();
            boolean first = true;
            while (pi.hasNext()) {
                CwmParameter p = (CwmParameter) pi.next();

                //REVIEW: functions can't have INOUT or OUT parameters, right?
                if (p.getKind().equals(ParameterDirectionKindEnum.PDK_IN)) {
                    if (!first) {
                        sb.append(", ");
                        sb.append(NL);
                    } else {
                        first = false;
                    }
                    sb.append("   " + quote(p.getName()));
                    sb.append(" ");
                    CwmSqldataType type = (CwmSqldataType) p.getType();
                    SqlTypeName stn = getSqlTypeName(type);
                    sb.append(type.getName());
                } else if (p.getKind().equals(
                        ParameterDirectionKindEnum.PDK_RETURN)) {
                    returns.add(p);
                }
            }
            sb.append(NL);
            sb.append(" )");
            sb.append(NL);
            if (returns.size() > 0) {
                sb.append(" RETURNS");
                Iterator ri = returns.iterator();
                while (ri.hasNext()) {
                    CwmParameter p = (CwmParameter) ri.next();
                    CwmSqldataType type = (CwmSqldataType) p.getType();
                    SqlTypeName stn = getSqlTypeName(type);

                    sb.append(" " + type.getName());

                    //REVIEW: how to get type length, precision?
                    if (ri.hasNext()) {
                        sb.append(", ");
                        sb.append(NL);
                    }
                }
                sb.append(NL);
            }
        }

        sb.append(" LANGUAGE ");
        sb.append(routine.getLanguage());
        sb.append(NL);

        if (routine.getDataAccess().equals(
                RoutineDataAccessEnum.RDA_MODIFIES_SQL_DATA)) {
            sb.append(" MODIFIES SQL DATA");
        } else if (routine.getDataAccess().equals(
                RoutineDataAccessEnum.RDA_CONTAINS_SQL)) {
            sb.append(" CONTAINS SQL");
        } else if (routine.getDataAccess().equals(
                RoutineDataAccessEnum.RDA_NO_SQL)) {
            sb.append(" NO SQL");
        } else if (routine.getDataAccess().equals(
                RoutineDataAccessEnum.RDA_READS_SQL_DATA)) {
            sb.append(" READS SQL DATA");
        }
        sb.append(NL);

        //REVIEW: where do we find "deterministic"?
        sb.append(" EXTERNAL NAME ");
        sb.append(literal(routine.getExternalName()));
        sb.append(NL);

        stmt.addStmt(sb.toString());
    }

    public void create(
        FemDataServer server,
        GeneratedDdlStmt stmt)
    {
        StringBuilder sb = new StringBuilder();
        createHeader(sb, "SERVER", stmt);

        sb.append(quote(server.getName()));
        stmt.addStmt(sb.toString());
        sb = new StringBuilder();
        sb.append(" TYPE ");
        sb.append(literal(server.getType()));
        sb.append(" VERSION ");
        sb.append(literal(server.getVersion()));
        sb.append(NL);
        sb.append(" FOREIGN DATA WRAPPER ");
        sb.append(quote(server.getWrapper().getName()));
        addOptions(
            sb,
            server.getStorageOptions());
        addDescription(sb, (FemAnnotatedElement) server);
        stmt.addStmt(sb.toString());
    }

    public void drop(
        CwmSchema schema,
        GeneratedDdlStmt stmt)
    {
        StringBuffer sb = new StringBuffer();
        sb.append("DROP SCHEMA ");
        sb.append(quote(schema.getName()));
        sb.append(" CASCADE");
        stmt.addStmt(sb.toString());
    }

    public void drop(
        CwmView view,
        GeneratedDdlStmt stmt)
    {
        drop(view, "VIEW", stmt);
    }

    public void drop(
        FemDataServer server,
        GeneratedDdlStmt stmt)
    {
        drop(server, "SERVER", stmt);
    }

    public void drop(
        CwmTable table,
        GeneratedDdlStmt stmt)
    {
        drop(table, "TABLE", stmt);
    }

    public void drop(
        FemDataWrapper plugin,
        GeneratedDdlStmt stmt)
    {
        drop(plugin, "FOREIGN DATA WRAPPER", stmt);
    }

    protected void addColumns(
        StringBuilder sb,
        Iterator columns)
    {
        addColumns(sb, columns, false, false);
    }

    protected void addColumns(
        StringBuilder sb,
        Iterator columns,
        boolean skipDefaults,
        boolean skipNullable)
    {
        boolean isLast = false;
        List<String> pk = null;

        if (columns.hasNext()) {
            sb.append(" (");
            sb.append(NL);
            while (columns.hasNext()) {
                CwmColumn col = (CwmColumn) columns.next();

                if (col instanceof FemStoredColumn) {
                    if (hasPrimaryKeyConstraint((FemStoredColumn) col)) {
                        if (pk == null) {
                            pk = new ArrayList<String>();
                        }
                        pk.add(col.getName());
                    }
                }

                isLast = !columns.hasNext() && (pk == null);

                sb.append("   " + quote(col.getName()));

                //TODO: handle other 2 type classes
                CwmSqldataType type = (CwmSqldataType) col.getType();
                SqlTypeName stn = getSqlTypeName(type);

                sb.append(" " + type.getName());

                //there must be an easier way
                Integer len = col.getLength();
                Integer scale = col.getScale();

                if ((len != null) && stn.allowsPrec()) {
                    sb.append("(" + len);
                    if ((scale != null) && stn.allowsScale()) {
                        sb.append("," + scale);
                    }
                    sb.append(")");
                }

                if (!skipDefaults) {
                    CwmExpression e = col.getInitialValue();
                    if (e != null) {
                        String val = e.getBody();
                        if ((val != null) && !val.equals(VALUE_NULL)) {
                            sb.append(" DEFAULT ");

                            // we expect the setter of body to be responsible
                            // for forming a valid SQL expression given the
                            // datatype
                            sb.append(val);
                        }
                    }
                }

                if (!skipNullable) {
                    if (NullableTypeEnum.COLUMN_NO_NULLS.toString().equals(
                            col.getIsNullable().toString())) {
                        sb.append(" NOT NULL");
                    }
                }

                // is this a stored column?
                if (col instanceof FemElementWithStorageOptions) {
                    addOptions(
                        sb,
                        ((FemElementWithStorageOptions) col)
                        .getStorageOptions(),
                        2);
                }

                if (!isLast) {
                    sb.append(",");
                }

                sb.append(NL);
            }

            addPrimaryKeyConstraint(sb, pk);
            sb.append(" )");
        }
    }

    protected void addOptions(
        StringBuilder sb,
        Collection options)
    {
        addOptions(sb, options, 1);
    }

    protected void addOptions(
        StringBuilder sb,
        Collection options,
        int indent)
    {
        if ((options == null) || (options.size() == 0)) {
            return;
        }

        if (indent == 1) {
            sb.append(NL);
        }
        sb.append(" OPTIONS (");
        sb.append(NL);
        Iterator i = options.iterator();
        while (i.hasNext()) {
            FemStorageOption option = (FemStorageOption) i.next();
            for (int j = 0; j < indent; j++) {
                sb.append("  ");
            }
            sb.append(option.getName());
            sb.append(" ");
            sb.append(literal(option.getValue()));
            if (i.hasNext()) {
                sb.append(",");
            }
            sb.append(NL);
        }
        sb.append(" ");
        for (int j = 0; j < indent; j++) {
            sb.append(" ");
        }
        sb.append(")");
    }

    private void addPrimaryKeyConstraint(
        StringBuilder sb,
        List<String> keyColumns)
    {
        if (keyColumns != null) {
            sb.append("   PRIMARY KEY (");
            Iterator i = keyColumns.iterator();
            boolean isFirst = true;
            while (i.hasNext()) {
                if (!isFirst) {
                    sb.append(",");
                } else {
                    isFirst = false;
                }
                sb.append(quote((String) i.next()));
            }
            sb.append(")");
            sb.append(NL);
        }
    }

    protected void addDescription(
        StringBuilder sb,
        FemAnnotatedElement element)
    {
        String desc = element.getDescription();
        if ((desc != null) && (desc.length() > 0)) {
            sb.append(NL);
            sb.append(" DESCRIPTION ");
            sb.append(literal(escapeApostrophesAndQuotes(desc)));
            sb.append(NL);
        }
    }

    protected void drop(
        CwmModelElement e,
        String elementType,
        GeneratedDdlStmt stmt)
    {
        if (e != null) {
            StringBuffer sb = new StringBuffer();
            sb.append("DROP " + elementType + " ");

            // we're already in the current schema (SET SCHEMA) so don't fully
            // qualify name
            sb.append(quote(e.getName()));
            stmt.addStmt(sb.toString());
        }
    }
}

// End FarragoDdlGenerator.java
