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

import net.sf.farrago.ddl.*;
import net.sf.farrago.util.*;
import net.sf.farrago.type.*;
import net.sf.farrago.query.*;
import net.sf.farrago.session.*;
import net.sf.farrago.cwm.relational.enumerations.*;
import net.sf.farrago.cwm.core.*;

import net.sf.saffron.core.*;

import org.netbeans.mdr.handlers.*;
import org.netbeans.mdr.storagemodel.*;

import java.sql.*;
import java.util.logging.*;
import java.util.*;

import javax.jmi.reflect.*;

/**
 * CwmViewImpl is a custom implementation for CWM View.
 *
 * @author Kinkoi Lo
 * @version $Id$
 */
public abstract class CwmViewImpl
    extends InstanceHandler implements CwmView, DdlValidatedElement
{
    private static Logger tracer =
            TraceUtil.getClassTrace(CwmViewImpl.class);

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new CwmViewImpl object.
     *
     * @param storable .
     */
    protected CwmViewImpl(StorableObject storable)
    {
        super(storable);
    }

    //~ Methods ---------------------------------------------------------------

    // implement DdlValidatedElement
    public void validateDefinition(DdlValidator validator, boolean creation)
    {
        if (!creation) {
            return;
        }
        
        FarragoSession session = validator.newReentrantSession();

        try {
            validateImpl(validator, session);
        } catch (FarragoUnvalidatedDependencyException ex) {
            // pass this one through
            throw ex;
        } catch (Throwable ex) {
            // TODO:  if e has parser position information in it, need to either
            // delete it or adjust it
            throw validator.res.newValidatorInvalidViewDefinition(
                getName(),
                ex.getMessage(),
                ex);
        } finally {
            validator.releaseReentrantSession(session);
        }
    }

    private void validateImpl(
        DdlValidator validator,
        FarragoSession session) throws SQLException
    {
        String sql = getQueryExpression().getBody();

        tracer.fine(sql);
        FarragoSessionViewInfo viewInfo = session.analyzeViewQuery(sql);
        ResultSetMetaData metaData = viewInfo.resultMetaData;

        List columnList = getFeature();
        boolean implicitColumnNames = true;
        
        if (columnList.size() != 0) {
            implicitColumnNames = false;
            // number of explicitly specified columns needs to match the number
            // of columns produced by the query
            if (metaData.getColumnCount() != columnList.size()) {
                throw validator.res.newValidatorViewColumnCountMismatch();
            }
        }

        if (viewInfo.parameterMetaData.getParameterCount() != 0) {
            throw validator.res.newValidatorInvalidViewDynamicParam();
        }

        // Derive column information from result set metadata
        FarragoTypeFactory typeFactory = validator.getTypeFactory();
        SaffronType rowType = typeFactory.createResultSetType(metaData);
        int n = rowType.getFieldCount();
        SaffronField [] fields = rowType.getFields();
        for (int i = 0; i < n; ++i) {
            CwmColumn column;
            if (implicitColumnNames) {
                column = validator.getCatalog().newCwmColumn();
                columnList.add(column);
            } else {
                column = (CwmColumn) columnList.get(i);
            }
            typeFactory.convertFieldToCwmColumn(fields[i],column);
            CwmColumnImpl.validateType(validator,column);
        }

        validator.validateUniqueNames(this, getFeature(), false);

        getQueryExpression().setBody(viewInfo.validatedSql);

        validator.createDependency(
            this,
            viewInfo.dependencies,
            "ViewUsage");
    }

    // implement DdlValidatedElement
    public void validateDeletion(DdlValidator validator, boolean truncation)
    {
    }
}
