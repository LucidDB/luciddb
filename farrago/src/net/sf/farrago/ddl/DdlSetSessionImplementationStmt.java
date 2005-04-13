/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
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
import net.sf.farrago.session.*;
import net.sf.farrago.plugin.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.catalog.*;

import org.eigenbase.sql.*;

/**
 * DdlSetSessionImplementationStmt represents an ALTER SESSION SET
 * IMPLEMENTATION LIBRARY statement.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class DdlSetSessionImplementationStmt extends DdlStmt
{
    private final SqlIdentifier jarName;
    private FemJar femJar;

    public DdlSetSessionImplementationStmt(SqlIdentifier jarName)
    {
        super(null);
        this.jarName = jarName;
    }
    
    // implement DdlStmt
    public void visit(DdlVisitor visitor)
    {
        visitor.visit(this);
    }

    // implement FarragoSessionDdlStmt
    public void preValidate(FarragoSessionDdlValidator ddlValidator)
    {
        if (jarName == null) {
            return;
        }
        femJar = (FemJar) ddlValidator.getStmtValidator().findSchemaObject(
            jarName,
            ddlValidator.getRepos().getSql2003Package().getFemJar());
    }

    public FarragoSessionPersonality newPersonality(
        FarragoSession session,
        FarragoSessionPersonality defaultPersonality)
    {
        if (femJar == null) {
            return defaultPersonality;
        }
        String url = FarragoCatalogUtil.getJarUrl(femJar);
        Class factoryClass =
            session.getPluginClassLoader().loadClassFromJarUrlManifest(
                url,
                FarragoPluginClassLoader.PLUGIN_FACTORY_CLASS_ATTRIBUTE);
        try {
            FarragoSessionPersonalityFactory factory =
                (FarragoSessionPersonalityFactory) factoryClass.newInstance();
            return factory.newSessionPersonality(
                session,
                defaultPersonality);
        } catch (Throwable ex) {
            throw FarragoResource.instance().newPluginInitFailed(url, ex);
        }
    }
}

// End DdlSetSessionImplementationStmt.java
