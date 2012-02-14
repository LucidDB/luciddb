/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/
package net.sf.farrago.ddl;

import net.sf.farrago.catalog.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.plugin.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.session.*;

import org.eigenbase.sql.*;


/**
 * DdlSetSessionImplementationStmt represents an ALTER SESSION {SET|ADD}
 * IMPLEMENTATION LIBRARY statement.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class DdlSetSessionImplementationStmt
    extends DdlStmt
{
    //~ Instance fields --------------------------------------------------------

    private final SqlIdentifier jarName;
    private final boolean add;
    private FemJar femJar;

    //~ Constructors -----------------------------------------------------------

    public DdlSetSessionImplementationStmt(
        SqlIdentifier jarName,
        boolean add)
    {
        super(null);
        this.jarName = jarName;
        this.add = add;
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
        if (jarName == null) {
            return;
        }
        femJar =
            ddlValidator.getStmtValidator().findSchemaObject(
                jarName,
                FemJar.class);
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
                (FarragoSessionPersonalityFactory) session
                .getPluginClassLoader().newPluginInstance(
                    factoryClass);
            return factory.newSessionPersonality(
                session,
                add ? session.getPersonality() : defaultPersonality);
        } catch (Throwable ex) {
            throw FarragoResource.instance().PluginInitFailed.ex(url, ex);
        }
    }
}

// End DdlSetSessionImplementationStmt.java
