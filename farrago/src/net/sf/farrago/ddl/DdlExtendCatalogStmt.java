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

import java.io.*;

import java.net.*;

import java.util.jar.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.plugin.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.session.*;

import org.eigenbase.sql.*;
import org.eigenbase.util.*;


/**
 * DdlExtendCatalogStmt represents an ALTER SYSTEM ADD CATALOG JAR statement.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class DdlExtendCatalogStmt
    extends DdlStmt
{
    //~ Instance fields --------------------------------------------------------

    private final SqlIdentifier jarName;
    private FarragoRepos repos;
    private FarragoSessionFactory sessionFactory;
    private FemJar femJar;
    private String jarUrlUnexpanded;

    //~ Constructors -----------------------------------------------------------

    public DdlExtendCatalogStmt(SqlIdentifier jarName)
    {
        super(null);
        this.jarName = jarName;
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
        repos = ddlValidator.getRepos();
        sessionFactory =
            ddlValidator.getStmtValidator().getSession().getSessionFactory();
        femJar =
            ddlValidator.getStmtValidator().findSchemaObject(
                jarName,
                FemJar.class);
        if (femJar.isModelExtension()) {
            throw FarragoResource.instance().CatalogModelAlreadyImported.ex(
                repos.getLocalizedObjectName(femJar));
        }
        femJar.setModelExtension(true);
    }

    // implement FarragoSessionDdlStmt
    public void preExecute()
    {
        // We do the real work here since there's nothing session-specific
        // involved.  We don't bother updating transient information
        // like the FarragoDatabase list of installed model extensions,
        // because we're going to require a restart anyway.

        // TODO jvs 6-Apr-2005: verify that model name does not conflict with
        // any existing one.  Also, verify that we are in single-session
        // mode.

        // Before modifying the catalog, verify that we can actually
        // load the plugin; otherwise, we'll be in bad shape after
        // reboot.
        FarragoSessionModelExtension modelExtension =
            sessionFactory.newModelExtension(
                new FarragoPluginClassLoader(),
                femJar);
        // TODO:  trace information about modelExtension

        JarInputStream jarInputStream = null;

        try {
            jarUrlUnexpanded = femJar.getUrl();
            URL jarUrl = new URL(FarragoCatalogUtil.getJarUrl(femJar));
            jarInputStream = new JarInputStream(jarUrl.openStream());
            Manifest manifest = jarInputStream.getManifest();
            String xmiResourceName =
                manifest.getMainAttributes().getValue(
                    FarragoPluginClassLoader.PLUGIN_MODEL_ATTRIBUTE);
            URL xmiResourceUrl =
                new URL("jar:" + jarUrl + "!/" + xmiResourceName);
            FarragoReposUtil.importSubModel(
                repos.getMdrRepos(),
                xmiResourceUrl);
        } catch (Exception ex) {
            throw FarragoResource.instance().CatalogModelImportFailed.ex(
                repos.getLocalizedObjectName(femJar),
                ex);
        } finally {
            Util.squelchStream(jarInputStream);
        }
    }

    public String getJarUrl()
    {
        return jarUrlUnexpanded;
    }
}

// End DdlExtendCatalogStmt.java
