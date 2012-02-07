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
package net.sf.farrago.rng;

import net.sf.farrago.session.*;
import net.sf.farrago.rng.resource.*;
import net.sf.farrago.ddl.DdlHandler;

import org.eigenbase.util.*;
import org.eigenbase.resource.*;
import org.eigenbase.resgen.*;
import org.eigenbase.sql.*;
import org.eigenbase.oj.rex.*;

import java.lang.reflect.*;
import java.util.*;

/**
 * FarragoRngPluginFactory implements the
 * {@link FarragoSessionPersonalityFactory} interface by producing
 * session personality instances capable of understanding RNG DDL statements.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoRngPluginFactory
    implements FarragoSessionPersonalityFactory,
        FarragoSessionModelExtensionFactory
{
    public static final FarragoRngResource res;

    static
    {
        // REVIEW jvs 12-Apr-2005:  FarragoRngResource.instance() has
        // problems loading the bundle from the jar.  Find out why;
        // I think it has to do with ResourceBundle.getBundle not
        // knowing what to do when called from a static method.
        try {
            res = new FarragoRngResource();
        } catch (Throwable ex) {
            throw Util.newInternal(ex);
        }
    }

    // implement FarragoSessionPersonalityFactory
    public FarragoSessionPersonality newSessionPersonality(
        FarragoSession session,
        FarragoSessionPersonality defaultPersonality)
    {
        // create a delegating proxy, overriding only the methods
        // we're interested in
        return (FarragoSessionPersonality)
            Proxy.newProxyInstance(
                FarragoRngPluginFactory.class.getClassLoader(),
                new Class[] {
                    FarragoSessionPersonality.class,
                },
                new RngPersonality(defaultPersonality));
    }

    // implement FarragoSessionModelExtensionFactory
    public FarragoSessionModelExtension newModelExtension()
    {
        return new RngModelExtension();
    }

    public static class RngPersonality extends DelegatingInvocationHandler
    {
        private final FarragoSessionPersonality defaultPersonality;

        RngPersonality(FarragoSessionPersonality defaultPersonality)
        {
            this.defaultPersonality = defaultPersonality;
        }

        // implement DelegatingInvocationHandler
        protected Object getTarget()
        {
            return defaultPersonality;
        }

        // implement FarragoSessionPersonality
        public FarragoSessionParser newParser(FarragoSession session)
        {
            return new FarragoRngParser();
        }

        // implement FarragoSessionPersonality
        public SqlOperatorTable getSqlOperatorTable(
            FarragoSessionPreparingStmt preparingStmt)
        {
            return FarragoRngOperatorTable.rngInstance();
        }

        // implement FarragoSessionPersonality
        public OJRexImplementorTable getOJRexImplementorTable(
            FarragoSessionPreparingStmt preparingStmt)
        {
            return FarragoRngImplementorTable.rngInstance();
        }

        // implement FarragoSessionPersonality
        public boolean supportsFeature(ResourceDefinition feature)
        {
            // We disable SELECT DISTINCT in this personality just
            // so we can test the ability to selectively disable features
            // within specific personalities.
            if (feature == EigenbaseResource.instance().SQLFeature_E051_01) {
                return false;
            }
            return defaultPersonality.supportsFeature(feature);
        }

        // NOTE:  we don't specify defineDdlHandlers here to avoid
        // duplication with RngModelExtension below.
    }

    public static class RngModelExtension
        implements FarragoSessionModelExtension
    {
        // implement FarragoSessionModelExtension
        public void defineDdlHandlers(
            FarragoSessionDdlValidator ddlValidator,
            List<DdlHandler> handlerList)
        {
            handlerList.add(
                new FarragoRngDdlHandler(ddlValidator));
        }

        // implement FarragoSessionModelExtension
        public void defineResourceBundles(
            List<ResourceBundle> bundleList)
        {
            bundleList.add(res);
        }

        // implement FarragoSessionModelExtension
        public void definePrivileges(
            FarragoSessionPrivilegeMap map)
        {
            // TODO:  invent a custom rng privilege
        }
    }
}

// End FarragoRngPluginFactory.java
