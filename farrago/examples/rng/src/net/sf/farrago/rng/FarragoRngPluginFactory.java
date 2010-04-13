/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
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
