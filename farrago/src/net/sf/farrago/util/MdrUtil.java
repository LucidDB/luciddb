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
package net.sf.farrago.util;

import java.io.*;
import java.util.*;
import java.util.logging.*;

import org.netbeans.api.mdr.*;
import org.netbeans.mdr.*;
import org.netbeans.mdr.persistence.btreeimpl.btreestorage.*;

import org.openide.ErrorManager;
import org.openide.util.Lookup;
import org.openide.util.lookup.*;

// NOTE:  This class gets compiled independently of everything else since
// it is used by build-time utilities such as ProxyGen.  That means it must
// have no dependencies on other Farrago code.

/**
 * Static MDR utilities.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class MdrUtil
{
    //~ Static fields/initializers --------------------------------------------

    // NOTE jvs 23-Dec-2004: This tracer cannot be statically initialized,
    // because MdrUtil is not allowed to depend on FarragoTrace.  Instead, this
    // tracer must be initialized via the integrateTracing() method.  Don't use
    // it outside of TracingErrorManager.
    private static Logger tracer;
    
    //~ Methods ---------------------------------------------------------------

    /**
     * Loads an MDRepository instance.
     *
     * @param storageFactoryClassName fully qualified name of the class
     * used to implement repository storage (if null, this
     * defaults to BtreeFactory if not specified as an entry in storageProps)
     *
     * @param storageProps storage-specific properties
     * (with or without the MDRStorageProperty prefix)
     *
     * @return loaded repository
     */
    public static MDRepository loadRepository(
        String storageFactoryClassName,
        Properties storageProps)
    {
        Iterator iter;
        String classNameProp =
            "org.netbeans.mdr.storagemodel.StorageFactoryClassName";
        Properties sysProps = System.getProperties();
        Map savedProps = new HashMap();

        String storagePrefix = "MDRStorageProperty.";

        if (storageFactoryClassName == null) {
            // may be specified as a property
            storageFactoryClassName = storageProps.getProperty(classNameProp);
        }
        
        if (storageFactoryClassName == null) {
            // use default
            storageFactoryClassName = BtreeFactory.class.getName();
        }

        if (storageFactoryClassName.equals(BtreeFactory.class.getName())) {
            // special case
            storagePrefix = "";
        }

        // save existing system properties first
        savedProps.put(
            classNameProp,
            sysProps.get(classNameProp));
        iter = storageProps.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry entry = (Map.Entry) iter.next();
            String propName = applyPrefix(
                storagePrefix, entry.getKey().toString());
            savedProps.put(
                propName,
                sysProps.get(propName));
        }

        try {
            // set desired properties
            sysProps.put(classNameProp, storageFactoryClassName);
            iter = storageProps.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry entry = (Map.Entry) iter.next();
                sysProps.put(
                    applyPrefix(storagePrefix, entry.getKey().toString()),
                    entry.getValue());
            }

            // load repository
            return new NBMDRepositoryImpl();
        } finally {
            // restore saved system properties
            iter = savedProps.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry entry = (Map.Entry) iter.next();
                if (entry.getValue() == null) {
                    sysProps.remove(entry.getKey());
                } else {
                    sysProps.put(
                        entry.getKey(),
                        entry.getValue());
                }
            }
        }
    }

    private static String applyPrefix(String storagePrefix, String propName)
    {
        if (propName.startsWith(storagePrefix)) {
            return propName;
        }
        return storagePrefix + propName;
    }

    private static final String LOOKUP_PROP_NAME = "org.openide.util.Lookup";

    /**
     * Integrates MDR tracing with Farrago tracing.  Must be called
     * before first usage of MDR.
     *
     * @param mdrTracer Logger for MDR tracing
     */
    public static void integrateTracing(Logger mdrTracer)
    {
        tracer = mdrTracer;
        
        // Install a lookup mechanism which will register our
        // TracingErrorManager.
        try {
            System.setProperty(
                LOOKUP_PROP_NAME,
                TraceIntegrationLookup.class.getName());

            // Force load of our lookup now if it hasn't been done yet.
            Lookup.getDefault();
        } finally {
            System.getProperties().remove(LOOKUP_PROP_NAME);
        }
    }

    /**
     * Helper class for implementing Farrago/MDR trace integration.
     */
    public static class TraceIntegrationLookup extends ProxyLookup
    {
        public TraceIntegrationLookup()
        {
            // Delete the property we set to get here.
            System.getProperties().remove(LOOKUP_PROP_NAME);

            // Now it's safe to call the default.  This
            // is a recursive call, but it will hit the base case
            // now since we cleared the property first.
            Lookup defaultLookup = Lookup.getDefault();

            // Register our custom ErrorManager together with the default.
            ErrorManager em = new TracingErrorManager(tracer);
            setLookups(new Lookup[]{
                defaultLookup, 
                Lookups.singleton(em)
            });
        }
    }

    /**
     * TracingErrorManager overrides the Netbeans ErrorManager to
     * intercept messages and route them to Farrago tracing.
     */
    private static class TracingErrorManager extends ErrorManager
    {
        private final Logger tracer;
        
        TracingErrorManager(Logger tracer)
        {
            this.tracer = tracer;
        }

        // implement ErrorManager
        public Throwable attachAnnotations(
            Throwable t, Annotation[] arr)
        {
            return null;
        }
        
        // implement ErrorManager
        public Annotation[] findAnnotations(Throwable t)
        {
            return null;
        }
        
        // implement ErrorManager
        public Throwable annotate(
            Throwable t, int severity,
            String message, String localizedMessage,
            Throwable stackTrace, java.util.Date date)
        {
            tracer.throwing(
                "MdrUtil.TracingErrorManager", "annotate", stackTrace);
            tracer.log(convertSeverity(severity), message);
            return t;
        }
        
        // implement ErrorManager
        public void notify(int severity, Throwable t)
        {
            tracer.throwing("MdrUtil.TracingErrorManager", "notify", t);
        }
        
        // implement ErrorManager
        public void log(int severity, String s)
        {
            tracer.log(convertSeverity(severity), s);
        }

        private static Level convertSeverity(int severity)
        {
            switch(severity) {
            case INFORMATIONAL:
                return Level.FINE;
            case WARNING:
                return Level.WARNING;
            case USER:
                return Level.INFO;
            case EXCEPTION:
                return Level.SEVERE;
            case ERROR:
                return Level.SEVERE;
            default:
                return Level.FINER;
            }
        }
        
        // implement ErrorManager
        public ErrorManager getInstance(String name)
        {
            return this;
        }
    }
}


// End MdrUtil.java
