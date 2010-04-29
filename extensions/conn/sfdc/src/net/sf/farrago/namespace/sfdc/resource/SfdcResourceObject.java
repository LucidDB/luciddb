/*
// $Id$
// (C) Copyright 2006-2006 LucidEra, Inc.
 */
package net.sf.farrago.namespace.sfdc.resource;

/**
 * Contains a singleton instance of {@link SfdcResource} class for default
 * locale. Note: this is a workaround since SfdcResource.instance() has problems
 * loading the bundle from the jar.
 *
 * @author Elizabeth Lin
 * @version $Id$
 */
public class SfdcResourceObject
{
    //~ Static fields/initializers ---------------------------------------------

    private static final SfdcResource res;

    static {
        try {
            res = new SfdcResource();
        } catch (Throwable ex) {
            throw new Error(ex);
        }
    }

    //~ Methods ----------------------------------------------------------------

    public static SfdcResource get()
    {
        return res;
    }
}

// End SfdcResourceObject.java
