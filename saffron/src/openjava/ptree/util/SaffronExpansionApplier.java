/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Tech
// You must accept the terms in LICENSE.html to use this software.
*/

package openjava.ptree.util;

import openjava.ptree.CompilationUnit;
import openjava.ptree.ParseTreeException;
import openjava.mop.Environment;
import net.sf.saffron.oj.xlat.OJSchemaExpander;
import net.sf.saffron.oj.xlat.OJQueryExpander;
import net.sf.saffron.core.EmptySaffronConnection;
import org.eigenbase.relopt.RelOptSchema;
import org.eigenbase.relopt.RelOptConnection;

/**
 * <code>SaffronExpansionApplier</code> performs the same expansions as its
 * base class {@link ExpansionApplier}, plus it expands references to objects
 * of type {@link RelOptSchema} and converts queries into regular Java code.
 *
 * @author jhyde
 * @since 15 February, 2002
 * @version $Id$
 **/
public class SaffronExpansionApplier extends ExpansionApplier {
    private RelOptConnection connection = new EmptySaffronConnection();

    public SaffronExpansionApplier( Environment env ) {
        super( env );
    }
    // override
    public CompilationUnit evaluateUp( CompilationUnit ptree )
            throws ParseTreeException
    {
        Environment env = getEnvironment();
        OJSchemaExpander schemaExpander = new OJSchemaExpander(env);
        ptree.accept( schemaExpander );

        QueryExpander queryExpander = new OJQueryExpander(env, connection);
        ptree.accept( queryExpander );

        return super.evaluateUp(ptree);
    }
}


// End SaffronExpansionApplier.java
