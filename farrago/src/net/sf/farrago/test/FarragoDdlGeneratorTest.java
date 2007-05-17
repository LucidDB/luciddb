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
package net.sf.farrago.test;

import junit.framework.Test;
import net.sf.farrago.cwm.core.CwmModelElement;
import net.sf.farrago.cwm.relational.CwmCatalog;
import net.sf.farrago.cwm.relational.CwmSchema;
import net.sf.farrago.ddl.gen.DdlGenerator;
import net.sf.farrago.ddl.gen.FarragoDdlGenerator;
import net.sf.farrago.fem.med.FemDataServer;
import net.sf.farrago.fem.med.FemDataWrapper;
import org.eigenbase.test.DiffRepository;

import java.util.ArrayList;
import java.util.List;

/**
 * Unit test cases for {@link FarragoDdlGenerator}.
 *
 * @author Jason Ouellette
 * @version $Id$
 */
public class FarragoDdlGeneratorTest
    extends FarragoTestCase
{

    //~ Constructors -----------------------------------------------------------

    public FarragoDdlGeneratorTest(String testName)
        throws Exception
    {
        super(testName);
    }

    //~ Helper methods ---------------------------------------------------------

    protected DdlGenerator newDdlGenerator()
    {
        return new FarragoDdlGenerator();
    }

    protected DiffRepository getDiffRepos()
    {
        return DiffRepository.lookup(FarragoDdlGeneratorTest.class);
    }

    public static Test suite()
    {
        return wrappedSuite(FarragoDdlGeneratorTest.class);
    }

    //~ Testcase methods -------------------------------------------------------

    public void testExportSales()
    {
        String output = exportSchema("SALES", true);
        getDiffRepos().assertEquals("output", "${output}", output);
    }

    private String exportSchema(
        String schemaName,
        boolean includeNonSchemaElements)
    {
        DdlGenerator ddlGen = newDdlGenerator();
        List<CwmModelElement> list = new ArrayList<CwmModelElement>();
        gatherElements(list, schemaName, includeNonSchemaElements);
        return ddlGen.getExportText(list);
    }

    /**
     * Gathers a list of elements in a schema, optionally including elements
     * which don't belong to any schema.
     *
     * @param list List to populate
     * @param schemaName Name of schema
     *
     * @param includeNonSchemaElements Whether to include elements which do not
     * belong to a schema
     */
    protected void gatherElements(
        List<CwmModelElement> list,
        String schemaName,
        boolean includeNonSchemaElements)
    {
        CwmCatalog catalog = repos.getSelfAsCatalog();
        for (CwmModelElement element : catalog.getOwnedElement()) {
            if (element instanceof CwmSchema) {
                CwmSchema schema = (CwmSchema) element;
                if (schema.getName().equals(schemaName)) {
                    list.add(schema);
                    for (CwmModelElement element2 : schema.getOwnedElement()) {
                        list.add(element2);
                    }
                }
            } else if (includeNonSchemaElements) {
                list.add(element);
            }
        }
        if (includeNonSchemaElements) {
            for (FemDataServer dataServer : repos.allOfType(FemDataServer.class)) {
                list.add(dataServer);
            }
            for (FemDataWrapper dataWrapper : repos.allOfType(FemDataWrapper.class)) {
                list.add(dataWrapper);
            }
        }
    }

}

// End FarragoDdlGeneratorTest.java
