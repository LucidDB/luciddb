import org.netbeans.api.mdr.*;

import org.omg.cwm.resource.relational.*;
import org.omg.cwm.objectmodel.core.*;

/**
 * Test for MDR export/import of CWM data.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class CreateTestData
{
    public static void main(String [] args)
    {
        MDRManager manager = MDRManager.getDefault();
        try {
            MDRepository repos = MDRManager.getDefault().getDefaultRepository();
            RelationalPackage pkg = (RelationalPackage)
                repos.getExtent("Original");
            createObjects(pkg);
        } finally {
            manager.shutdownAll();
        }
    }

    private static void createObjects(RelationalPackage pkg)
    {
        Catalog catalog = pkg.getCatalog().createCatalog();
        catalog.setName("C1");
        Schema schema = pkg.getSchema().createSchema();
        schema.setName("S1");
        catalog.getOwnedElement().add(schema);
    }
}

// End CreateTestData.java
