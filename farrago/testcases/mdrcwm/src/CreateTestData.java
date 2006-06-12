import org.netbeans.api.mdr.*;

import org.omg.cwm.analysis.olap.*;
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
            OlapPackage pkg = (OlapPackage)
                repos.getExtent("Mondrian");
            createObjects(pkg);
        } finally {
            manager.shutdownAll();
        }
    }

    private static void createObjects(OlapPackage pkg)
    {
        Schema schema = pkg.getSchema().createSchema();
        schema.setName("FoodMart");

        Dimension dimension = pkg.getDimension().createDimension();
        dimension.setName("Store");

        schema.getDimension().add(dimension);
        
        LevelBasedHierarchy hierarchy =
            pkg.getLevelBasedHierarchy().createLevelBasedHierarchy();
        hierarchy.setName("Default");

        dimension.getHierarchy().add(hierarchy);

        Level level1 = pkg.getLevel().createLevel();
        level1.setName("Level1");

        Level level2 = pkg.getLevel().createLevel();
        level2.setName("Level2");

        dimension.getMemberSelection().add(level1);
        dimension.getMemberSelection().add(level2);

        HierarchyLevelAssociation level1a =
            pkg.getHierarchyLevelAssociation().
            createHierarchyLevelAssociation();
        level1a.setCurrentLevel(level1);
        level1a.setName("level1a");

        HierarchyLevelAssociation level2a =
            pkg.getHierarchyLevelAssociation().
            createHierarchyLevelAssociation();
        level2a.setCurrentLevel(level2);
        level2a.setName("level2a");

        hierarchy.getHierarchyLevelAssociation().add(level1a);
        hierarchy.getHierarchyLevelAssociation().add(level2a);
    }
}

// End CreateTestData.java
