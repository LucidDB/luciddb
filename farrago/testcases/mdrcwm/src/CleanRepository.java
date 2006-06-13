import org.netbeans.api.mdr.*;

import javax.jmi.reflect.*;

/**
 * Utility to delete extent from the repository.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class CleanRepository
{
    public static void main(String [] args)
    {
        MDRManager manager = MDRManager.getDefault();
        try {
            MDRepository repos = MDRManager.getDefault().getDefaultRepository();
            RefPackage pkg = repos.getExtent("Mondrian");
            if (pkg != null) {
                pkg.refDelete();
            }
        } finally {
            manager.shutdownAll();
        }
    }
}

// End CleanRepository.java
