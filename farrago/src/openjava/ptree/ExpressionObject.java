/*
 * ExpressionObject.java 1.0
 *
 *
 * June 23, 2000
 *
 * @see openjava.ptree.ParseTree
 * @version 1.0 last updated:  June 23, 2000
 * @author  Michiaki Tatsubori
 */
package openjava.ptree;


import openjava.mop.*;



/**
 * Abstract implementation of {@link Expression}.
 *
 * @see openjava.ptree.ParseTree
 * @see openjava.ptree.NonLeaf
 */
public abstract class ExpressionObject extends NonLeaf
    implements Expression
{
    private OJClass cachedType = null;

    void soilCache() {
        cachedType = null;
        ParseTree parent = getParent();
        if (parent instanceof ExpressionObject) {
            ExpressionObject pexp = (ExpressionObject) parent;
            pexp.soilCache();
        }
    }

    /**
     * dirty implementation
     */
    public OJClass getCachedType(Environment env) throws Exception {
        if (cachedType == null)  cachedType = getType(env);
        return cachedType;
    }

    public abstract OJClass getType(Environment env, boolean using_cache)
        throws Exception;

    public abstract OJClass getType(Environment env)
        throws Exception;

    /**
     * Makes this ptree a list presenting for
     * [ p ]
     *
     * @param  ptrees  list's element
     */
    protected void set( Object[] ptrees )  {
        soilCache();
        super.set(ptrees);
    }

}
