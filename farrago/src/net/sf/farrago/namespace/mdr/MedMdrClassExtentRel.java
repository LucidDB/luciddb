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

package net.sf.farrago.namespace.mdr;

import net.sf.saffron.core.*;
import net.sf.saffron.opt.*;
import net.sf.saffron.rel.*;
import net.sf.saffron.rex.*;
import net.sf.saffron.util.*;
import net.sf.saffron.oj.rel.*;
import net.sf.saffron.oj.util.*;
import net.sf.saffron.oj.*;
import net.sf.saffron.oj.stmt.*;

import net.sf.farrago.type.*;
import net.sf.farrago.util.*;

import openjava.ptree.*;
import openjava.mop.*;

import java.util.*;
import javax.jmi.model.*;
import javax.jmi.reflect.*;

import org.netbeans.lib.jmi.util.*;

import java.util.List;

/**
 * MedMdrClassExtentRel is the relational expression corresponding
 * to a scan over all rows of a MedMdrClassExtent.
 *
 *<p>
 *
 * TODO:  support push-down of projection and filtering
 *
 * @author John V. Sichi
 * @version $Id$
 */
class MedMdrClassExtentRel extends TableAccessRel
{
    /**
     * Refinement for super.table.
     */
    final MedMdrClassExtent mdrClassExtent;
    
    public MedMdrClassExtentRel(
        VolcanoCluster cluster,
        MedMdrClassExtent mdrClassExtent,
        SaffronConnection connection)
    {
        super(cluster,mdrClassExtent,connection);
        this.mdrClassExtent = mdrClassExtent;
    }

    // implement SaffronRel
    public CallingConvention getConvention()
    {
        return CallingConvention.ITERATOR;
    }

    // implement SaffronRel
    public Object implement(RelImplementor implementor,int ordinal)
    {
        assert (ordinal == -1);

        // convert array of names into an ExpressionList to be used in
        // generating lookup code
        ExpressionList nameList = new ExpressionList();
        for (int i = 0; i < mdrClassExtent.foreignName.length; ++i) {
            nameList.add(
                Literal.makeLiteral(mdrClassExtent.foreignName[i]));
        }

        Variable connectionVariable =
            new Variable(OJStatement.connectionVariable);

        Expression metaClassExpression =
            new CastExpression(
                OJClass.forClass(RefClass.class),
                new MethodCall(
                    connectionVariable,
                    "getDataServerRuntimeSupport",
                    new ExpressionList(
                        Literal.makeLiteral(
                            mdrClassExtent.directory.dataWrapper.serverMofId),
                        new ArrayAllocationExpression(
                            TypeName.forOJClass(OJSystem.STRING),
                            new ExpressionList(null),
                            new ArrayInitializer(nameList)))));
        Expression collectionExpression =
            new MethodCall(
                metaClassExpression,
                "refAllOfType",
                new ExpressionList());
        Expression iterExpression =
            new MethodCall(
                collectionExpression,
                "iterator",
                new ExpressionList());

        Variable varInputRow = implementor.newVariable();

        TagProvider tagProvider = new TagProvider();

        // Look up the Java interface generated for the class being queried.
        String className = tagProvider.getImplFullName(
            (ModelElement) (mdrClassExtent.refClass.refMetaObject()),
            TagProvider.INSTANCE);
        assert(className.endsWith("Impl"));
        className = className.substring(0,className.length() - 4);
        Class rowClass;
        boolean useReflection = false;
        try {
            rowClass = Class.forName(className);
        } catch (ClassNotFoundException ex) {
            // This is possible when we're querying an external repository
            // for which we don't know the class mappings.  Do everything
            // via JMI reflection instead.
            rowClass = RefObject.class;
            useReflection = true;
        }

        // This is a little silly.  Have to put in a dummy cast
        // so that type inference will stop early (otherwise it
        // fails to find variable reference).
        Expression castInputRow = new CastExpression(
            OJClass.forClass(rowClass),varInputRow);
        
        SaffronType inputRowType = getCluster().typeFactory.createJavaType(
            rowClass);
        SaffronType outputRowType = getRowType();

        List features = JmiUtil.getFeatures(
            mdrClassExtent.refClass,StructuralFeature.class);
        int n = features.size();
        Expression [] accessorExps = new Expression[n + 2];
        SaffronField [] outputFields = outputRowType.getFields();
        for (int i = 0; i < n; ++i) {
            StructuralFeature feature = (StructuralFeature) features.get(i);
            if (useReflection) {
                accessorExps[i] = new MethodCall(
                    castInputRow,
                    "refGetValue",
                    new ExpressionList(Literal.makeLiteral(feature.getName())));
            } else {
                String accessorName = tagProvider.getSubstName(feature);
                String prefix = null;
                if (feature.getType().getName().equals("Boolean")) {
                    if (!accessorName.startsWith("is")) {
                        prefix = "is";
                    }
                } else {
                    prefix = "get";
                }
                if (prefix != null) {
                    accessorName = prefix
                        + Character.toUpperCase(accessorName.charAt(0))
                        + accessorName.substring(1);
                }
                accessorExps[i] = new MethodCall(
                    castInputRow,
                    accessorName,
                    new ExpressionList());
            }
            if (feature instanceof Reference) {
                CastExpression castExp = new CastExpression(
                    OJClass.forClass(RefBaseObject.class),
                    accessorExps[i]);
                accessorExps[i] = new MethodCall(
                    connectionVariable,
                    "getRefMofId",
                    new ExpressionList(castExp));
            }
        }

        // tack on the object's own MofId
        accessorExps[n] = new MethodCall(
            castInputRow,
            "refMofId",
            new ExpressionList());

        // and class name
        accessorExps[n+1] = new MethodCall(
            new MethodCall(
                castInputRow,
                "refMetaObject",
                new ExpressionList()),
            "refGetValue",
            new ExpressionList(
                Literal.makeLiteral("name")));

        JavaRexBuilder javaRexBuilder =
            (JavaRexBuilder) getCluster().rexBuilder;
        RexNode [] rexExps = new RexNode[accessorExps.length];
        for (int i = 0; i < accessorExps.length; ++i) {
            rexExps[i] = javaRexBuilder.makeJava(
                getCluster().env,accessorExps[i]);
            // REVIEW:  This cast may cause the generated code to forget
            // something important like pad/truncate.
            rexExps[i] = javaRexBuilder.makeAbstractCast(
                outputFields[i].getType(),
                rexExps[i]);
        }
        
        return IterCalcRel.implementAbstract(
            implementor,
            this,
            iterExpression,
            varInputRow,
            inputRowType,
            outputRowType,
            null,
            rexExps);
    }
}

// End MedMdrClassExtentRel.java
