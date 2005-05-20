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
package net.sf.farrago.namespace.mdr;

import java.util.*;
import java.util.List;

import javax.jmi.model.*;
import javax.jmi.reflect.*;

import net.sf.farrago.type.*;
import net.sf.farrago.util.*;

import openjava.mop.*;
import openjava.ptree.*;

import org.eigenbase.oj.*;
import org.eigenbase.oj.rel.*;
import org.eigenbase.oj.stmt.*;
import org.eigenbase.oj.util.*;
import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.util.*;


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
class MedMdrClassExtentRel extends TableAccessRel implements JavaRel
{
    //~ Instance fields -------------------------------------------------------

    /**
     * Refinement for super.table.
     */
    final MedMdrClassExtent mdrClassExtent;
    Class rowClass;
    boolean useReflection;

    //~ Constructors ----------------------------------------------------------

    public MedMdrClassExtentRel(
        RelOptCluster cluster,
        MedMdrClassExtent mdrClassExtent,
        RelOptConnection connection)
    {
        super(
            cluster, new RelTraitSet(CallingConvention.ITERATOR),
            mdrClassExtent, connection);
        this.mdrClassExtent = mdrClassExtent;

        rowClass = JmiUtil.getClassForRefClass(mdrClassExtent.refClass);
        useReflection = (rowClass == RefObject.class);
    }

    //~ Methods ---------------------------------------------------------------

    // implement RelNode
    public Object clone()
    {
        MedMdrClassExtentRel clone =
            new MedMdrClassExtentRel(cluster, mdrClassExtent, connection);
        clone.traits = cloneTraits();
        return clone;
    }

    private String [] getRuntimeName(RefBaseObject refObject)
    {
        List nameList = new ArrayList();

        boolean useModel = (refObject instanceof ModelElement);

        // determine the path from the root package to refObject
        RefBaseObject refPackage = JmiUtil.getContainer(refObject);
        String rootPackageName =
            JmiUtil.getMetaObjectName(
                mdrClassExtent.directory.server.getExtentPackage());
        for (;;) {
            String packageName = JmiUtil.getMetaObjectName(refPackage);
            if (packageName.equals(rootPackageName)) {
                break;
            }

            // we're building up the list in reverse order
            nameList.add(0, packageName);
            refPackage = JmiUtil.getContainer(refPackage);
            assert (refPackage != null);
        }

        nameList.add(JmiUtil.getMetaObjectName(refObject));
        String typeName;
        if (useModel) {
            typeName = JmiUtil.getMetaObjectName(refObject.refMetaObject());
        } else {
            typeName =
                JmiUtil.getMetaObjectName(
                    refObject.refMetaObject().refMetaObject());
        }
        nameList.add(typeName);
        return (String []) nameList.toArray(new String[0]);
    }

    RefBaseObject getRefObjectFromModelElement(ModelElement modelElement)
    {
        String [] runtimeName = getRuntimeName(modelElement);
        MedMdrNameDirectory extentDirectory =
            mdrClassExtent.directory.server.getExtentNameDirectory();
        return extentDirectory.lookupRefBaseObject(runtimeName);
    }

    Expression getRefBaseObjectRuntimeExpression(RefBaseObject refObject)
    {
        String [] runtimeName = getRuntimeName(refObject);

        ExpressionList nameList = new ExpressionList();
        for (int i = 0; i < runtimeName.length; ++i) {
            nameList.add(Literal.makeLiteral(runtimeName[i]));
        }

        return mdrClassExtent.directory.server.generateRuntimeSupportCall(
            new ArrayAllocationExpression(
                TypeName.forOJClass(OJSystem.STRING),
                new ExpressionList(null),
                new ArrayInitializer(nameList)));
    }

    public Expression getIteratorExpression()
    {
        Expression metaClassExpression =
            new CastExpression(
                OJClass.forClass(RefClass.class),
                getRefBaseObjectRuntimeExpression(mdrClassExtent.refClass));
        Expression collectionExpression =
            new MethodCall(metaClassExpression, "refAllOfType",
                new ExpressionList());
        Expression iterExpression =
            new MethodCall(collectionExpression, "iterator",
                new ExpressionList());

        return iterExpression;
    }

    // implement RelNode
    public ParseTree implement(JavaRelImplementor implementor)
    {
        Expression iterExpression = getIteratorExpression();

        Variable varInputRow = implementor.newVariable();

        RelDataType inputRowType =
            getCluster().typeFactory.createJavaType(rowClass);
        RelDataType outputRowType = getRowType();

        RexNode [] rexExps = implementProjection(varInputRow);

        return IterCalcRel.implementAbstract(implementor, this,
            iterExpression, varInputRow, inputRowType, outputRowType, null,
            rexExps);
    }

    RexNode [] implementProjection(Expression inputRow)
    {
        Variable connectionVariable =
            new Variable(OJPreparingStmt.connectionVariable);

        // This is a little silly.  Have to put in a dummy cast
        // so that type inference will stop early (otherwise it
        // fails to find variable reference).
        Expression castInputRow =
            new CastExpression(
                OJClass.forClass(rowClass),
                inputRow);

        RelDataType outputRowType = getRowType();
        List features =
            JmiUtil.getFeatures(mdrClassExtent.refClass,
                StructuralFeature.class, false);
        int n = features.size();
        Expression [] accessorExps = new Expression[n + 2];
        RelDataTypeField [] outputFields = outputRowType.getFields();

        for (int i = 0; i < n; ++i) {
            StructuralFeature feature = (StructuralFeature) features.get(i);
            if (useReflection) {
                accessorExps[i] =
                    new MethodCall(
                        castInputRow,
                        "refGetValue",
                        new ExpressionList(
                            Literal.makeLiteral(feature.getName())));
            } else {
                String accessorName = JmiUtil.getAccessorName(feature);
                accessorExps[i] =
                    new MethodCall(
                        castInputRow,
                        accessorName,
                        new ExpressionList());
            }
            if (feature instanceof Reference) {
                CastExpression castExp =
                    new CastExpression(
                        OJClass.forClass(RefBaseObject.class),
                        accessorExps[i]);
                accessorExps[i] =
                    new MethodCall(
                        connectionVariable,
                        "getRefMofId",
                        new ExpressionList(castExp));
            }
        }

        // tack on the object's own MofId
        accessorExps[n] =
            new MethodCall(
                castInputRow,
                "refMofId",
                new ExpressionList());

        // and class name
        accessorExps[n + 1] =
            new MethodCall(
                new MethodCall(
                    castInputRow,
                    "refMetaObject",
                    new ExpressionList()),
                "refGetValue",
                new ExpressionList(Literal.makeLiteral("name")));

        JavaRexBuilder javaRexBuilder =
            (JavaRexBuilder) getCluster().rexBuilder;
        RexNode [] rexExps = new RexNode[accessorExps.length];
        for (int i = 0; i < accessorExps.length; ++i) {
            rexExps[i] =
                javaRexBuilder.makeJava(getCluster().env, accessorExps[i]);

            // REVIEW:  This cast may cause the generated code to forget
            // something important like pad/truncate.
            rexExps[i] =
                javaRexBuilder.makeAbstractCast(
                    outputFields[i].getType(),
                    rexExps[i]);
        }
        return rexExps;
    }
}


// End MedMdrClassExtentRel.java
