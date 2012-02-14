/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/
package net.sf.farrago.namespace.mdr;

import java.util.*;
import java.util.List;

import javax.jmi.model.*;
import javax.jmi.reflect.*;

import net.sf.farrago.query.*;

import openjava.mop.*;

import openjava.ptree.*;

import org.eigenbase.jmi.*;
import org.eigenbase.oj.rel.*;
import org.eigenbase.oj.stmt.*;
import org.eigenbase.oj.util.*;
import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.runtime.*;


/**
 * MedMdrClassExtentRel is the relational expression corresponding to a scan
 * over all rows of a MedMdrClassExtent.
 *
 * <p>TODO: support push-down of projection and filtering
 *
 * @author John V. Sichi
 * @version $Id$
 */
class MedMdrClassExtentRel
    extends TableAccessRelBase
    implements JavaRel
{
    //~ Instance fields --------------------------------------------------------

    /**
     * Refinement for super.table.
     */
    final MedMdrClassExtent mdrClassExtent;
    Class<? extends RefObject> rowClass;
    boolean useReflection;

    //~ Constructors -----------------------------------------------------------

    public MedMdrClassExtentRel(
        RelOptCluster cluster,
        MedMdrClassExtent mdrClassExtent,
        RelOptConnection connection)
    {
        super(
            cluster,
            new RelTraitSet(CallingConvention.ITERATOR),
            mdrClassExtent,
            connection);
        this.mdrClassExtent = mdrClassExtent;

        FarragoPreparingStmt preparingStmt =
            FarragoRelUtil.getPreparingStmt(this);

        rowClass =
            JmiObjUtil.getClassForRefClass(
                preparingStmt.getRepos().getMdrRepos(),
                mdrClassExtent.refClass);
        useReflection = (rowClass == RefObject.class);
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelNode
    public MedMdrClassExtentRel clone()
    {
        MedMdrClassExtentRel clone =
            new MedMdrClassExtentRel(
                getCluster(),
                mdrClassExtent,
                connection);
        clone.inheritTraitsFrom(this);
        return clone;
    }

    private String [] getRuntimeName(RefBaseObject refObject)
    {
        List<String> nameList = new ArrayList<String>();

        boolean useModel = (refObject instanceof ModelElement);

        // determine the path from the root package to refObject
        RefBaseObject refPackage = JmiObjUtil.getContainer(refObject);
        String rootPackageName =
            JmiObjUtil.getMetaObjectName(
                mdrClassExtent.directory.server.getExtentPackage());
        for (;;) {
            String packageName = JmiObjUtil.getMetaObjectName(refPackage);
            if (packageName.equals(rootPackageName)) {
                break;
            }

            // we're building up the list in reverse order
            nameList.add(0, packageName);
            refPackage = JmiObjUtil.getContainer(refPackage);
            assert (refPackage != null);
        }

        nameList.add(JmiObjUtil.getMetaObjectName(refObject));
        String typeName;
        if (useModel) {
            typeName = JmiObjUtil.getMetaObjectName(refObject.refMetaObject());
        } else {
            typeName =
                JmiObjUtil.getMetaObjectName(
                    refObject.refMetaObject().refMetaObject());
        }
        nameList.add(typeName);
        return nameList.toArray(new String[0]);
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

    public Expression getCollectionExpression()
    {
        Expression metaClassExpression =
            new CastExpression(
                OJClass.forClass(RefClass.class),
                getRefBaseObjectRuntimeExpression(mdrClassExtent.refClass));
        Expression collectionExpression =
            new MethodCall(
                metaClassExpression,
                "refAllOfType",
                new ExpressionList());
        return collectionExpression;
    }

    // implement RelNode
    public ParseTree implement(JavaRelImplementor implementor)
    {
        Variable varInputRow = implementor.newVariable();

        RelDataType inputRowType =
            getCluster().getTypeFactory().createJavaType(rowClass);
        RelDataType outputRowType = getRowType();

        RexNode [] rexExps = implementProjection(varInputRow);

        final RexProgram program =
            RexProgram.create(
                inputRowType,
                rexExps,
                null,
                outputRowType,
                getCluster().getRexBuilder());

        Expression collectionExpression = getCollectionExpression();
        Expression adapterExp =
            new AllocationExpression(
                OJUtil.typeNameForClass(RestartableCollectionTupleIter.class),
                new ExpressionList(
                    collectionExpression));
        return IterCalcRel.implementAbstractTupleIter(
            implementor,
            this,
            adapterExp,
            varInputRow,
            inputRowType,
            outputRowType,
            program,
            null);
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
        List<StructuralFeature> features =
            JmiObjUtil.getFeatures(
                mdrClassExtent.refClass,
                StructuralFeature.class,
                false);
        int n = features.size();
        Expression [] accessorExps = new Expression[n + 2];
        RelDataTypeField [] outputFields = outputRowType.getFields();

        for (int i = 0; i < n; ++i) {
            StructuralFeature feature = features.get(i);
            if (useReflection) {
                accessorExps[i] =
                    new MethodCall(
                        castInputRow,
                        "refGetValue",
                        new ExpressionList(
                            Literal.makeLiteral(feature.getName())));
            } else {
                String accessorName = JmiObjUtil.getAccessorName(feature);
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
            (JavaRexBuilder) getCluster().getRexBuilder();
        RexNode [] rexExps = new RexNode[accessorExps.length];
        for (int i = 0; i < accessorExps.length; ++i) {
            rexExps[i] =
                javaRexBuilder.makeJava(
                    getCluster().getEnv(),
                    accessorExps[i]);

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
