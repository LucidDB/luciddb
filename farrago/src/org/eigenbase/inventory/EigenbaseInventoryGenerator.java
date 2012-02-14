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
package org.eigenbase.inventory;

import com.sun.mirror.apt.*;
import com.sun.mirror.declaration.*;
import com.sun.mirror.type.*;
import com.sun.mirror.util.*;

import java.util.*;

import static java.util.Collections.*;
import static com.sun.mirror.util.DeclarationVisitors.*;


/**
 * Produces an XML file containing an inventory of all Eigenbase components
 * by processing inventory annotations in source files.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class EigenbaseInventoryGenerator implements AnnotationProcessorFactory
{
    // Process any set of annotations
    private static final Collection<String> supportedAnnotations =
        unmodifiableCollection(Arrays.asList("*"));

    // No supported options
    private static final Collection<String> supportedOptions = emptySet();

    public Collection<String> supportedAnnotationTypes()
    {
        return supportedAnnotations;
    }

    public Collection<String> supportedOptions()
    {
        return supportedOptions;
    }

    public AnnotationProcessor getProcessorFor(
        Set<AnnotationTypeDeclaration> atds,
        AnnotationProcessorEnvironment env)
    {
        return new InventoryProcessor(env);
    }

    private static class InventoryProcessor implements AnnotationProcessor
    {
        private final AnnotationProcessorEnvironment env;
        private final Set<String> packageNames = new HashSet<String>();

        InventoryProcessor(AnnotationProcessorEnvironment env)
        {
            this.env = env;
        }

        public void process()
        {
            for (TypeDeclaration decl : env.getSpecifiedTypeDeclarations()) {
                decl.accept(
                    getDeclarationScanner(
                        new InventoryVisitor(),
                        NO_OP));
            }
        }

        private class InventoryVisitor extends SimpleDeclarationVisitor
        {
            public void visitClassDeclaration(ClassDeclaration d) {
                visitPackageDeclaration(d.getPackage());
            }

            public void visitPackageDeclaration(PackageDeclaration d)
            {
                if (packageNames.contains(d.getQualifiedName())) {
                    return;
                }
                packageNames.add(d.getQualifiedName());
                System.out.println(d.getQualifiedName());
                for (AnnotationMirror m : d.getAnnotationMirrors()) {
                    for (Map.Entry<AnnotationTypeElementDeclaration,
                             AnnotationValue> e
                             : m.getElementValues().entrySet())
                    {
                        System.out.print(e.getKey().getSimpleName());
                        System.out.print(" = ");
                        System.out.println(e.getValue().toString());
                    }
                }
            }
        }
    }
}

// End EigenbaseInventoryGenerator.java
