/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
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
    private static final Collection<String> supportedAnnotations
        = unmodifiableCollection(Arrays.asList("*"));

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
