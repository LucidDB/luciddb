/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2003-2003 Disruptive Technologies, Inc.
// You must accept the terms in LICENSE.html to use this software.
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
package net.sf.saffron.core;

/**
 * A saffron connection which doesn't have any objects available.
 *
 * <p>A statement which uses this connection will only be able to reference
 * statement parameters.</p>
 *
 * @see SaffronConnection
 *
 * @author jhyde
 * @since Nov 28, 2003
 * @version $Id$
 **/
public class EmptySaffronConnection implements SaffronConnection {
    private final EmptySaffronSchema schema = new EmptySaffronSchema();

    public SaffronSchema getSaffronSchema() {
        return schema;
    }

    public Object contentsAsArray(String qualifier, String tableName) {
        return null;
    }

    private static class EmptySaffronSchema implements SaffronSchema {
        private final SaffronTypeFactoryImpl typeFactory = new SaffronTypeFactoryImpl();
        public SaffronTable getTableForMember(String[] names) {
            return null;
        }

        public SaffronTypeFactory getTypeFactory() {
            return typeFactory;
        }

        public void registerRules(SaffronPlanner planner) throws Exception {
        }
    }
}

// End EmptySaffronConnection.java
