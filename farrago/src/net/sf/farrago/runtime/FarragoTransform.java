/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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
package net.sf.farrago.runtime;

import java.util.List;

/**
 * A piece of generated code must implement this interface if it is to
 * be callable from a Java execution object.
 *
 * @author Julian Hyde
 * @version $Id$
 */
public interface FarragoTransform
{
    /**
     * Returns a list of this transform's ports ({@link Port}), including the
     * objects they are bound to.
     */
    List getPorts();

    /**
     * Binds all ports, and initializes the transform.
     *
     * <p>For each port, the transform first calls
     * {@link Binding#getPort(FarragoTransform)} to identify the port to be
     * bound. It must not return null.
     * Then the transform obtains the object to be bound, by calling the
     * {@link Binding#getObjectToBind(FarragoTransform)} method.
     */
    void init(
        FarragoRuntimeContext connection,
        FarragoTransform.Binding[] bindings);

    /**
     * A port is an point where a transform needs to be connected to an
     * input or an output.
     */
    interface Port
    {
        /**
         * Returns the transform that this port belongs to.
         */
        FarragoTransform getTransform();

        /**
         * ID of port, unique within the transform.
         */
        int getOrdinal();

        /**
         * Whether input or output.
         */
        boolean isInput();

        /**
         * Binds this port to an object. The object must appropriate for the
         * type of port.
         *
         * @return The bound object, usually the same as the argument.
         */
        Object bind(Object o);

        /**
         * Returns the object bound to this port.
         */
        Object getBound();
    }

    /**
     * Binding of an object to a port.
     *
     * <p>A simple binding (see {@link BindingImpl}) just pairs a port name
     * with an object to bind it to. A more complex binding can create an
     * object dynamically.
     */
    interface Binding
    {
        /**
         * Looks up the corresponding port.
         *
         * @throws IllegalArgumentException if the port is not found
         */
        Port getPort(FarragoTransform transform);

        /**
         * Returns the object to connect to.
         *
         * <p>This method is called once, when the transform is instantiated.
         * For advanced transforms, this method can serve as a callback to do
         * setup work.
         */
        Object getObjectToBind(FarragoTransform transform);
    }

    /**
     * Basic implementation of {@link Port}.
     */
    public static class PortImpl
        implements Port
    {
        private final FarragoTransform transform;
        private final int ordinal;
        private final boolean input;
        private Object bound;

        public PortImpl(FarragoTransform transform, int ordinal, boolean input)
        {
            this.transform = transform;
            this.ordinal = ordinal;
            this.input = input;
        }

        public FarragoTransform getTransform()
        {
            return transform;
        }

        public int getOrdinal()
        {
            return ordinal;
        }

        public boolean isInput()
        {
            return input;
        }

        public Object bind(Object bindee)
        {
            return bound = bindee;
        }

        public Object getBound()
        {
            return bound;
        }
    }

    /**
     * Basic implementation of {@link Binding}.
     */
    public static class BindingImpl
        implements Binding
    {
        private final int portOrdinal;
        private final Object bindee;

        public BindingImpl(int portOrdinal, Object bindee)
        {
            this.portOrdinal = portOrdinal;
            this.bindee = bindee;
        }

        public Port getPort(FarragoTransform transform)
        {
            Port port = lookupPort(transform.getPorts(), portOrdinal);
            if (port == null) {
                throw new IllegalArgumentException("invalid portid " + portOrdinal);
            }
            return port;
        }

        public Object getObjectToBind(FarragoTransform transform)
        {
            return bindee;
        }

        /**
         * Looks up a port with a given ID.
         *
         * @param ports List of {@link Port} objects to search
         * @param portId Sought ID
         * @return port
         */
        private static FarragoTransform.Port lookupPort(
            List ports, int portId)
        {
            for (int i = 0; i < ports.size(); i++) {
                Port port = (Port) ports.get(i);
                if (port.getOrdinal() == portId) {
                    return port;
                }
            }
            return null;
        }
    }
}

// End FarragoTransform.java
