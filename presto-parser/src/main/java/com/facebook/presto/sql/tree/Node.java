/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.tree;

import com.google.common.base.Equivalence;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public abstract class Node
{
    public static final Equivalence<Node> NODE_VALUE_EQUIVALENCE = new Equivalence<Node>()
    {
        @Override
        protected int doHash(Node node)
        {
            return node.hash();
        }

        @Override
        protected boolean doEquivalent(Node a, Node b)
        {
            return a.isEqualTo(b);
        }
    };

    private final Optional<NodeLocation> location;

    protected Node(Optional<NodeLocation> location)
    {
        this.location = requireNonNull(location, "location is null");
    }

    /**
     * Accessible for {@link AstVisitor}, use {@link AstVisitor#process(Node, Object)} instead.
     */
    protected <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitNode(this, context);
    }

    public Optional<NodeLocation> getLocation()
    {
        return location;
    }

    public abstract List<? extends Node> getChildren();

    public final int hashCode()
    {
        // Prevent Nodes from being accidentally used in an equality-based collection
        throw new UnsupportedOperationException("Node doesn't implement hashCode, use Node.NODE_VALUE_EQUIVALENCE instead");
    }

    @Override
    public final boolean equals(Object obj)
    {
        // Prevent Nodes from being accidentally used in an equality-based collection
        throw new UnsupportedOperationException("Node doesn't implement equals, use Node.NODE_VALUE_EQUIVALENCE instead");
    }

    protected abstract int hash();

    protected abstract boolean isEqualTo(Node obj);

    @Override
    public abstract String toString();
}
