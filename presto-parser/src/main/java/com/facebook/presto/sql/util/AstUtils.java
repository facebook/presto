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
package com.facebook.presto.sql.util;

import com.facebook.presto.sql.tree.Node;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Objects.requireNonNull;

public class AstUtils
{
    public static boolean nodeContains(Node node, Node subNode)
    {
        requireNonNull(node, "node is null");
        requireNonNull(subNode, "subNode is null");

        return preOrder(node)
                .anyMatch(childNode -> childNode == subNode);
    }

    public static Stream<Node> preOrder(Node node)
    {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(new PreOrderIterator(node), 0), false);
    }

    private static final class PreOrderIterator
            implements Iterator<Node>
    {
        private final Deque<Node> remaining = new ArrayDeque<>();

        public PreOrderIterator(Node node)
        {
            remaining.push(node);
        }

        @Override
        public boolean hasNext()
        {
            return remaining.size() > 0;
        }

        @Override
        public Node next()
        {
            Node node = remaining.pop();
            node.getChildren().forEach(remaining::push);
            return node;
        }
    }

    private AstUtils() {}
}
