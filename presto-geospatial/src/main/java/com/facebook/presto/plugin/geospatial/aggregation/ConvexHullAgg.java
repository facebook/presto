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
package com.facebook.presto.plugin.geospatial.aggregation;

import com.esri.core.geometry.ogc.OGCGeometry;
import com.facebook.presto.geospatial.serde.GeometrySerde;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import io.airlift.slice.Slice;

import static com.facebook.presto.plugin.geospatial.GeometryType.GEOMETRY;
import static com.facebook.presto.plugin.geospatial.GeometryType.GEOMETRY_TYPE_NAME;

/**
 * Aggregate form of ST_ConvexHull, which takes a set of geometries and computes the convex hull
 * of all the geometries in the set. The output is a single geometry.
 */
@Description("Returns a geometry that is the convex hull of all the geometries in the set.")
@AggregationFunction("convex_hull_agg")
public class ConvexHullAgg
{
    private ConvexHullAgg() {}

    @InputFunction
    public static void input(@AggregationState GeometryState state, @SqlType(GEOMETRY_TYPE_NAME) Slice input)
    {
        OGCGeometry geometry = GeometrySerde.deserialize(input);
        if (state.getGeometry() == null) {
            state.setGeometry(geometry.convexHull());
        } else if (!geometry.isEmpty()) {
            state.setGeometry(state.getGeometry().union(geometry).convexHull());
        }
    }

    @CombineFunction
    public static void combine(@AggregationState GeometryState state, @AggregationState GeometryState otherState)
    {
        if (state.getGeometry() == null) {
            state.setGeometry(otherState.getGeometry());
        } else if (otherState.getGeometry() != null && !otherState.getGeometry().isEmpty()) {
            state.setGeometry(state.getGeometry().union(otherState.getGeometry()).convexHull());
        }
    }

    @OutputFunction(GEOMETRY_TYPE_NAME)
    public static void output(@AggregationState GeometryState state, BlockBuilder out)
    {
        if (state.getGeometry() == null) {
            out.appendNull();
        } else {
            GEOMETRY.writeSlice(out, GeometrySerde.serialize(state.getGeometry()));
        }
    }
}
