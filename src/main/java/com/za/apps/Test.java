package com.za.apps;

import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.Traversal;




public class Test {

    private static enum RelTypes implements RelationshipType {
        NEO_NODE,
        KNOWS,
        CODED_BY
    }

    public static void main(String[] args) {
        PathFinder<WeightedPath> finder = GraphAlgoFactory.dijkstra(
                (PathExpander) Traversal.expanderForTypes( RelTypes.KNOWS, Direction.BOTH ), "cost" );

      //  WeightedPath path = finder.findSinglePath( nodeA, nodeB );

// Get the weight for the found path
       // path.weight();
    }
}
