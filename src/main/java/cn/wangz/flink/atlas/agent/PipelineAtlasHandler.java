package cn.wangz.flink.atlas.agent;

import java.util.stream.Collectors;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.streaming.api.graph.StreamGraph;

import cn.wangz.flink.atlas.agent.entities.StreamNodeEntity;

public class PipelineAtlasHandler {

    public static void handle(Pipeline pipeline) {
        if (pipeline instanceof StreamGraph) {
            handle((StreamGraph) pipeline);
        } else if (pipeline instanceof Plan) {
            handle((Plan) pipeline);
        }

    }

    private static void handle(StreamGraph streamGraph) {
        // TODO

        // flink application entity

        // source entities
        streamGraph.getSourceIDs()
                .stream()
                .map(id -> streamGraph.getStreamNode(id))
                .map(node -> new StreamNodeEntity(node).toEntity())
                .collect(Collectors.toList());

        // sink entities
        streamGraph.getSinkIDs()
                .stream()
                .map(id -> streamGraph.getStreamNode(id))
                .map(node -> new StreamNodeEntity(node).toEntity())
                .collect(Collectors.toList());
    }

    private static void handle(Plan plan) {
        // TODO
    }


}
