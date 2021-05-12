package cn.wangz.flink.atlas.agent;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;

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
        List<AtlasEntity> sourceEntities = streamGraph.getSourceIDs().stream().map(id -> streamGraph.getStreamNode(id))
                .map(node -> new StreamNodeEntity(node).toEntity()).filter(entity -> entity != null)
                .collect(Collectors.toList());

        // sink entities
        List<AtlasEntity> sinkEntities = streamGraph.getSinkIDs()
                .stream()
                .map(id -> {
                    StreamNode streamNode = streamGraph.getStreamNode(id);
                    StreamOperator<?> operator = streamNode.getOperator();
                    if (operator instanceof AbstractUdfStreamOperator) {
                        Function userFunction = ((AbstractUdfStreamOperator) operator).getUserFunction();
                        // hive or file writer, end of DiscardingSink
                        if (userFunction instanceof DiscardingSink) {
                            int sourceId = streamNode.getInEdges().get(0).getSourceId();
                            StreamNode sourceNode = streamGraph.getStreamNode(sourceId);
                            return sourceNode;
                        }
                    }
                    return streamNode;
                })
                .map(node -> new StreamNodeEntity(node).toEntity())
                .filter(entity -> entity != null)
                .collect(Collectors.toList());

    }

    private static void handle(Plan plan) {
        // TODO
    }


}
