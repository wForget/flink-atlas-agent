package cn.wangz.flink.atlas.agent.entities;

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;

import cn.wangz.flink.atlas.agent.entities.function.FlinkKafkaConsumerEntity;

public class StreamNodeEntity extends NodeEntity<StreamNode> {

    public StreamNodeEntity(StreamNode streamNode) {
        super(streamNode);
    }

    @Override
    public AtlasEntity toEntity() {
        StreamOperator<?> operator = node.getOperator();
        if (operator instanceof AbstractUdfStreamOperator) {
            Function userFunction = ((AbstractUdfStreamOperator) operator).getUserFunction();
            String userFunctionClass = userFunction.getClass().getName();
            switch (userFunctionClass) {
                case FLINK_KAFKA_CONSUMER_CLASS:
                    return new FlinkKafkaConsumerEntity(userFunction).toEntity();
                case GENERIC_JDBC_SINK_FUNCTION_CLASS:
                    break;
                default:
                    return null;
            }
        }
        return null;
    }

    private static final String FLINK_KAFKA_CONSUMER_CLASS = "org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer";
    private static final String GENERIC_JDBC_SINK_FUNCTION_CLASS = "org.apache.flink.connector.jdbc.internal.GenericJdbcSinkFunction";

}
