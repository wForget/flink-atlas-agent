package cn.wangz.flink.atlas.agent.entities.function;

import java.util.Properties;

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicsDescriptor;

import cn.wangz.flink.atlas.agent.entities.NodeEntity;
import cn.wangz.flink.atlas.agent.utils.ClassUtils;

public class FlinkKafkaConsumerEntity extends NodeEntity<FlinkKafkaConsumer> {

    public FlinkKafkaConsumerEntity(Object node) {
        super(node);
    }

    @Override
    public AtlasEntity toEntity() {
        FlinkKafkaConsumer flinkKafkaConsumer = this.node;
        try {
            Properties kafkaProperties = ClassUtils.getFiledValue(flinkKafkaConsumer, "properties", Properties.class);
            KafkaTopicsDescriptor topicsDescriptor = ClassUtils.getFiledValue(flinkKafkaConsumer,
                    "topicsDescriptor", KafkaTopicsDescriptor.class);

            // TODO

        } catch (Throwable t) {
            // TODO
        }
        return null;
    }


}
