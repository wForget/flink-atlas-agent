package cn.wangz.flink.atlas.agent.entities.function;

import java.util.Properties;

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import cn.wangz.flink.atlas.agent.entities.NodeEntity;
import cn.wangz.flink.atlas.agent.utils.ClassUtils;

public class FlinkKafkaProducerEntity extends NodeEntity<FlinkKafkaProducer> {

    public FlinkKafkaProducerEntity(Object node) {
        super(node);
    }

    @Override
    public AtlasEntity toEntity() {
        FlinkKafkaProducer flinkKafkaProducer = this.node;
        try {
            Properties kafkaProperties = ClassUtils.getFiledValue(flinkKafkaProducer, "producerConfig", Properties.class);
            String defaultTopicId = ClassUtils.getFiledValue(flinkKafkaProducer, "defaultTopicId", String.class);

            // TODO

        } catch (Throwable t) {
            // TODO
        }
        return null;
    }

}
