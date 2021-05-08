package cn.wangz.flink.atlas.agent.entities.function;

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.flink.connector.jdbc.internal.GenericJdbcSinkFunction;

import cn.wangz.flink.atlas.agent.entities.NodeEntity;

public class GenericJdbcSinkFunctionEntity extends NodeEntity<GenericJdbcSinkFunction> {

    public GenericJdbcSinkFunctionEntity(Object node) {
        super(node);
    }

    @Override
    public AtlasEntity toEntity() {
        return null;
    }
}
