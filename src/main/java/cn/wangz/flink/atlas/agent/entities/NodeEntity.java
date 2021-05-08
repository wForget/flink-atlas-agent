package cn.wangz.flink.atlas.agent.entities;

import org.apache.atlas.model.instance.AtlasEntity;

public abstract class NodeEntity<T> {

    protected T node;

    public NodeEntity(Object node) {
        this.node = (T) node;
    }

    public abstract AtlasEntity toEntity();

}
