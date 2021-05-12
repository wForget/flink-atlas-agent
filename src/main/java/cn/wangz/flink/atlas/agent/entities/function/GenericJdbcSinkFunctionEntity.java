package cn.wangz.flink.atlas.agent.entities.function;


import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.internal.AbstractJdbcOutputFormat;
import org.apache.flink.connector.jdbc.internal.GenericJdbcSinkFunction;
import org.apache.flink.connector.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;

import cn.wangz.flink.atlas.agent.entities.NodeEntity;
import cn.wangz.flink.atlas.agent.utils.ClassUtils;

public class GenericJdbcSinkFunctionEntity extends NodeEntity<GenericJdbcSinkFunction> {

    public GenericJdbcSinkFunctionEntity(Object node) {
        super(node);
    }

    @Override
    public AtlasEntity toEntity() {
        GenericJdbcSinkFunction jdbcSinkFunction = this.node;

        try {

            AbstractJdbcOutputFormat outputFormat = ClassUtils.getFiledValue(jdbcSinkFunction,
                    "outputFormat", AbstractJdbcOutputFormat.class);

            JdbcDmlOptions dmlOptions = null;
            try {
                dmlOptions = ClassUtils.getFiledValue(outputFormat, "dmlOptions", JdbcDmlOptions.class);
            } catch (Throwable t) {
                // do noting
            }

            JdbcConnectionProvider connectionProvider = ClassUtils.getFiledValue(outputFormat,
                    "connectionProvider", JdbcConnectionProvider.class);

            JdbcConnectionOptions jdbcOptions = null;
            if (connectionProvider instanceof SimpleJdbcConnectionProvider) {
                jdbcOptions = ClassUtils.getFiledValue(connectionProvider, "jdbcOptions", JdbcConnectionOptions.class);
            }

            // TODO

        } catch (Throwable t) {
            // TODO
        }

        return null;
    }
}
