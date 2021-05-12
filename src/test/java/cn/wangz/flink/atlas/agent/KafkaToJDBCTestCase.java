package cn.wangz.flink.atlas.agent;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaDeserializationSchemaWrapper;
import org.junit.Before;
import org.junit.Test;

public class KafkaToJDBCTestCase {

    private static final String databaseURL = "jdbc:derby:memory:test";

    @Before
    public void setup() {
        try (Connection conn = DriverManager.getConnection(databaseURL + ";create=true")) {
            Statement statement = conn.createStatement();
            String sql = "CREATE TABLE \"user\" (name varchar(64), age int )";
            statement.execute(sql);
        } catch (SQLException throwables) {
            if (throwables.getSQLState().equals("X0Y32")) {
                System.out.println("Talbe already exists.  No need to recreate");
            } else {
                throwables.printStackTrace();
            }
        }
    }

    @Test
    public void kafkaToJdbcTest() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final KafkaDeserializationSchema<KafkaToHiveTestCase.User> userSchema =
                new KafkaDeserializationSchemaWrapper<>(
                        new TypeInformationSerializationSchema<>(
                                TypeInformation.of(KafkaToHiveTestCase.User.class), new ExecutionConfig()));
        // kafka source
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");
        DataStream<KafkaToHiveTestCase.User> dataStream = env
                .addSource(new FlinkKafkaConsumer<>("user", userSchema, properties));

        // jdbc sink
        SinkFunction sink = JdbcSink.<User>sink(
                "insert into \"user\" (name, age) values (?, ?)",
                (statement, user) -> {
                    statement.setString(1, user.name);
                    statement.setInt(2, user.age);
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(200)
                        .withMaxRetries(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(databaseURL)
                        .withDriverName("org.apache.derby.jdbc.EmbeddedDriver")
                        .build()
        );

        dataStream.addSink(sink);

        env.execute("kafkaToJdbcTest");
    }

    public static class User {
        private String name;
        private Integer age;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Integer getAge() {
            return age;
        }

        public void setAge(Integer age) {
            this.age = age;
        }
    }

}
