package cn.wangz.flink.atlas.agent;

import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
public class KafkaToJDBC {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // kafka source
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");
        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer<>("topic", new SimpleStringSchema(), properties));

        // jdbc sink
        SinkFunction sink = JdbcSink.<Book>sink(
                "insert into books (id, title, authors, year) values (?, ?, ?, ?)",
                (statement, book) -> {
                    statement.setLong(1, book.id);
                    statement.setString(2, book.title);
                    statement.setString(3, book.authors);
                    statement.setInt(4, book.year);
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(200)
                        .withMaxRetries(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://localhost:3306/test")
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUsername("username")
                        .withPassword("password")
                        .build()
        );

        stream.map(s -> new Book(s))
                .addSink(sink);

        env.execute("test");
    }

    static class Book {

        public Book(String json) {

        }

        public Book(Long id, String title, String authors, Integer year) {
            this.id = id;
            this.title = title;
            this.authors = authors;
            this.year = year;
        }
        Long id = 0L;
        String title = "";
        String authors = "";
        Integer year = 0;
    }

}
