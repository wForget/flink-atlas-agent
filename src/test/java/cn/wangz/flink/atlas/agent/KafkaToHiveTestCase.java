package cn.wangz.flink.atlas.agent;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.*;

import java.util.Properties;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaDeserializationSchemaWrapper;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.annotations.HiveRunnerSetup;
import com.klarna.hiverunner.annotations.HiveSQL;
import com.klarna.hiverunner.config.HiveRunnerConfig;

@RunWith(StandaloneHiveRunner.class)
public class KafkaToHiveTestCase {

    @HiveSQL(files = {})
    private HiveShell shell;

    @HiveRunnerSetup
    private static final HiveRunnerConfig CONFIG =
            new HiveRunnerConfig() {
                {
                    if (HiveShimLoader.getHiveVersion().startsWith("3.")) {
                        // hive-3.x requires a proper txn manager to create ACID table
                        getHiveConfSystemOverride()
                                .put(HIVE_TXN_MANAGER.varname, DbTxnManager.class.getName());
                        getHiveConfSystemOverride().put(HIVE_SUPPORT_CONCURRENCY.varname, "true");
                        // tell TxnHandler to prepare txn DB
                        getHiveConfSystemOverride().put(HIVE_IN_TEST.varname, "true");
                    }
                }
            };

    @Before
    public void setupSourceDatabase() {
        shell.execute("CREATE DATABASE test");
        shell.execute(new StringBuilder()
                .append("CREATE TABLE `test`.`user` (")
                .append("name STRING, age INT")
                .append(")")
                .toString());
    }

    @Test
    public void kafkaToHiveTest() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final KafkaDeserializationSchema<User> userSchema =
                new KafkaDeserializationSchemaWrapper<>(
                        new TypeInformationSerializationSchema<>(
                                TypeInformation.of(User.class), new ExecutionConfig()));

        // kafka source
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");
        DataStream<User> dataStream = env
                .addSource(new FlinkKafkaConsumer<>("user", userSchema, properties));

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        //构造hive catalog
        HiveConf hiveConf = shell.getHiveConf();
        HiveCatalog hive = HiveTestUtils.createHiveCatalog(hiveConf);
        tEnv.registerCatalog("hive", hive);
        tEnv.useCatalog("hive");
        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tEnv.useDatabase("test");

        tEnv.createTemporaryView("users", dataStream);

        String insertSql = "insert into `test`.`user` SELECT name, age FROM users";
        tEnv.executeSql(insertSql);
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
