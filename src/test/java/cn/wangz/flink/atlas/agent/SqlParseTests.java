package cn.wangz.flink.atlas.agent;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;
import org.apache.flink.table.planner.calcite.CalciteParser;
import org.junit.Test;

public class SqlParseTests {

    @Test
    public void insertIntoTest() {
        SqlParser.Config config = SqlParser.configBuilder().setParserFactory(FlinkSqlParserImpl.FACTORY)
                .setConformance(FlinkSqlConformance.DEFAULT).setLex(Lex.JAVA).build();

        CalciteParser parser = new CalciteParser(config);

        String sql = "insert into `test`.`user` SELECT name, age FROM users as a";

        SqlNode sqlNode = parser.parse(sql);

        System.out.println(sqlNode);
    }

}
