package org.example;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class Main {
    private static final ObjectMapper JSON_OBJECT_MAPPER = new JsonMapper();
    private static final ObjectMapper YAML_OBJECT_MAPPER = new YAMLMapper();

    public static void main(String[] args) throws Exception {
        log.info("执行开始");

        val config = getShardingConfig(args);

        val tuples = getShardingKeys(config.getDatasource(), config.getGetShardingKeysSql());

        execBeforeAllSql(config.getDatasource(), config.getBeforeAllSql());

        for (val tuple : tuples) {
            execShardingSql(config.getDatasource(), config.getShardingSqlHook(), tuple);
        }

        log.info("执行结束");
    }


    private static ShardingConfig getShardingConfig(String[] args) throws IOException {
        try (
            val is = args.length == 1 ? Files.newInputStream(Paths.get(args[0])) : ClassLoader.getSystemResourceAsStream("config.yaml")
        ) {
            val config = YAML_OBJECT_MAPPER.readValue(is, new TypeReference<ShardingConfig>() {});
            config.normalize();
            return config;
        }
    }


    private static List<ShardingKeyTuple> getShardingKeys(Datasource datasource, String getShardingKeysSql) throws Exception {
        log.info("[获取分片键]执行开始");
        val result = new ArrayList<ShardingKeyTuple>();
        try (
            val c = JdbcUtil.getConnection(datasource);
            val s = c.createStatement();
            val rs = s.executeQuery(getShardingKeysSql);
        ) {
            while (rs.next()) {
                val shardingKey = JdbcUtil.coalease(rs.getString(ShardingKeyTuple.HOOK_SHARDING_KEY.toLowerCase()), rs.getString(ShardingKeyTuple.HOOK_SHARDING_KEY.toUpperCase()));
                val count = JdbcUtil.coalease(rs.getLong(ShardingKeyTuple.HOOK_COUNT.toLowerCase()), rs.getLong(ShardingKeyTuple.HOOK_COUNT.toLowerCase()));
                val sum = JdbcUtil.coalease(rs.getLong(ShardingKeyTuple.HOOK_SUM.toLowerCase()), rs.getLong(ShardingKeyTuple.HOOK_SUM.toLowerCase()));
                val rn = JdbcUtil.coalease(rs.getLong(ShardingKeyTuple.HOOK_RN.toLowerCase()), rs.getLong(ShardingKeyTuple.HOOK_RN.toLowerCase()));
                result.add(new ShardingKeyTuple(shardingKey, count, sum, rn));
            }
        }
        val json = JSON_OBJECT_MAPPER.writeValueAsString(result);
        log.info("[获取分片键]执行结束 shardingKeys={}", json);
        return result;
    }


    private static void execBeforeAllSql(Datasource datasource, String beforeSql) throws Exception {
        log.info("[执行前置语句]执行开始 beforeSql={}", beforeSql.replace("\n", " "));
        try (
            val c = JdbcUtil.getConnection(datasource);
            val s = c.createStatement();
        ) {
            s.execute(beforeSql);
        }
        log.info("[执行前置语句]执行结束");
    }

    private static void execShardingSql(Datasource datasource, String shardingSqlHook, ShardingKeyTuple tuple) throws Exception {
        val shardingSql = shardingSqlHook
            .replace("${" + ShardingKeyTuple.HOOK_SHARDING_KEY + "}", tuple.getShardingKey())
            .replace("${" + ShardingKeyTuple.HOOK_COUNT + "}", tuple.getCount().toString())
            .replace("${" + ShardingKeyTuple.HOOK_SUM + "}", tuple.getSum().toString())
            .replace("${" + ShardingKeyTuple.HOOK_RN + "}", tuple.getRn().toString());
        val json = JSON_OBJECT_MAPPER.writeValueAsString(tuple);
        log.info("[执行分片语句]执行开始 tuple={}", json);
        long cost;
        try (
            val c = JdbcUtil.getConnection(datasource);
            val s = c.createStatement();
        ) {
            val start = System.currentTimeMillis();
            s.execute(shardingSql);
            val end = System.currentTimeMillis();
            cost = (end - start) / 1000;
        }
        log.info("[执行分片语句]执行结束 tuple={} cost={}", json, cost);
    }
}
