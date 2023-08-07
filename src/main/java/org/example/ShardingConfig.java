package org.example;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import java.util.Map;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ShardingConfig {
    private Map<String, Datasource> datasources;
    private String getShardingKeysSql;
    private String beforeAllSql;
    private String shardingSqlHook;
    private String datasourceCode;

    private Datasource datasource;

    public void normalize() {
        datasource = datasources.get(datasourceCode);
    }
}
