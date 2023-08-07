package org.example;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@AllArgsConstructor
@NoArgsConstructor
public class ShardingKeyTuple {
    public static final String HOOK_SHARDING_KEY = "sharding_key";
    public static final String HOOK_COUNT = "count";
    public static final String HOOK_SUM = "sum";
    public static final String HOOK_RN = "rn";

    private String shardingKey;
    private Long count;
    private Long sum;
    private Long rn;
}
