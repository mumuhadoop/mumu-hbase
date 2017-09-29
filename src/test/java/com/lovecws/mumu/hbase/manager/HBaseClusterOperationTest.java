package com.lovecws.mumu.hbase.manager;

import org.junit.Test;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 集群状态测试
 * @date 2017-09-28 16:31
 */
public class HBaseClusterOperationTest {

    private HBaseClusterOperation hBaseClusterOperation = new HBaseClusterOperation();

    @Test
    public void clusterStatus() {
        hBaseClusterOperation.clusterStatus();
    }
}
