package com.lovecws.mumu.hbase.basic;

import org.junit.Test;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: hbase扫描器
 * @date 2017-09-27 16:24
 */
public class HBaseScanerOperationTest {

    private HBaseScanerOperation baseScanerOperation = new HBaseScanerOperation();

    @Test
    public void scanner() {
        baseScanerOperation.scanner("babymm", null, 2);
    }
}
