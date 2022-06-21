package org.darkphoenixs.hbase.repair.domain;

import lombok.Data;
import org.apache.hadoop.hbase.TableName;


@Data
public class MateRow {
    String tableName;
    String startKey;
    String endKey;
    String rowStr;
    String regionName;
    String encodedName;
    String sn;
    byte[]  startKeyByte;
    byte[]  endKeyByte;
    TableName tablenameDto;

}
