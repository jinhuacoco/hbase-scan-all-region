package org.darkphoenixs.hbase.repair;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.darkphoenixs.hbase.repair.domain.MateRow;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.security.AlgorithmConstraints;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@SpringBootApplication
public class HbaseScanAllRegion {


    public static void main(String[] args) throws Exception {
        HbaseScanAllRegion hbaseScanAllRegion = new HbaseScanAllRegion();
        hbaseScanAllRegion.run();
    }


    private String zookeeperNodeParent = "";
    private String zookeeperAddress = "";
    private String onlyTableName = "";
    Set<String> exclusiveTableSet = Arrays.asList(new String[]{}).stream().collect(Collectors.toSet());
    boolean fastFail = false;//为true，则如果扫描失败一个表，停止扫描这个表的其它regin


    public void run() throws Exception {
        System.out.println("");
        System.out.println("");
        System.out.println("");
        System.out.println("");
        System.out.println("");
        System.out.println("");
        System.out.println("");
        System.out.println("");
        System.out.println("先sleep下==================================");
        System.out.println("");
        System.out.println("");
        System.out.println("");
        System.out.println("");
        Thread.sleep(500);
        Thread currentThread = Thread.currentThread();
        Thread HeartBateThread = new Thread(new HeartBateRunable(currentThread));
        HeartBateThread.start();
        long l1 = System.currentTimeMillis();
        AtomicInteger atomicInteger = new AtomicInteger(0);
        try {
            //读取配置文件
            String userdir = System.getProperty("user.dir");
            System.out.println("user.dir:" + userdir);
            String path = System.getProperty("user.dir") + File.separator + "hbase-scan-all-region.properties";
            System.out.println("即将读取配置文件：path:" + path);
            Properties properties = new Properties();
            InputStream in = null;
            String dev = System.getProperties().getProperty("dev");
            if ("true".equals(dev)) {
                ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
                in = contextClassLoader.getResourceAsStream("hbase-scan-all-region.properties");
            } else {
                File file = new File(path);
                in = new FileInputStream(file);
            }
            properties.load(in);
            System.out.println("================正在获取配置文件值=================");
            zookeeperNodeParent = properties.getProperty("zookeeper.nodeParent");
            zookeeperAddress = properties.getProperty("zookeeper.address");
            onlyTableName = properties.getProperty("onlyTableName");
            String exclusiveTableSetArr = properties.getProperty("exclusiveTableSetArr");
            String threadnumStr = properties.getProperty("threadnum");
            //校验参数
            if (exclusiveTableSetArr != null) {
                String[] split = exclusiveTableSetArr.split(",");
                exclusiveTableSet = new HashSet<String>();
                exclusiveTableSet.addAll(Arrays.asList(split));
            }
            if (StringUtils.isEmpty(zookeeperNodeParent) || StringUtils.isEmpty(zookeeperAddress)) {
                System.err.println("zookeeperNodeParent和zookeeperAddress不能为空");
                return;
            }
            if (threadnumStr == null || "".endsWith(threadnumStr)) {
                threadnumStr = "1";
            }
            int threadnum = 1;
            try {
                threadnum = Integer.parseInt(threadnumStr);
            } catch (Exception e) {
                e.printStackTrace();
                System.err.println("threadnum填写错误");
                return;
            }
            if (threadnum < 1 || threadnum > 30) {
                System.err.println("threadnum太大或者太小");
                return;
            }
            //打印参数
            System.out.println("zookeeperAddress:" + zookeeperAddress);
            System.out.println("zookeeperNodeParent:" + zookeeperNodeParent);
            System.out.println("onlyTableName:" + onlyTableName);
            System.out.println("exclusiveTableSet:" + JSON.toJSONString(exclusiveTableSet));
            System.out.println("threadnum:" + threadnum);


            //获取连接（仅仅用于扫描元数据表）
            String HBASE_QUORUM = "hbase.zookeeper.quorum";
            String HBASE_ZNODE_PARENT = "zookeeper.znode.parent";
            String RETRIES_NUMBER = "hbase.client.retries.number";
            Configuration configuration = HBaseConfiguration.create();
            configuration.set(HBASE_QUORUM, zookeeperAddress);
            configuration.set(HBASE_ZNODE_PARENT, this.zookeeperNodeParent);
            configuration.set(RETRIES_NUMBER, "1");
            Connection conn = ConnectionFactory.createConnection(configuration);
            //定义线程池
            ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(threadnum, threadnum, 60, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<>(), new ThreadPoolExecutor.CallerRunsPolicy());
            //获取hbase:meta表的数据
            System.out.println("================获取hbase:meta表的数据=================");
            ArrayList<MateRow> metaRegions = getMetaRegions(conn);
            Map<String, List<MateRow>> tableMap = metaRegions.stream().collect(Collectors.groupingBy(MateRow::getTableName));
            //定义汇总时需要使用到的变量
            List<String> regionNameFailArr = Collections.synchronizedList(new ArrayList<>());
            List<String> encodeNameFailArr = Collections.synchronizedList(new ArrayList<>());
            final AtomicInteger requesrNum = new AtomicInteger(0);
            Set<String> tableScaned = Collections.synchronizedSet(new HashSet<>());
            Set<String> tableScaneFail = Collections.synchronizedSet(new HashSet<>());

            System.out.println();
            System.out.println();
            System.out.println();
            System.out.println();
            System.out.println();
            System.out.println();

            //开始多线程扫描
            //Collections.shuffle(metaRegions);
            System.out.println("================开始多线程扫描=================");
            try {
                for (MateRow mate : metaRegions) {
                    MetaScanRunable runable = new MetaScanRunable(mate, requesrNum, tableScaneFail, regionNameFailArr, encodeNameFailArr, tableScaned, conn,metaRegions.size(),configuration);
                    threadPoolExecutor.submit(runable);
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
            try{
                System.out.println("====准备关闭线程池===");
                threadPoolExecutor.shutdown();
                threadPoolExecutor.awaitTermination(1, TimeUnit.DAYS);
                System.out.println("====准备关闭所有连接===");
                Collection<Connection> conArr = MetaScanRunable.poolMap.values();
                for(Connection con : conArr){
                    con.close();
                }
                conn.close();
                Thread.sleep(500);
            }catch (Exception e){
                e.printStackTrace();
            }
            //扫描完毕，打印汇总参数
            System.out.println("");
            System.out.println("");
            System.out.println("");
            System.out.println("");
            System.out.println("");
            System.out.println("");
            System.out.println("=================================");
            System.out.println("程序正常结束,requesrNum:" + requesrNum + ",metaRegions.size:" + metaRegions.size());
            System.out.println("已经扫描过的表：tableScaned : " + tableScaned);
            System.out.println("扫描失败的表：tableScaneFail : " + tableScaneFail);
            System.out.println("扫描失败的表的个数tableScaneFail.size() : " + tableScaneFail.size());
            System.out.println("扫描失败的region个数：regionNameFailArr.size() : " + regionNameFailArr.size());
            System.out.println("=========扫描失败的region全称集合 regionNameFailArr begin ========");
            if (regionNameFailArr != null) {
                for (String region : regionNameFailArr) {
                    System.out.println(region);
                }
            } else {
                System.out.println("无");
            }
            System.out.println("=========扫描失败的region全称集合 regionNameFailArr end ========");
            System.out.println("=========扫描失败的region简称集合 encodeNameFailArr begin ========");
            if (encodeNameFailArr != null) {
                for (String encodeName : encodeNameFailArr) {
                    System.out.println(encodeName);
                }
            } else {
                System.out.println("无");
            }
            System.out.println("=========扫描失败的region简称集合 encodeNameFailArr end ========");

        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("程序出现了异常，停止运行了");
        }
        long l2 = System.currentTimeMillis();
        System.out.println("程序耗时：" + ((l2 - l1) / 1000) + "秒");

    }


    public ArrayList<MateRow> getMetaRegions(Connection conn) throws Exception {

        Table table = conn.getTable(TableName.valueOf("hbase:meta"));
        Scan scan = new Scan();
        if (onlyTableName != null && onlyTableName != "") {
            //此处需要把表状态的数据也找出来
            PrefixFilter filter = new PrefixFilter(Bytes.toBytes(onlyTableName));
            scan.setFilter(filter);
        }
        scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("regioninfo"));
        Iterator<Result> iterator = table.getScanner(scan).iterator();


        ArrayList<MateRow> mateRowArr = new ArrayList<>();
        while (iterator.hasNext()) {
            Result result = iterator.next();
            String Row = Bytes.toStringBinary(result.getRow());
            String sn = Bytes.toStringBinary(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("regioninfo")));
            RegionLocations regionLocations = MetaTableAccessor.getRegionLocations(result);
            HRegionLocation[] regionLocationsArr = regionLocations.getRegionLocations();
            for (int i = 0; i < regionLocationsArr.length; ++i) {
                HRegionLocation hRegionLocation = regionLocationsArr[i];
                RegionInfo region = hRegionLocation.getRegion();
                byte[] startKey = region.getStartKey();
                byte[] endKey = region.getEndKey();
                String regionNameAsString = region.getRegionNameAsString();
                TableName tableNameDto = region.getTable();
                String encodedName = region.getEncodedName();
                byte[] tableNameByte = tableNameDto.getName();
                String startKeyStr = Bytes.toStringBinary(startKey);
                String endKeyStr = Bytes.toStringBinary(endKey);
                String tableNameStr = Bytes.toStringBinary(tableNameByte);
                if (!StringUtils.isBlank(onlyTableName)) {
                    if (!onlyTableName.equals(tableNameStr)) {
                        continue;
                    }
                }
                if (!exclusiveTableSet.isEmpty()) {
                    if (exclusiveTableSet.contains(tableNameStr)) {
                        System.out.println("排除表" + tableNameStr);
                        continue;
                    }
                }
                MateRow mateRow = new MateRow();
                mateRow.setStartKey(startKeyStr);
                mateRow.setEndKey(endKeyStr);
                mateRow.setTableName(tableNameStr);
                mateRow.setSn(sn);
                mateRow.setRowStr(Row);
                mateRow.setRegionName(regionNameAsString);
                mateRow.setStartKeyByte(startKey);
                mateRow.setEndKeyByte(endKey);
                mateRow.setTablenameDto(tableNameDto);
                mateRow.setEncodedName(encodedName);
                mateRowArr.add(mateRow);
            }
        }
        return mateRowArr;
    }
}
