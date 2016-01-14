package org.apache.flume;

import org.apache.flume.instrumentation.MonitorService;
import org.apache.flume.instrumentation.util.JMXPollUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by zhangzhikuan on 16/1/13.
 */
public class RedisMonitorServer implements MonitorService {

    private static final Logger logger = LoggerFactory.getLogger(RedisMonitorServer.class);

    //redis ip地址
    private static final String CONFIG_IP = "ip";
    //redis端口号
    private static final String CONFIG_PORT = "port";
    //redis的db号
    private static final String CONFIG_DB = "db";

    //redis ip地址
    private String ip;
    //redis端口号
    private int port;
    //redis的db号
    private int db;


    //redis客户端
    private Jedis redis = null;

    //定时任务
    private ScheduledExecutorService ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    //负责收集redis的metris信息
    protected final RedisMonitorServer.RedisCollector collectorRunnable = new RedisMonitorServer.RedisCollector();

    //时间戳
    private static DateFormat df = new SimpleDateFormat("yyyyMMddHHmm");


    //保存历史数据
    private Map<String, Long> valMap = null;

    public void start() {
        //创建redis客户端
        redis = new Jedis(ip, port);
        //选择redis的db
        redis.select(this.db);

        //判断当前server是否已经停止了,如果停止了,重新新建一个
        if (this.ScheduledExecutorService.isShutdown() || this.ScheduledExecutorService.isTerminated()) {
            this.ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        }

        //开始调度,每一分钟调度一次.
        this.ScheduledExecutorService.scheduleWithFixedDelay(this.collectorRunnable, 10L, 60L, TimeUnit.SECONDS);

        //保存历史数据,每次重启,都需要清零的
        if (valMap == null) {
            valMap = new ConcurrentHashMap<String, Long>();
        }
    }

    public void stop() {
        //判断redis是否为空,如果不为空,则关闭
        if (redis != null) {
            redis.close();
        }

        //将保存的数据清理
        valMap.clear();

        //将调度器关闭
        this.ScheduledExecutorService.shutdown();

        //等待调度器是否已经关闭.
        while (!this.ScheduledExecutorService.isTerminated()) {
            try {
                logger.warn("Waiting for Redis service to stop");
                this.ScheduledExecutorService.awaitTermination(500L, TimeUnit.MILLISECONDS);
            } catch (InterruptedException var2) {
                logger.warn("Interrupted while waiting for Redis monitor to shutdown", var2);
                this.ScheduledExecutorService.shutdownNow();
            }
        }
    }

    //读取配置文件
    public void configure(Context context) {
        this.ip = context.getString(CONFIG_IP, "127.0.0.1");
        this.port = context.getInteger(CONFIG_PORT, 6379);
        this.db = context.getInteger(CONFIG_DB, 0);
    }

    //真正干活的
    protected class RedisCollector implements Runnable {


        protected RedisCollector() {
        }

        public void run() {
            try {
                //获取所有的信息
                Map t = JMXPollUtil.getAllMBeans();
                //迭代器
                Iterator i$ = t.keySet().iterator();

                Date currentDate = new Date();

                while (i$.hasNext()) {
                    //获取组件
                    String component = (String) i$.next();
                    //组件也是一个map
                    Map attributeMap = (Map) t.get(component);

                    //进行迭代
                    for (Iterator i$1 = attributeMap.keySet().iterator(); i$1.hasNext(); ) {

                        //属性,组件+属性=>value
                        String attribute = (String) i$1.next();
                        //key
                        String key = "flume." + component + "." + attribute;


                        try {
                            //value值
                            Long newValue = Long.parseLong(attributeMap.get(attribute).toString());
                            //获取原有的值
                            Long oldValue = valMap.get(key);
                            if (oldValue == null) {
                                oldValue = 0L;
                            }


                            //redis key
                            String timeKey = key + "::" + df.format(currentDate);

                            //发送给redis
                            redis.set(key, String.valueOf(newValue - oldValue));
                            //设置新值
                            valMap.put(key, newValue);

                        } catch (Exception e) {
                            RedisMonitorServer.logger.warn("[" + key + "]对应的值为[" + attributeMap.get(attribute).toString() + "]");
                            continue;
                        }
                    }
                }
            } catch (Throwable t) {
                RedisMonitorServer.logger.error("Unexpected error", t);
            }

        }
    }
}
