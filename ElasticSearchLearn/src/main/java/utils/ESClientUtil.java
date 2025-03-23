package utils;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;

/**
 * @author: gj
 * @description: TODO
 */
public class ESClientUtil {
    private static volatile RestHighLevelClient client = null;

    private ESClientUtil() {
    }

    static {
        // 注册一个 JVM 关闭时的钩子
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (client != null) {
                System.out.println("JVM 关闭，释放资源...");
                try {
                    client.close();
                } catch (IOException e) {
                    System.out.println("关闭失败：" + e.getMessage());
                }
            }
        }));
    }

    public static RestHighLevelClient getClient() {
        if (client == null) {
            synchronized (ESClientUtil.class) {
                if (client == null) {
                    client = new RestHighLevelClient(
                            RestClient.builder(new HttpHost("localhost", 9200, "http"))
                                    .setRequestConfigCallback(requestConfigBuilder ->
                                            requestConfigBuilder
                                                    .setConnectTimeout(5000)  // 连接超时（默认1秒）
                                                    .setSocketTimeout(60000)  // 读取超时（默认30秒）
                                    )
                                    .setHttpClientConfigCallback(httpClientBuilder ->
                                            httpClientBuilder.setMaxConnTotal(100)  // 最大连接数
                                                    .setMaxConnPerRoute(10)  // 每个主机的最大连接数
                                    )
                    );
                }
            }
        }
        return client;
    }

    public static void closeClient() throws IOException {
        if (client != null) {
            client.close();
        }
    }

}
