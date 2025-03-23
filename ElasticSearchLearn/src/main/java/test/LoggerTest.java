package test;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author: gj
 * @description: TODO
 */
public class LoggerTest {
    private static final Logger logger = LogManager.getLogger("test");

    public static void main(String[] args) {
        logger.info("日志测试2");
    }
}
