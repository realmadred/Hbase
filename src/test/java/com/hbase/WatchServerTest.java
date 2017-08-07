package com.hbase;

import org.springframework.core.io.support.PropertiesLoaderUtils;

import java.nio.file.*;
import java.util.Objects;
import java.util.Properties;

public class WatchServerTest {
    public static void main(String[] args) throws Exception {
        WatchService watchService = FileSystems.getDefault().newWatchService();
        final Path path = Paths.get(WatchServerTest.class.getResource("/").toURI());
        path.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY);
        while (true) {
            final WatchKey wk = watchService.take();
            for (WatchEvent<?> event : wk.pollEvents()) {
                if (StandardWatchEventKinds.OVERFLOW.equals(event.kind())) continue;
                final Object context = event.context();
                if ("test.properties".equals(Objects.toString(context))) {
                    final Properties properties = PropertiesLoaderUtils.loadAllProperties("test.properties");
                    System.out.println(properties.getProperty("aaa"));
                }
            }

            // reset the key
            boolean valid = wk.reset();
            if (!valid) {
                System.out.println("Key has been unregisterede");
                break;
            }
        }
        watchService.close();
    }
}
