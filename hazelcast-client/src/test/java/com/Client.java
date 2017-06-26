package com;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.util.Date;

/**
 * Created by alarmnummer on 26-6-17.
 */
public class Client {

    public static void main(String[] args) throws Exception {
//        Process server1 = startServer("server1");
//        Process server2 = startServer("server2");
//        Process server3 = startServer("server3");
//
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setConnectionAttemptLimit(100);
        clientConfig.getNetworkConfig().addAddress("10.212.1.116").addAddress("10.212.1.117").addAddress("10.212.1.118");
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        IMap map = client.getMap("foo");

        System.out.println("Inserting data");
        for(int k=0;k<1000*1000;k++) {
            map.put(k, new byte[1000]);

            if(k%10000==0){
                System.out.println("    at:"+k);
            }
        }
        System.out.println("Finished inserting data");


        int l = 0;
        for (; ; ) {
            for (int k = 0; k < 30; k++) {
                Thread.sleep(500);
                Object put = map.get(l);
                System.out.println(new Date() + ":inserting:" + l);
                l++;
            }
//            kill(server1);
        }
    }

    private static void kill(Process process)throws Exception{
        int pid = pid(process);
        Runtime.getRuntime().exec("kill -9 "+pid);
    }

    private static int pid(Process p) throws Exception {
        Field f = p.getClass().getDeclaredField("pid");
        f.setAccessible(true);
        return f.getInt(p);
    }

    private static Process startServer(String name) throws IOException {
        String javaExecutable = System.getProperty("java.home") + "/bin/java";
        String classpath = System.getProperty("java.class.path");

        ProcessBuilder processBuilder = new ProcessBuilder(javaExecutable, "-cp", classpath, "com.hazelcast.core.server.StartServer");
    //    processBuilder.redirectErrorStream();
        Process process = processBuilder.start();
        new StreamGobbler(process.getInputStream(),name).start();
        new StreamGobbler(process.getErrorStream(),name).start();
        return process;
    }

    private static class StreamGobbler extends Thread {
        InputStream is;
        String type;

        private StreamGobbler(InputStream is, String type) {
            this.is = is;
            this.type = type;
        }

        @Override
        public void run() {
            try {
                System.out.println("foo");

                InputStreamReader isr = new InputStreamReader(is);
                BufferedReader br = new BufferedReader(isr);
                String line;
                while ((line = br.readLine()) != null)
                    System.out.println(type + "> " + line);
            }catch (Throwable e){
                e.printStackTrace();
            }
        }
    }
}
