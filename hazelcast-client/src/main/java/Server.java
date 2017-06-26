import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

/**
 * Created by alarmnummer on 26-6-17.
 */
public class Server {
    public static void main(String[] args){
        Config config = new Config();
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true).addMember("10.212.1.116").addMember("10.212.1.117");
        HazelcastInstance hz= Hazelcast.newHazelcastInstance(config);
    }
}
