package com.folkol;

import com.couchbase.client.dcp.Client;
import com.couchbase.client.dcp.message.DcpFailoverLogResponse;
import com.couchbase.client.deps.io.netty.util.ReferenceCounted;
import kafka.utils.ZKStringSerializer$;
import org.I0Itec.zkclient.ZkClient;

public class ZkFailoverLogLogger
{
    private static ZkClient zkClient;
    private static String cbHost;
    private static String zkHost;
    private static String bucket;
    private static String passwd;

    public static void main(String[] args) throws Exception
    {
        if (args.length != 4) {
            System.out.println("usage: java ZkFailoverLogLogger cbHost zkHost bucketname bucketpasswd");
            System.exit(1);
        }

        cbHost = args[0];
        zkHost = args[1];
        bucket = args[2];
        passwd = args[3];

        zkClient = new ZkClient(zkHost, 4000, 6000, ZKStringSerializer$.MODULE$);

        final Client client = Client.configure()
                                    .hostnames(cbHost)
                                    .bucket(bucket)
                                    .password(passwd)
                                    .build();
        client.controlEventHandler(ReferenceCounted::release);
        client.dataEventHandler(event -> System.out.println("Got DCP event: " + event));
        client.connect().await();

        client.failoverLogs()
              .toBlocking()
              .forEach(buffer -> {
                  if (DcpFailoverLogResponse.is(buffer)) {
                      short partition = DcpFailoverLogResponse.vbucket(buffer);
                      int numEntries = DcpFailoverLogResponse.numLogEntries(buffer);
                      for (int i = 0; i < numEntries; i++) {
                          long vid = DcpFailoverLogResponse.vbuuidEntry(buffer, 0);
                          long seqno = DcpFailoverLogResponse.seqnoEntry(buffer, 0);
                          System.out.printf("%d\t%d\t%d\t%d%n", partition, i, vid, seqno);
                      }
                  } else {
                      System.err.println("Expected DcpFailoverLog, got: " + buffer);
                  }
              });
        client.disconnect().await();
        zkClient.close();
    }
}
