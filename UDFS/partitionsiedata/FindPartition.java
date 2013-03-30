package partitionsiedata;
import java.util.Iterator;
import java.util.Set;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.Random;
import java.lang.StringBuffer;
import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.data.DataType;

public class FindPartition extends EvalFunc<String>
{
    public String exec(Tuple input) throws IOException {

      // Total number of buckets per day of log data
      // If you change this, then the corresponding number in FindPartition python 
      // tool should also be changed.
      long TOTBUCKETS = 200;
      HashMap<String, Double> querytype_tld_distribution= new HashMap<String, Double>();

      querytype_tld_distribution.put("A_COM",     new Double(34.494));
      querytype_tld_distribution.put("A_NET",     new Double(22.182));
      querytype_tld_distribution.put("A_ORG",     new Double(2.725));
      querytype_tld_distribution.put("A_ARPA",    new Double(0.136));
      querytype_tld_distribution.put("A_OTHR",    new Double(7.029));
      querytype_tld_distribution.put("PTR_COM",   new Double(0.098));
      querytype_tld_distribution.put("PTR_NET",   new Double(0.065));
      querytype_tld_distribution.put("PTR_ORG",   new Double(0.010));
      querytype_tld_distribution.put("PTR_ARPA",  new Double(15.260));
      querytype_tld_distribution.put("PTR_OTHR",  new Double(0.017));
      querytype_tld_distribution.put("AAAA_COM",  new Double(7.215));
      querytype_tld_distribution.put("AAAA_NET",  new Double(3.510));
      querytype_tld_distribution.put("AAAA_ORG",  new Double(0.358));
      querytype_tld_distribution.put("AAAA_ARPA", new Double(0.004));
      querytype_tld_distribution.put("AAAA_OTHR", new Double(2.001));
      querytype_tld_distribution.put("OTHR_COM",  new Double(2.357));
      querytype_tld_distribution.put("OTHR_NET",  new Double(1.220));
      querytype_tld_distribution.put("OTHR_ORG",  new Double(0.358));
      querytype_tld_distribution.put("OTHR_ARPA", new Double(0.295));
      querytype_tld_distribution.put("OTHR_OTHR", new Double(0.663));

      int queryType = (Integer)input.get(0);
      String revRegisteredDomain = (String)input.get(1);

      // Identify query type bucket
      String queryTypeBucket = "";
      if (queryType == 1 /*A record; About 66% */) {
        queryTypeBucket = "A";
      } else if (queryType == 12 /*PTR record; About 15% */) {
        queryTypeBucket = "PTR";
       } else if (queryType == 28 /*AAAA record; About 12% */) {
        queryTypeBucket = "AAAA";
      } else /*others; 7%*/ {
        queryTypeBucket = "OTHR";
      }

      // Identify TLD bucket
      String[] domain_parts = revRegisteredDomain.split("\\.");
      String tld = domain_parts[0];

      String topLevelDomain = "";
      if (tld.equals("com") || tld.equals("COM")) /*44% */ {
        topLevelDomain = "COM";
      } else if (tld.equals("net") || tld.equals("NET") /*26*/) {
        topLevelDomain = "NET";
      } else if (tld.equals("arpa") || tld.equals("ARPA") /*15*/) {
        topLevelDomain = "ARPA";
      } else if (tld.equals("org") || tld.equals("ORG") /*3*/) {
        topLevelDomain = "ORG";
      } else /*12%*/ {
        topLevelDomain = "OTHR";
      }

      String qtype_tld = queryTypeBucket + "_" + topLevelDomain;
      double percent_of_buckets_for_current_group  = querytype_tld_distribution.get(qtype_tld).doubleValue();

      // Identify domain bucket based on hashcode value
      long numbuckets = (long) (TOTBUCKETS*percent_of_buckets_for_current_group/100.0);
      if (numbuckets == 0)
        numbuckets = 1;

      int signedHashCode = revRegisteredDomain.hashCode();
      long unsignedHashCode = signedHashCode & 0x00000000ffffffffL;
      long bucket_id = unsignedHashCode % numbuckets;
      String domainBucketNumber = String.valueOf(bucket_id);

      // return string of type 'AAAA_NET_198'
      return (qtype_tld + "_" + domainBucketNumber);
    }
}
