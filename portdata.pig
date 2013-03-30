REGISTER hdfs:///user/pdhakshi/jars/piggybank.jar;
REGISTER hdfs:///user/pdhakshi/jars/partitionsiedata.jar;
REGISTER hdfs:///user/hadoop/jars/scala-library-2.9.1.jar;
REGISTER hdfs:///user/hadoop/jars/elephant-bird-2.0.4-SNAPSHOT.jar;
REGISTER hdfs:///user/hadoop/jars/google-collect-1.0.jar;
REGISTER hdfs:///user/hadoop/jars/json-simple-1.1.jar;
REGISTER hdfs:///user/hadoop/jars/commons-lang-2.6.jar;
REGISTER hdfs:///user/hadoop/jars/dnsparser_2.9.1-1.0.jar;
REGISTER hdfs:///user/apitsill/jars/apitsill.jar;
REGISTER hdfs:///user/hadoop/jars/dnsudf_2.9.1-1.0.jar;
REGISTER hdfs:///user/hadoop/jars/hadoopdns.jar;

DEFINE ExtractRegisteredDomain org.chris.dnsproc.ExtractRegisteredDomain();   
DEFINE ReverseDomain com.apitsill.pig.ReverseDomain();

set mapred.child.java.opts '-Xmx4G'
-- set mapred.task.timeout 5400000 
set -XX -UseGCOverheadLimit
-- set job.priority LOW

rawcsv = LOAD '/feeds/sie/ch202/$paramyearmonth/raw_processed.$paramday*.0.gz' as (line:chararray);

-- Parse the CSVs using Chris's parser
dnsrequests = FOREACH rawcsv
    GENERATE FLATTEN(org.chris.dnsproc.ParseDNSFast(line)) as (
  ts: int,
  type: int,
  src_ip: chararray,
  dst_ip: chararray,
  domain: chararray,
  qtype: int,
  rcode: int,
  answer: bag { t: tuple(a:chararray, b:chararray, c:chararray, d:chararray, e:chararray) },
  authoritative: bag { t: tuple(a:chararray, b:chararray, c:chararray, d:chararray, e:chararray) },
  additional: bag { t: tuple(a:chararray, b:chararray, c:chararray, d:chararray, e:chararray) }
    );

p1 = FILTER dnsrequests by (ts is not null) AND (type == 0) AND (domain is not null);

-- Collect required columns, plus the reverse domain name
p2 = FOREACH p1 GENERATE ts, src_ip, dst_ip, domain, ExtractRegisteredDomain(LOWER(domain)) AS r_domain, qtype, answer;

p3 = FILTER p2 BY (r_domain is not null);

p4 = FOREACH p3 GENERATE ts, src_ip, dst_ip, domain, ReverseDomain(r_domain) AS rev_domain, qtype, answer;

p5 = GROUP p4 by (src_ip, domain, qtype);

p6 = FOREACH p5 {
       sorted_p4 = ORDER p4 by ts;
       GENERATE FLATTEN(partitionsiedata.PruneRecords(sorted_p4)) as (ts: int, 
                                                        src_ip: chararray,
                                                        dst_ip:chararray,
                                                        domain: chararray,
                                                        rev_domain: chararray,
                                                        qtype: int,
                                                        ttl: chararray,
                                                        answer: bag { answertuple: tuple (ip:chararray)});
       }

p7 = FOREACH p6 GENERATE ts, src_ip, dst_ip, domain, rev_domain, qtype, ttl, answer, partitionsiedata.FindPartition(qtype, rev_domain) as bucket_id:chararray;

p8 = GROUP p7 by bucket_id PARALLEL 200;

p9 = FOREACH p8 {
       sorted_p7 = ORDER p7 by rev_domain, domain, answer, dst_ip, ttl, src_ip, ts;
       GENERATE FLATTEN(sorted_p7);
     }

STORE p9 INTO '/user/pdhakshi/pigouts/siedata/$paramday' using org.apache.pig.piggybank.storage.MultiStorage('/user/pdhakshi/pigouts/siedata/$paramday', '8', 'gz');
