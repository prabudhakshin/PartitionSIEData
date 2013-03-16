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

-- Load files (this loads all files in the folder).
-- Specifying a folder recursively loads all files in it.
rawcsv = LOAD '%(input)s' as (
  ts: int,
  src_ip: chararray,
  dst_ip: chararray,
  domain: chararray,
  rev_domain: chararray,
  qtype: int,
  ttl: chararray,
  answer: bag { t: tuple(a:chararray) },
  filenamemeta: chararray
  );

p1 = FOREACH rawcsv GENERATE ts, src_ip, dst_ip, domain, rev_domain, qtype, answer;

p2 = FILTER p1 by (%(qtypes)s) AND rev_domain MATCHES '%(regex)s';

-- Store results to disk
STORE p2 INTO '%(output)s';
