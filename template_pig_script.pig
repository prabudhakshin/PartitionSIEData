rawcsv = LOAD '%(input)s' as (
  ts: int,
  src_ip: chararray,
  dst_ip: chararray,
  domain: chararray,
  rev_domain: chararray,
  qtype: int,
  ttl: chararray,
  answer: bag { t: tuple(a:chararray) }
  );

p1 = FOREACH rawcsv GENERATE ts, src_ip, dst_ip, domain, rev_domain, qtype, answer;

p2 = FILTER p1 by (%(qtypes)s) AND rev_domain MATCHES '%(regex)s';

STORE p2 INTO '%(output)s';
