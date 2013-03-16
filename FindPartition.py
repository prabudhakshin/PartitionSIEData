#! /usr/bin/env python

import sys
import optparse
import StringIO
import registered_domain as regdom
from collections import defaultdict as defdic
import re

domainFileName = ""
dateRange = ""
queryTypes = ""

# WARNING: DO NOT CHANGE this unless the corresponding number in the
# TagRecord UDF is also changed.
TOTBUCKETS = 200

# Point this variable to the parent directory where the data for various
# days are stored day by day basis.
BASEPATH = "/user/pdhakshi/pigouts/smart_data_2months"

qtypeCodeToNameMap = {"1"  : "A",
                      "12" : "PTR",
                      "28" : "AAAA"}

qtypeNameToCodeMap = {      "KX" : "36",
                           "KEY" : "25",
                         "DHCID" : "49",
                            "DS" : "43",
                        "DNSKEY" : "48",
                          "CERT" : "37",
                           "APL" : "42",
                           "SOA" : "6",
                           "LOC" : "29",
                           "SPF" : "99",
                           "SIG" : "24",
                         "RRSIG" : "46",
                             "A" : "1",
                          "AAAA" : "28",
                         "SSHFP" : "44",
                           "PTR" : "12",
                          "NSEC" : "47",
                            "TA" : "32768",
                    "NSEC3PARAM" : "51",
                           "TXT" : "16",
                           "HIP" : "55",
                            "MX" : "15",
                         "AFSDB" : "18",
                      "IPSECKEY" : "45",
                            "RP" : "17",
                           "SRV" : "33",
                           "DLV" : "32769",
                         "DNAME" : "39",
                            "NS" : "2",
                          "TSIG" : "250",
                           "CAA" : "257",
                         "NSEC3" : "50",
                         "NAPTR" : "35",
                         "CNAME" : "5",
                          "TLSA" : "52",
                          "TKEY" : "249"}

acceptedQueryTypeCodes = ["36","25","49","43","48","37","42","6","29","99",
                          "24","46","1","28","44","12","47","32768","51","16",
                          "55","15","18","45","17","33","32769","39","2","250",
                          "257","50","35","5","52","249"]

# As far as query names are concerned, anything other than the following three
# are considered as OTHR type.
shrinkedQueryTypeNames = ["A", "AAAA", "PTR"]

bucketDistribution = {
                       "A_COM"     : 34.494,
                       "A_NET"     : 22.182,
                       "A_ORG"     : 2.725,
                       "A_ARPA"    : 0.136,  
                       "A_OTHR"    : 7.029,  
                       "PTR_COM"   : 0.098,  
                       "PTR_NET"   : 0.065,  
                       "PTR_ORG"   : 0.010,  
                       "PTR_ARPA"  : 15.260,  
                       "PTR_OTHR"  : 0.017,  
                       "AAAA_COM"  : 7.215,  
                       "AAAA_NET"  : 3.510,  
                       "AAAA_ORG"  : 0.358,  
                       "AAAA_ARPA" : 0.004,  
                       "AAAA_OTHR" : 2.001,  
                       "OTHR_COM"  : 2.357,  
                       "OTHR_NET"  : 1.220,  
                       "OTHR_ORG"  : 0.358,  
                       "OTHR_ARPA" : 0.295,  
                       "OTHR_OTHR" : 0.663 }

def printHelpExit(parser, errormsg):
  print "%s\n" % errormsg
  print parser.print_help()
  sys.exit()

def getRegex(domain_list):
  data = {}

  for line in domain_list:
    tokens = line.split('.')
    if len(tokens) < 2:
      print 'WARNING: %s' % line
      continue
    tokens.reverse()
    if not tokens[0] in data:
      data[tokens[0]] = []
    buf = StringIO.StringIO()
    for t in tokens[1:]:
      buf.write('%s\\\\.' % t)
    data[tokens[0]].append(buf.getvalue()[:-3])

  buf = StringIO.StringIO()
  for key, val in data.items():        
    buf2 = StringIO.StringIO()
    for v in val:
      buf2.write('%s|' % v)
    buf.write('(?:%s\\\\.(?:%s))|' % (key, buf2.getvalue()[:-1]))

  return '^%s$' % buf.getvalue()[:-1]

def write_pig(inputstring, outputstring, qtypestring, regexstring):
    template_file = 'template_pig_script.pig'
    buf = StringIO.StringIO()
    fin = open(template_file, 'r')    
    for line in fin:
        buf.write(line)

    fin.close()
    template = buf.getvalue().strip()

    print template % {'input' : inputstring,
                      'qtypes': qtypestring,
                      'regex' : regexstring,
                      'output': outputstring}

# Parse dateRange into separate dates and return a list containing 
# the dates. Each element in the list is a string.
# dateRange: 20120201, 20120301-20120303
# output: ['20120201', '20120301', '20120302', '20120303']
def parseDateField(parser, dateRange):
  datePeriods = dateRange.split(",")
  days = []
  for period in datePeriods:
    rangevalue = period.split("-")
    # only one day is given
    if len(rangevalue) == 1:
      days.append(rangevalue[0])
    # date range is given: 20120201-20120215
    elif len(rangevalue) == 2:
      days = days + map (str, range(int(rangevalue[0]), int(rangevalue[1])+1))
    else:
      printHelpExit(parser, "Date period has more than one hiphen (-)")
  return days

def parseQueryTypeField(parser, queryType):

  # regex to detect whether query CODE was given
  re_qcode = re.compile("^\d+$")
  # regex to detect whether query NAME was given
  re_qname = re.compile("^[a-zA-Z]+$")

  queryTypeNames = [] # list of names; only A, AAAA, PTR, OTHR are the possible values
  queryTypeCodes = [] # list of codes; valid query codes are possible values

  if queryType == "ALL":
    queryTypeNames = ["A", "PTR", "AAAA", "OTHR"]
    queryTypeCodes = ["0"]
  else:
    queries = [query.strip() for query in queryType.split(",")]
    for query in queries:
      # Check if query type code was supplied
      if re_qcode.match(query):
        if query not in acceptedQueryTypeCodes:
          printHelpExit(optparser, 'Given query type "%s" is not valid.' % (query))
        if query in qtypeCodeToNameMap:
          queryName = qtypeCodeToNameMap[query]
        else:
          queryName = "OTHR"
        # Skip if the query type was already added
        if queryName not in queryTypeNames:
          queryTypeNames.append(queryName)

        # add the query type code to the list
        if query not in queryTypeCodes:
          queryTypeCodes.append(query)

      # check if query type name was supplied
      elif re_qname.match(query):
        if query in qtypeNameToCodeMap:
          if qtypeNameToCodeMap[query] not in queryTypeCodes:
            queryTypeCodes.append(qtypeNameToCodeMap[query])
        else:
           printHelpExit(optparser, 'Given query type "%s" is not valid.' % (query))

        # Treat anything other than A, AAAA, PTR as OTHR
        if query not in shrinkedQueryTypeNames:
          query = "OTHR"

        # add query name to the list if it is not already in the list
        if query not in queryTypeNames:
          queryTypeNames.append(query) 

      else:
         printHelpExit(optparser, 'Given query type "%s" is not valid.' % (query))

  return queryTypeNames, queryTypeCodes


def parseArgs(argslist):
  usagestring = "Usage: %prog [options] domain_list_file"
  optparser = optparse.OptionParser(usage=usagestring)
  optparser.add_option("-p", "--period", dest="dateRange", help="Range of days of the form '20120201,20120203,...' or 20120201-20120227 or 20120201-20120227,20120301-20120330", type=str, default="20120405");
  optparser.add_option("-q", "--querytype", dest="queryType", help="Comma separated DNS querytypes (1,12,28...) to search. * to include all query types", type=str, default="1");
  optparser.add_option("-o", "--outputpath", dest="outputPath", help="Path to output dir", type=str, default="output.gz");

  (options, args) = optparser.parse_args(argslist)

  if (len(args) != 1):
    printHelpExit(optparser, 'Invalid number of positional arguments.')

  # domain file name
  domainFileName = args[0]

  # parse the list of days
  dateRange = options.dateRange
  days = parseDateField(optparser, dateRange)

  # outputpath to use in Pig script
  outputPath = options.outputPath

  # list of queries
  qtypefield = options.queryType

  queryTypeNames, queryTypeCodes = parseQueryTypeField(optparser, qtypefield)

  return domainFileName, days, queryTypeNames, queryTypeCodes, outputPath

def getJavahash(s):
  h = 0
  for c in s:
    h = (31 * h + ord(c)) & 0xFFFFFFFF
  hashvalue = ((h + 0x80000000) & 0xFFFFFFFF) - 0x80000000
  return hashvalue & 0x00000000ffffffff;

def getBucketNumber(percent_dist_for_group, domainHashCode):
  global TOTBUCKETS
  bucketCount = int(TOTBUCKETS*percent_dist_for_group/100.0)
  if (bucketCount < 1):
    bucketCount = 1

  return str(domainHashCode % bucketCount)

def findFiles(domainList, queryTypes, days):
  fileList = []
  for domain in domainList:
    regDomain = regdom.get_registered_domain(domain)
    revRegDomain = ".".join(regDomain.split(".")[::-1])

    tld = revRegDomain.split('.')[0]
    tld = tld.upper()

    if (tld != "COM" and
        tld != "NET" and
        tld != "ORG" and
        tld != "ARPA"):
        tld = "OTHR"

    domainHashCode = getJavahash(revRegDomain)

    qtype_tld_list = []
    for qtype in queryTypes:
      qtype_tld = qtype + "_" + tld
      percent_dist_for_group = bucketDistribution[qtype_tld]
      bucketnumber = getBucketNumber(percent_dist_for_group, domainHashCode)
      qtype_tld_list.append(qtype_tld + "_" + bucketnumber)

    # Add the file to the output list only if it was not added already while processing some
    # other domain.
    for afile in qtype_tld_list:
      if afile not in fileList:
        fileList.append(afile)

  #return "{%s}/{%s}" % (",".join(days), ",".join(fileList))
  return fileList

def makeInputString(fileList, days):
  global BASEPATH
  # Generate outputfilelist for each day
  # {20120201,20120202}/{A_COM_0,AAAA_NET_10}.gz/*.gz
  filesToSearch = "{%s}/{%s}.gz/*.gz" % (",".join(days), ",".join(fileList))
  return BASEPATH + "/" + filesToSearch 

def makeQueryString(queryTypes):
  if len(queryTypes) == 1 and queryTypes[0]== "0":
    #wildcard case; Need to include all query types
    querystring = "qtype != 0"
    return querystring
  else:
    temp = []
    for atype in queryTypes:
      temp.append("(qtype == " + atype + ")")
    return " OR ".join(temp)

def main():

  domainFileName, days, queryTypeNames, queryTypeCodes, outputPath = parseArgs(sys.argv[1:])
  domainFileDesc = open(domainFileName, "r")

  domainList = []
  regDomainList = []

  # read domains form the input file
  for domain in domainFileDesc:
    domain = domain.strip().lower()
    if domain[-1] == '.':
      domain = domain[:-1]
    regdomain = regdom.get_registered_domain(domain)
    if regdomain is not None:
      domainList.append(domain)
      regDomainList.append(regdomain)
    else:
      print 'Domain %s does not have valid registered domain, skipping.' % domain

  filesToSearch = findFiles(domainList, queryTypeNames, days)
  inputPath = makeInputString(filesToSearch, days)
  print inputPath
  regexString = getRegex(regDomainList)
  queryString = makeQueryString (queryTypeCodes)
  write_pig(inputPath, outputPath, queryString, regexString)

if __name__ == "__main__":
  main()
