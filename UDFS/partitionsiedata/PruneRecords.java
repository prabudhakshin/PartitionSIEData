package partitionsiedata;
import java.util.Iterator;
import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.backend.executionengine.ExecException;

public class PruneRecords extends EvalFunc<DataBag> {

  public PruneRecords() {
    tuplefactory = TupleFactory.getInstance();
  }

  private TupleFactory tuplefactory;

  private void copyTupleFields(Tuple resulttuple, Tuple srctuple) throws Exception {
    resulttuple.set(0, (Integer)srctuple.get(0)); // ts
    resulttuple.set(1, (String)srctuple.get(1));  // src_ip
    resulttuple.set(2, (String)srctuple.get(2));  // dst_ip
    resulttuple.set(3, (String)srctuple.get(3));  // domain 
    resulttuple.set(4, (String)srctuple.get(4));  // rev_domain 
    resulttuple.set(5, (Integer)srctuple.get(5)); // qtype
    // set the ttl to be 0 for now. It will be later reset if there is a valid ttl.
    resulttuple.set(6, "N/A");  //ttl

    DataBag resulttupleanswerbag = new DefaultDataBag();
    resulttuple.set(7, resulttupleanswerbag);    // Bag of answers

    try {
      // Copy the answer fields from the srctuple to the result tuple.
      addAddresstoTuple(resulttuple, srctuple);
    } catch(ExecException e) {
      System.out.println("Error while retrving answer bag (6) from the input tuple: ");
      e.printStackTrace();
    }
  }

  /**
   * Given a source record (as a tuple) and a resulttuple, it copies each individual answer (a tuple)
   * from the source records to the resulttuple's answer bag.
   **/
  private void addAddresstoTuple (Tuple resulttuple, Tuple srctuple) throws Exception {
    // Get a reference to the answer bag of resulttuple.
    DataBag resulttupleanswerbag = (DataBag)resulttuple.get(7);
    Tuple answertuple = null;
    // Get a reference to the answer bag of srctuple. 
    DataBag answerbag = (DataBag)srctuple.get(6);
    // Iterate over each individual answer in the answer bag and copy each answer
    // to the result.
    for(Tuple answer : answerbag) {
      // In the output, answer tuple has only one field: the ultimate answer (for ex, for
      // A typle records, the answer is the IP address).
      answertuple = tuplefactory.newTuple(1);
      try {
        // Field #4 of input answer tuple has the ultimate answer. Copy that to the answertuple.
        answertuple.set(0, (String)answer.get(4));
      } catch(ExecException e) {
        System.out.println("Error while retrieving field 4 from an answer");
        e.printStackTrace();
        continue;
      }

      // Add the individual answer to the result tuple's answer bag.
      resulttupleanswerbag.add(answertuple);

      //set the ttl field of the resulttuple
      try {
        // Field#1 of input answer has the TTL value.
        resulttuple.set(6, (String) answer.get(1));
      } catch(ExecException e) {
        System.out.println("Error while retrieving field 1 (ttl) from an answer");
        e.printStackTrace();
        continue;
      }
    }

    if (answerbag.size() > 0) {
      // There is atleast one answer. So copy the dst_ip from this source tuple
      resulttuple.set(2, (String)srctuple.get(2)); //dst_ip
    }
  }

  private boolean isPartofSameRequest(int ts_current, int ts_prev, Tuple currentResultTuple) {
    int time_diff = ts_current - ts_prev;

    if (time_diff == 0) {
      return true;
    }

    if (time_diff > 3) {
      return false;
    }

    // If the time diff is <=3, then see if answer section was already seen. If seen, then the current
    // records corresponds to a different request. Else, the current record is part of the same request
    // as the previous record.
    try {
      // If the TTL field is "N/A", it means we did not see an answer section from the previous
      // records. 
      if (((String) currentResultTuple.get(6)).equals("N/A")) {
        return true;
      }
    } catch(ExecException e) {
      System.out.println("Error while retrieving field 6 (ttl) from the result tuple");
      e.printStackTrace();
      return false;
    }

    return false;
  }

  /**
   * A bag of records grouped by <src_ip, domain, query_type> are passed as input.
   * Records within each group is sorted based on non-decreasing order of timestamp.
   *
   * The objective is, to coalesce records which belong to the same DNS resolution
   * request. This is done by iterating the input records and then coalescing records
   * which are not more than 3 secs apart from each other.
   *
   * The output is also a bag of records. Each record in the output has 9 fields as
   * described in the code.
   **/
  public DataBag exec(Tuple input) throws IOException {
    DataBag outputbag = new DefaultDataBag();
    Tuple prevtuple = null;
    Tuple resulttuple = tuplefactory.newTuple(8);
    DataBag inputbag = (DataBag)input.get(0);
    if (inputbag.size() == 0) {
      System.out.println("Bag does not have any tuples. Returning null");
      return null;
    }
    Iterator<Tuple> itertuple = inputbag.iterator();
    prevtuple = itertuple.next();

    try {
      // copy the fields from input tuple to output tuple
      copyTupleFields(resulttuple /*dest*/, prevtuple /*src*/);
    } catch (Exception e) {
      System.out.println("Exception while processing first tuple. Skipping process");
      e.printStackTrace();
      return outputbag;
    }

    while(itertuple.hasNext()) {
      try{
        Tuple curtuple = itertuple.next();
        // Check whether current record is part of the same request as the previous record.
        if (isPartofSameRequest((Integer)curtuple.get(0), (Integer)prevtuple.get(0), resulttuple)) {
          // If current record is part of same request as previous record, copy the answer section
          // from the current tuple to the result tuple.
          addAddresstoTuple(resulttuple, curtuple);
          prevtuple = curtuple;
        }
        else {
          // Current record and previous record belong to different requests. So add the result tuple
          // built so far to the output bag (one output tuple for each set of records corresponding to
          // a single DNS request) and start a new result tuple to represent next set of records.
          outputbag.add(resulttuple);
          // Create a new tuple
          resulttuple = tuplefactory.newTuple(8);
          prevtuple = curtuple;
          // Copy the fields from the current record to the result tuple. As more records that are part
          // of the current request is found, the result tuple is enhanced by adding answers.
          copyTupleFields(resulttuple /*dest*/, prevtuple /*src*/);
        }
      } catch (Exception e) {
          System.out.println("Exception while processing a tuple. Skipping the tuple");
          e.printStackTrace();
      }
    }
    outputbag.add(resulttuple);
    return outputbag;
  }
}
