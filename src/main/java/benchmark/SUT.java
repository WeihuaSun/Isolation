package benchmark;
import org.apache.logging.log4j.Logger;

public abstract class SUT<T extends TData<T>> {

    public Logger log;
    public final RunBench<T> rdata;
    public final Thread[] sutThreads;
    public final DataList<T> queue;
    //public HashMap<Integer,ArrayList<Graph.Txn>> result;
    public long numTxn;
    public SUT(RunBench<T> rdata){
        this.rdata = rdata;
        this.queue = rdata.getDataList();
        this.sutThreads = new Thread[rdata.numSUTThreads];
        this.numTxn = 0;
        //this.result = new HashMap<>();
        for (int i = 0; i<rdata.numSUTThreads;i++){
            //this.result.put(i,new ArrayList<>());
            SUTThread sut;
            try {
                sut = new SUTThread(i);
                sutThreads[i] = new Thread(sut);
                sutThreads[i].start();
            } catch (Exception ex) {
                log.error(ex.getMessage());
                log.info(ex);
            }
        }
    }

    public synchronized long incrementTxn() {
        numTxn++;
        return numTxn;
    }
    public void terminate() {
        synchronized (queue) {
            queue.truncate();
            for (int m = 0; m < rdata.numSUTThreads; m++) {
                T doneMsg = rdata.getTData();
                doneMsg.trans_type = TData.TT_DONE;
                queue.append(doneMsg);
            }
            queue.notify();
        }

    }
    public void queueAppend(T tdata) {
        synchronized (queue) {
            queue.append(tdata);
            queue.notify();
        }
    }
    public void queuePrepend(T tdata) {
        synchronized (queue) {
            queue.prepend(tdata);
            queue.notify();
        }
    }
    public abstract void processTransaction(T tdata,int sut_id);
    public abstract void afterTransaction(T tdata);
    public class SUTThread implements Runnable {
        private final int id;
        public SUTThread(int id){
            this.id = id;
        }
        public void run(){
            T tdata;
            for (;;) {
                synchronized (queue) {
                    while ((tdata = queue.first()) == null) {
                        try {
                            queue.wait();
                        } catch (InterruptedException e) {
                            log.error("sut-{} InterruptedException: {}", this.id, e.getMessage());
                            return;
                        }
                    }
                    queue.remove(tdata);
                    /* If there is more in the queue, notify the next. */
                    if (queue.first() != null)
                        queue.notify();
                }
                /* Special message type signaling that the benchmark is over. */
                if (tdata.trans_type == TData.TT_DONE)
                {   System.out.println("SUT done!");
                    break;
                }

                /* Stamp when the SUT started processing this transaction. */
                tdata.trans_start = System.currentTimeMillis();
                /* Process the requested transaction on the database. */
                processTransaction(tdata,id);
                /*
                 * Stamp the transaction end time into the terminal data.
                 */
                tdata.trans_end = System.currentTimeMillis();
                afterTransaction(tdata);
            }

        }
    }

}
