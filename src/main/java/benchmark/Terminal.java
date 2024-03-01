package benchmark;

import benchmark.tpcc.TPCCTData;
import org.apache.logging.log4j.Logger;

public abstract class Terminal<T extends TData<T>> {
    public Logger log;//日志，在子类中实例化
    public int numTerminals;//客户端数量
    public int numTransactions;//事务数量
    public int numMonkeys;//Monkey数量
    public RunBench<T> rdata;//运行数据
    public final DataList<T> queue;//数据队列
    public Thread[] monkeyThreads;//Monkey线程

    public int cur_trans_count;

    public abstract double processResult(T tdata);
    public abstract double generateNew(T tdata);

    public Terminal(RunBench<T> rdata) {
        this.rdata = rdata;
        this.numTerminals =rdata.numTerminals;
        this.numTransactions = rdata.numTransactions;
        this.numMonkeys = rdata.numMonkeys;
        this.monkeyThreads = new Thread[this.numMonkeys];
        this.queue = rdata.getDataList();
        this.cur_trans_count = -numMonkeys;
        for (int m = 0; m < numMonkeys; m++) {
            Monkey monkey = new Monkey(m);
            monkeyThreads[m] = new Thread(monkey);
            monkeyThreads[m].start();
        }
    }

    public void queueAppend(T tdata) {
        synchronized (queue) {
            queue.append(tdata);
            queue.notify();
        }
    }

    public void terminate() {
        synchronized (queue) {
            queue.truncate();
            for (int m = 0; m < numMonkeys; m++) {
                T doneMsg = rdata.getTData();
                doneMsg.trans_type = TPCCTData.TT_DONE;
                queue.append(doneMsg);
            }
            queue.notify();
        }

    }

    public class Monkey implements Runnable {
        private final int m_id;
        public Monkey(int m_id) {
            this.m_id = m_id;
        }

        @Override
        public void run() {
            T tdata;
            double think_time;
            double key_time;
            for (;;) {
                synchronized (queue) {
                    System.out.println(cur_trans_count);
                    if (cur_trans_count == numTransactions){
                        cur_trans_count++;
                        System.out.println("oneTerminal");
                        rdata.terminateAll();
                    }
                    while ((tdata = queue.first()) == null) {
                        try {
                            queue.wait();
                        } catch (InterruptedException e) {
                            log.error("monkey-{}, InterruptedException: {}", this.m_id, e.getMessage());
                            return;
                        }
                    }
                    queue.remove(tdata);
                    cur_trans_count++;
                    /*
                     * If there are more result, notify another input data generator (if there is an idle
                     * one).
                     */
                    if (queue.first() != null)
                        queue.notify();
                }
                /*
                 * Exit the loop (and terminate this thread) when we receive the DONE signal.
                 */
                if (tdata.trans_type == TPCCTData.TT_DONE)
                {
                    System.out.println("TERMINAL DONE!");
                    break;
                }

                /*
                 * Process the last transactions result and determine the think time based on the previous
                 * transaction type.
                 */
                think_time=processResult(tdata);
                key_time = generateNew(tdata);
                /*
                 * Set up the terminal data header fields. The Transaction due time is based on the last
                 * transactions end time. This eliminates delays caused by the monkeys not reading or typing
                 * at infinite speed.
                 */
                tdata.trans_due = tdata.trans_end + (long) ((think_time + key_time) * 1000.0);
                tdata.trans_start = 0;
                tdata.trans_end = 0;
                tdata.trans_error = false;
                rdata.scheduler.at(tdata.trans_due, Scheduler.SCHED_TERMINAL_DATA, tdata);
            }
        }
    }
}


