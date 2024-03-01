package benchmark;

import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class Scheduler<T extends TData<T>> implements Runnable {
    private static final Logger log = LogManager.getLogger(Scheduler.class);
    public static final int SCHED_TERMINAL_DATA = 0, SCHED_DONE = 1;

    public RunBench<T> rdata;
    private final Object avl_lock;
    private T avl_root;
    private int avl_num_nodes = 0;
    private int avl_max_nodes = 0;
    private int avl_max_height = 0;
    public  int current_trans_count;
    private final Random random;

    public Scheduler(RunBench<T> rdata){
        this.random = new Random(System.currentTimeMillis());
        this.avl_lock = new Object();
        this.rdata = rdata;
    }

    @Override
    public void run() {
        long now;
        T tdata;
        log.info("Scheduler, ready");
        for (;;) {
            /*
             * Fetch the next event from the "timestamp sorted" scheduler event queue.
             */
            synchronized (avl_lock) {
                try {
                    for (;;) {
                        /*
                         * If the queue is empty, we wait without a timeout until somebody is putting an event
                         * here. This actually should never happen because the main thread is placing the
                         * SCHED_DONE event into the queue on startup and this scheduler is going to exit when
                         * it receives that event.
                         */
                        if ((tdata = avl_first()) == null) {
                            log.trace("Scheduler, queue empty");
                            avl_lock.wait();
                            continue;
                        }
                        /*
                         * If the event at the head of the queue is not due yet, we wait until it is. We can get
                         * interrupted if someone is placing an event in front of the current queue head. In
                         * Java, we cannot distinguish between that and the timeout expiring or spurious wakeup,
                         * so we always recheck the head entry.
                         */
                        now = System.currentTimeMillis();
                        if (tdata.trans_due > now) {
                            log.trace("Scheduler, next event due at {}", new java.sql.Timestamp(tdata.trans_due));
                            avl_lock.wait(tdata.trans_due - now);
                            continue;
                        }
                        /*
                         * We received an event that is due now (or in the past). Consume it from the queue and
                         * exit the wait loop.
                         */
                        avl_remove(tdata);
                        break;
                    }
                } catch (InterruptedException e) {
                    log.error("Scheduler, InterruptedException: {}", e.getMessage());
                    return;
                }
            }
            /*
             * If this is the SCHED_DONE event, the benchmark duration has elapsed and we can exit.
             */
            if (tdata.sched_code == SCHED_DONE)
            {
                System.out.println("Seche Done!");
                break;
            }

            else if (tdata.sched_code == SCHED_TERMINAL_DATA) {
                rdata.systemUnderTest.queueAppend(tdata);
                current_trans_count++;
            }
            else {
                log.error("Scheduler, unknown scheduler code {}", tdata.sched_code);
                break;
            }
        }
        log.info("Scheduler, done");
    }

    public void at(long when, int code, T tdata) {
        tdata.sched_code = code;
        tdata.trans_due = when;

        synchronized (avl_lock) {
            avl_insert(tdata);
            avl_lock.notify();
        }
    }

    public void after(long delay, int type, T tdata) {
        at(System.currentTimeMillis() + delay, type, tdata);
    }

    /*
     * avl_insert()
     */
    private void avl_insert(T tdata) {
        tdata.sched_fuzz = randomInt(0, 999999999);
        while (avl_find(tdata) != null)
            tdata.sched_fuzz = randomInt(0, 999999999);

        avl_root = avl_insert_node(avl_root, tdata);

        avl_num_nodes++;
        if (avl_max_nodes < avl_num_nodes)
            avl_max_nodes = avl_num_nodes;
        if (avl_max_height < avl_root.tree_height)
            avl_max_height = avl_root.tree_height;
    }

    private T avl_insert_node(T into, T node) {
        long side;

        if (into == null)
            return node;

        side = avl_compare(node, into);
        if (side < 0)
            into.term_left = avl_insert_node(into.term_left, node);
        else if (side > 0)
            into.term_right = avl_insert_node(into.term_right, node);
        else
            log.error("Scheduler, duplicate avl node");
        return avl_balance(into);
    }

    /*
     * avl_remove()
     */
    private void avl_remove(T tdata) {
        avl_root = avl_remove_node(avl_root, tdata);
        avl_num_nodes--;
    }

    private T avl_remove_node(T stack, T needle) {
        T result;
        long side;

        if (stack == null) {
            //.error("Scheduler, entry not found in avl_remove_node: {}", needle.dumpHdr());
            return null;
        }
        side = avl_compare(needle, stack);
        if (side == 0) {
            result = avl_move_right(stack.term_left, stack.term_right);
            stack.term_left = null;
            stack.term_right = null;
            return result;
        }
        if (side < 0)
            stack.term_left = avl_remove_node(stack.term_left, needle);
        else
            stack.term_right = avl_remove_node(stack.term_right, needle);
        return avl_balance(stack);
    }

    private T avl_move_right(T node, T right) {
        if (node == null)
            return right;
        node.term_right = avl_move_right(node.term_right, right);
        return avl_balance(node);
    }

    /*
     * avl_first()
     */
    private T avl_first() {
        T node = avl_root;

        if (node == null)
            return null;

        while (node.term_left != null)
            node = node.term_left;

        return node;
    }

    /*
     * avl_find()
     */
    private T avl_find(T tdata) {
        return avl_find_node(avl_root, tdata);
    }

    private T avl_find_node(T stack, T needle) {
        long side;

        if (stack == null)
            return null;

        side = avl_compare(needle, stack);
        if (side == 0)
            return stack;
        if (side < 0)
            return avl_find_node(stack.term_left, needle);
        else
            return avl_find_node(stack.term_right, needle);
    }

    /*
     * avl_compare()
     */
    private long avl_compare(T node1, T node2) {
        long result;

        result = node1.trans_due - node2.trans_due;
        if (result != 0)
            return result;
        return node1.sched_fuzz - node2.sched_fuzz;
    }

    /*
     * avl_delta()
     */
    private int avl_delta(T node) {
        return ((node.term_left == null) ? 0 : node.term_left.tree_height)
                - ((node.term_right == null) ? 0 : node.term_right.tree_height);
    }

    /*
     * avl_balance()
     */
    private T avl_balance(T node) {
        int delta = avl_delta(node);
        if (delta < -1) {
            if (avl_delta(node.term_right) > 0)
                node.term_right = avl_rotate_right(node.term_right);
            return avl_rotate_left(node);
        } else if (delta > 1) {
            if (avl_delta(node.term_left) < 0)
                node.term_left = avl_rotate_left(node.term_left);
            return avl_rotate_right(node);
        }
        node.tree_height = 0;
        if (node.term_left != null && node.term_left.tree_height > node.tree_height)
            node.tree_height = node.term_left.tree_height;
        if (node.term_right != null && node.term_right.tree_height > node.tree_height)
            node.tree_height = node.term_right.tree_height;
        node.tree_height++;
        return node;
    }

    /*
     * avl_rotate_left()
     */
    private T avl_rotate_left(T node) {
        T r = node.term_right;
        node.term_right = r.term_left;
        r.term_left = avl_balance(node);
        return avl_balance(r);
    }

    /*
     * avl_rotate_right()
     */
    private T avl_rotate_right(T node) {
        T l = node.term_left;
        node.term_left = l.term_right;
        l.term_right = avl_balance(node);
        return avl_balance(l);
    }
    private long randomInt(long min, long max) {
        return (long) (this.random.nextDouble() * (max - min + 1) + min);
    }

}
