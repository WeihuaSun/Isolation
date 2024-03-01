package benchmark.tpcc;
import benchmark.Terminal;
import org.apache.logging.log4j.LogManager;
import java.util.Random;

public class TPCCTerminal extends Terminal<TPCCTData> {
    public TPCCTData[] terminal_data;
    private final Random random = new Random(System.currentTimeMillis());
    public TPCCTerminal(TPCCRun rdata) {
        super(rdata);
        log = LogManager.getLogger(TPCCTerminal.class);
        terminal_data = new TPCCTData[rdata.numTerminals];
        for (int t = 0; t < rdata.numTerminals; t++) {
            terminal_data[t] = new TPCCTData();
            terminal_data[t].term_w_id = (t / numTerminals) + 1;
            terminal_data[t].term_d_id = (t % 10) + 1;
            terminal_data[t].trans_type = TPCCTData.TT_NONE;
            terminal_data[t].trans_due = rdata.now;
            terminal_data[t].trans_start = terminal_data[t].trans_due;
            terminal_data[t].trans_end = terminal_data[t].trans_due;
            terminal_data[t].trans_error = false;
            queueAppend(terminal_data[t]);
        }
    }

    private double randomDouble() {
        return this.random.nextDouble();
    }

    private int nextTransactionType() {
        double chance = randomDouble() * 100.0;

        if (chance <= TPCCRun.paymentWeight)
            return TPCCTData.TT_PAYMENT;
        chance -= TPCCRun.paymentWeight;

        if (chance <= TPCCRun.orderStatusWeight)
            return TPCCTData.TT_ORDER_STATUS;
        chance -= TPCCRun.orderStatusWeight;

        if (chance <= TPCCRun.stockLevelWeight)
            return TPCCTData.TT_STOCK_LEVEL;
        chance -= TPCCRun.stockLevelWeight;

        if (chance <= TPCCRun.deliveryWeight)
            return TPCCTData.TT_DELIVERY;

        return TPCCTData.TT_NEW_ORDER;
    }

    private void generateNewOrder(TPCCTData tdata) {
        TPCCTData.NewOrderData screen = tdata.NewOrderData();
        screen.genInputData(tdata.term_w_id);
        tdata.new_order = screen;
    }
    private void generatePayment(TPCCTData tdata){
        TPCCTData.PaymentData screen = tdata.PaymentData();
        screen.genInputData(tdata.term_w_id);
        tdata.payment = screen;
    }
    private void generateOrderStatus(TPCCTData tdata){
        TPCCTData.OrderStatusData screen = tdata.OrderStatusData();
        screen.genInputData(tdata.term_w_id);
        tdata.order_status = screen;
    }
    private void generateStockLevel(TPCCTData tdata){
        tdata.stock_level = tdata.StockLevelData();
        tdata.stock_level.genInputData(tdata.term_w_id,tdata.term_d_id);
    }
    private void generateDelivery(TPCCTData tdata){
        tdata.delivery = tdata.DeliveryData();
        tdata.delivery.genInputData(tdata.term_w_id);
    }
    
    
    @Override
    public double processResult(TPCCTData tdata) {
        double think_time = switch (tdata.trans_type) {
            case TPCCTData.TT_NONE -> 0.0;
            case TPCCTData.TT_NEW_ORDER -> 12.0;
            case TPCCTData.TT_PAYMENT -> 12.0;
            case TPCCTData.TT_ORDER_STATUS -> 10.0;
            case TPCCTData.TT_STOCK_LEVEL -> 5.0;
            case TPCCTData.TT_DELIVERY -> 5.0;
            default -> 0.0;
        };
        /*
         * Initialize trans_rbk as false. The New Order input generator may set it to true.
         */
        tdata.trans_rbk = false;
        return think_time;
    }

    @Override
    public double generateNew(TPCCTData tdata) {
        double key_time;
        double key_mean;
        /*
         * Select the next transaction type.
         */
        tdata.trans_type = nextTransactionType();
        key_mean = switch (tdata.trans_type) {
            case TPCCTData.TT_NEW_ORDER -> {
                generateNewOrder(tdata);
                yield 18.0;
            }
            case TPCCTData.TT_PAYMENT -> {
                generatePayment(tdata);
                yield 3.0;
            }
            case TPCCTData.TT_ORDER_STATUS -> {
                generateOrderStatus(tdata);
                yield 2.0;
            }
            case TPCCTData.TT_STOCK_LEVEL -> {
                generateStockLevel(tdata);
                yield 2.0;
            }
            case TPCCTData.TT_DELIVERY -> {
                generateDelivery(tdata);
                yield 2.0;
            }
            default -> 0.0;
        };
        /*
         * Calculate keying time according to 5.2.5.4, then apply our non-standard multiplier that
         * allows us to drive a higher rate of transactions without scaling to more warehouses.
         */
        double r = randomDouble();
        if (r < 0.000045)
            key_time = key_mean * 10.0;
        else
            key_time = -Math.log(r) * key_mean;
        return  key_time;
    }
}
