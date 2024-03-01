package benchmark.tpcc;

import benchmark.TData;

public class TPCCTData extends TData<TPCCTData> {
    public final static int TT_NEW_ORDER = 0, TT_PAYMENT = 1, TT_ORDER_STATUS = 2, TT_STOCK_LEVEL = 3, TT_DELIVERY = 4, TT_DELIVERY_BG = 5, TT_NONE = 6, TT_DONE = 7;
    public final static String[] trans_type_names = {"NEW_ORDER", "PAYMENT", "ORDER_STATUS",
            "STOCK_LEVEL", "DELIVERY", "DELIVERY_BG", "NONE", "DONE"};
    private final TPCCRandom rnd = new TPCCRandom();
    public int term_w_id;
    public boolean trans_rbk;
    public int term_d_id;

    public NewOrderData new_order = null;
    public PaymentData payment = null;
    public OrderStatusData order_status = null;
    public StockLevelData stock_level = null;
    public DeliveryData delivery = null;
    public DeliveryBGData delivery_bg = null;
    private int w_id;

    public NewOrderData NewOrderData() {
        return new NewOrderData();
    }
    public class NewOrderData {
        /* terminal input data */
        public int w_id;
        public int d_id;
        public int c_id;

        public int[] ol_supply_w_id = new int[15];
        public int[] ol_i_id = new int[15];
        public int[] ol_quantity = new int[15];

        /* terminal output data */
        public String c_last;
        public String c_credit;
        public double c_discount;
        public double w_tax;
        public double d_tax;
        public int o_ol_cnt;
        public int o_id;
        public String o_entry_d;
        public double total_amount;
        public String execution_status;

        public String[] i_name = new String[15];
        public int[] s_quantity = new int[15];
        public String[] brand_generic = new String[15];
        public double[] i_price = new double[15];
        public double[] ol_amount = new double[15];

        public void genInputData(int w_id){
            int ol_count;
            int ol_idx = 0;
            /* 2.4.1.1 - w_id = terminal's w_id */
            this.w_id = w_id;

            /* 2.4.1.2 - random d_id and non-uniform random c_id */
            this.d_id = rnd.nextInt(1, 10);
            this.c_id = rnd.getCustomerID();

            /* 2.4.1.3 - random [5..15] order lines */
            ol_count = rnd.nextInt(5,15);
            while (ol_idx < ol_count) {
                /* 2.4.1.5 1) - non uniform ol_i_id */
                this.ol_i_id[ol_idx] = rnd.getItemID();

                /*
                 2.4.1.5 2) - In 1% of order lines the supply warehouse
                 is different from the terminal's home warehouse.
                */
                this.ol_supply_w_id[ol_idx] = w_id;

                /* 2.4.1.5 3) - random ol_quantity [1..10] */
                this.ol_quantity[ol_idx] = rnd.nextInt(1, 10);
                ol_idx++;
            }
            /*
             2.4.1.4 - 1% of orders must use an invalid ol_o_id in the last
             order line generated.
            */
            if (rnd.nextDouble(0.0, 100.0) <= 1) {
                this.ol_i_id[ol_idx - 1] = 999999;
            }
            while (ol_idx < 15) {
                this.ol_supply_w_id[ol_idx] = 0;
                this.ol_i_id[ol_idx] = 0;
                this.ol_quantity[ol_idx] = 0;
                ol_idx++;
            }
        }

    }

    public PaymentData PaymentData() {
        return new PaymentData();
    }

    /*
    Data used in Payment Transaction
     */
    public class PaymentData {
        /* terminal input data */
        public int w_id;
        public int d_id;
        public int c_id;
        public int c_d_id;
        public int c_w_id;
        public String c_last;
        public double h_amount;

        /* terminal output data */
        public String w_name;
        public String w_street_1;
        public String w_street_2;
        public String w_city;
        public String w_state;
        public String w_zip;
        public String d_name;
        public String d_street_1;
        public String d_street_2;
        public String d_city;
        public String d_state;
        public String d_zip;
        public String c_first;
        public String c_middle;
        public String c_street_1;
        public String c_street_2;
        public String c_city;
        public String c_state;
        public String c_zip;
        public String c_phone;
        public String c_since;
        public String c_credit;
        public double c_credit_lim;
        public double c_discount;
        public double c_balance;
        public String c_data;
        public String h_date;

        public void genInputData(int w_id){
            /* 2.5.1.1 - w_id = terminal's w_id */
            this.w_id = w_id;

            /* 2.5.1.2 - d_id = random [1..10] */
            this.d_id = rnd.nextInt(1, 10);

            /*
             2.5.1.2 - in 85% of cases (c_d_id, c_w_id) = (d_id, w_id)
             in 15% of cases they are randomly chosen.
            */

            if (rnd.nextInt(1, 100) <= 85) {
                this.c_d_id = this.d_id;
                this.c_w_id = this.w_id;
            } else {
                this.c_d_id = rnd.nextInt(1, 10);
                //we not change the value of w_id
            }

            /*
             2.5.1.2 - in 60% of cases customer is selected by last name,
             in 40% of cases by customer ID.
            */
            if (rnd.nextInt(1, 100) <= 60) {
                this.c_id = 0;
                this.c_last = rnd.getCLast();
            } else {
                this.c_id = rnd.getCustomerID();
                this.c_last = null;
            }

            /* 2.5.1.3 - h_amount = random [1.00 .. 5,000.00] */
            this.h_amount = ((double) rnd.nextLong(100, 500000)) / 100.0;
        }
    }

    public OrderStatusData OrderStatusData() {
        return new OrderStatusData();
    }

    /*
     * Data used in payment transaction.
     */
    public class OrderStatusData {
        /* terminal input data */
        public int w_id;
        public int d_id;
        public int c_id;
        public String c_last;

        /* terminal output data */
        public String c_first;
        public String c_middle;
        public double c_balance;
        public int o_id;
        public String o_entry_d;
        public int o_carrier_id;

        public int[] ol_supply_w_id = new int[15];
        public int[] ol_i_id = new int[15];
        public int[] ol_quantity = new int[15];
        public double[] ol_amount = new double[15];
        public String[] ol_delivery_d = new String[15];

        public void genInputData(int w_id) {
            /* 2.6.1.1 - w_id = terminal's w_id */
            this.w_id = w_id;

            /* 2.6.1.2 - d_id is random [1..10] */
            this.d_id = rnd.nextInt(1, 10);

            /*
             2.6.1.2 - in 60% of cases customer is selected by last name,
             in 40% of cases by customer ID.
            */
            if (rnd.nextInt(1, 100) <= 60) {
                this.c_id = 0;
                this.c_last = rnd.getCLast();
            } else {
                this.c_id = rnd.getCustomerID();
                this.c_last = null;
            }
        }
    }

    public StockLevelData StockLevelData() {
        return new StockLevelData();
    }

    /*
     * Data used in stockLevel transaction.
     */
    public class StockLevelData {
        /* terminal input data */
        public int w_id;
        public int d_id;
        public int threshold;

        /* terminal output data */
        public int low_stock;

        public void genInputData(int w_id, int d_id){
            this.w_id = w_id;
            this.d_id = d_id;
            this.threshold = rnd.nextInt(10, 20);
        }
    }

    public DeliveryData DeliveryData() {
        return new DeliveryData();
    }

    /*
    Data used in delivery transaction.
    */
    public class DeliveryData {
        /* terminal input data */
        public int w_id;
        public int o_carrier_id;

        /* terminal output data */
        public String execution_status;

        public void genInputData(int w_id){
            this.w_id = w_id;
            this.o_carrier_id = rnd.nextInt(1, 10);

        }
    }

    public DeliveryBGData DeliveryBGData() {
        return new DeliveryBGData();
    }

    public class DeliveryBGData {
        /* DELIVERY_BG data */
        public int w_id;
        public int o_carrier_id;
        public String ol_delivery_d;

        public int[] delivered_o_id;


    }
}

