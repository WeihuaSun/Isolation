package Graph;

import java.util.ArrayList;

public class WritePair {

        public TransactionLT txnA;
        ArrayList<WritePair> parent;
        public TransactionLT txnB;
        WriteLT writeA;
        WriteLT writeB;
        long endTime;
        int stage = 0;
        long checkTime;
        public WritePair(WriteLT writeA,WriteLT writeB){
            this.writeA = writeA;
            this.writeB = writeB;
            this.endTime = Math.max(writeA.parent.end,writeB.parent.end);
        }
        public boolean checkState(){
            stage++;
            if(stage==2){
                parent.remove(this);
                return true;
            }else
                return false;
        }


}
