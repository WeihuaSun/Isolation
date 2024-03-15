package Verifier;

public class ISException {
    public static class InternalRead extends Exception {
        public InternalRead(Long txnId,Long readKey,Long readFromTxn){
            super("Internal Read Exception: "+String.format("Transaction ID:%d,Read Key:%d,Read From Transaction ID:%d",txnId,readKey,readFromTxn));
        }
        public InternalRead(){
            super("Internal Read ");
        }
    }
    public static class ReadFromUnknown extends Exception{
        public ReadFromUnknown(){
            super("Read From Unknown Update(Aborted or Replaced)");
        }
    }
    public static class ConflictUpdate extends Exception{
        public ConflictUpdate(){
            super("Conflict Update!");
        }
    }
    public static class CycleException extends Exception{
        public CycleException(){
            super("Cycle");
        }
    }

}


