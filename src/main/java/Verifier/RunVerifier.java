package Verifier;

import Verifier.checker.TxnLevel;
import Verifier.checker.Verifier;

import java.util.Properties;

public class RunVerifier {
    public enum RecordLevel{Operator,Transaction,Group}
    public enum IsolationLevel {S_SER, SSI, SER, SI, RR, RC, RU}
    public Verifier verifier;

    public RunVerifier (Properties ini){
        RecordLevel recordLevel = RecordLevel.valueOf(ini.getProperty("record"));
        IsolationLevel isolationLevel = IsolationLevel.valueOf(ini.getProperty("isolation"));
        String logPath = ini.getProperty("log");
        if(recordLevel == RecordLevel.Transaction){
            verifier = new TxnLevel(isolationLevel,logPath);
        }

    }

}
