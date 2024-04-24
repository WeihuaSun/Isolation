import Verifier.checker.OpLevelVerifier;
import benchmark.History;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Properties;

public class Run {
    private static final Logger log = LogManager.getLogger(Run.class);
    public static History history;

    public static void main(String[] args){
        new Run();
    }

    private String getProp(Properties p, String pName) {
        String prop = p.getProperty(pName);
        log.info("main, {}={}", pName, prop);
        return (prop);
    }
    public Run(){
        Properties ini = new Properties();
        try {
            ini.load(this.getClass().getResourceAsStream("runConfig.properties"));
        }catch (IOException e){
            log.error("main, could not load properties file");
        }

        log.info("main, Isolation Checker Start Running");

        String benchmark = getProp(ini,"benchmark");

        switch (benchmark){
            case "tpcc":
                break;
            case "twitter":
                //TwitterRun.run(ini);
                OpLevelVerifier verifier = new OpLevelVerifier();
                break;
            case "blindw":
                //todo:blindw get history
                break;
            case "rubis":
                //todo:rubis get history
                break;
            default:
                log.warn("main, unexpected benchmark.");
        }
        //TODO:检查历史














    }




}
