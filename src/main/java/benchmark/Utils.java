package benchmark;

import java.io.*;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;

import Verifier.checker.Graph;
import net.openhft.hashing.LongHashFunction;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class Utils {
    public static Gson gson = new Gson();

    public static void setBitMapAt(byte[] b, int p) {
        b[p/8] |= (byte) (1<<p%8);
    }
    public static void clearBitMapAt(byte[] b, int p) {
        b[p/8] &= (byte) ~(1<<(p%8));
    }
    public static boolean getBitMapAt(byte[] b, int p) {
        return (b[p/8] & (1<<(p%8))) != 0;
    }

    public static String encodeTuple(int t) {
        return Integer.toString(t);
    }
    public static int decodeTuple_(String s){
        return Integer.parseInt(s);
    }

    public static String encodeTuple(HashMap<String, String> t) {
        return gson.toJson(t);
    }
    public static HashMap<String, String> decodeTuple(String json){
        Type type = new TypeToken<HashMap<String, String>>() {}.getType();
        return gson.fromJson(json, type);
    }

    public static String MakeTimeStamp() {
        return new SimpleDateFormat("dd-MM-yyyy").format(new Date());
    }
    public static String encodeKey(String tableName, int[] keys) {
        StringBuilder ret = new StringBuilder(tableName);
        for (int key : keys) {
            ret.append(":").append(Integer.toString(key));
        }
        return ret.toString();
    }

    public static String encodeKey(String tableName, String[] keys) {
        StringBuilder ret = new StringBuilder(tableName);
        for (String key : keys) {
            ret.append(":").append(key);
        }
        return ret.toString();
    }


    public static String[] decodeKey(String key) {
        return key.split(":");
    }

    public static String packVal(String val,long txnId,long opId){
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * 2);
        buffer.putLong(txnId);
        buffer.putLong(opId);
        String str_sig = Base64.getEncoder().encodeToString(buffer.array());
        return "&" + str_sig + val;
    }
    public static ValInfo unpackVal(String s){
        if(s.length() < 25 || s.charAt(0) != '&') {
            return null;
        }
        String str_sig = s.substring(1, 25);
        String real_val = s.substring(25);
        byte[] bs= Base64.getDecoder().decode(str_sig);
        ByteBuffer bf = ByteBuffer.wrap(bs);
        long txnId = bf.getLong();
        long wId = bf.getLong();
        return new ValInfo(real_val,txnId,wId);
    }
    public static class ValInfo{
        public String val;
        public long txnId;
        public long wId;
        public ValInfo(String val,long txnId,long wId){
            this.val = val;
            this.txnId = txnId;
            this.wId = wId;
        }

    }
    public static long hashString(String s){
        return LongHashFunction.xx().hashChars(s);
    }

    public static void dumpLog(Graph.Txn txn,String path) throws IOException{
            try (FileOutputStream outputStream = new FileOutputStream(path, true)) {
                while (!txn.txnOps.isEmpty()) {
                    outputStream.write(txn.txnOps.removeFirst().toByte());
                }
            }catch (IOException e){
                throw new IOException(e.getMessage());
            }
    }

}
