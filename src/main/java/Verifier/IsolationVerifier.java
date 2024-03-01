package Verifier;

import java.awt.*;
import java.util.ArrayList;
import java.util.PriorityQueue;


public class IsolationVerifier {
    public ArrayList<ArrayList<Graph.Txn>> sessions;
    public Graph cGraph;
    public Graph.Txn[] sortedTxn;
    public IsolationVerifier(){
        cGraph = new Graph();
    }
    public long curTime = 0;
    public void initGraph(){

    }
    public void processWrite(){

    }
    public void processRead(){

    }
    public void feedTxn(Graph.Txn nextTxn){

    }

    public boolean overlap(Object a,Object b){
        long aStart=0,bStart=0;
        long aEnd=0,bEnd=0;
        if (a instanceof Graph.Txn){
            aStart = ((Graph.Txn) a).start;
            aEnd = ((Graph.Txn) a).end;
        }
        else if (a instanceof Graph.TxnOp){
            aStart = ((Graph.TxnOp) a).start;
            aEnd= ((Graph.TxnOp) a).end;
        }
        if (b instanceof Graph.Txn){
            bStart = ((Graph.Txn) b).start;
            bEnd = ((Graph.Txn) b).end;
        }
        else if (a instanceof Graph.TxnOp){
            bStart = ((Graph.TxnOp) b).start;
            bEnd= ((Graph.TxnOp) b).end;
        }
        if (aStart<bStart && aEnd>bStart)
            return  true;
        else return bStart < aStart && bEnd > aStart;


    }
    private void mergeSort(){
        PriorityQueue<Element> minHeap = new PriorityQueue<>();
        int resultSize = 0;
        for(int i=0;i<sessions.size();i++){
            ArrayList<Graph.Txn> session = sessions.get(i);
            resultSize+=session.size();
            if(!session.isEmpty())
                minHeap.add(new Element(session.removeFirst(),i));
        }
        sortedTxn = new Graph.Txn[resultSize];
        int index = 0;
        while (!minHeap.isEmpty()){
            Element element = minHeap.poll();
            sortedTxn[index++]  = element.value;
            feedTxn(element.value);
            if(!sessions.get(element.rowIndex).isEmpty()){
                minHeap.add(new Element(sessions.get(element.rowIndex).removeFirst(),element.rowIndex))
            }
        }
    }
    static class Element implements Comparable<Element> {
        Graph.Txn value;
        int rowIndex;
        public Element(Graph.Txn value, int rowIndex) {
            this.value = value;
            this.rowIndex = rowIndex;
        }
        @Override
        public int compareTo(Element other) {
            return Long.compare(this.value.start, other.value.start);
        }
    }



}
