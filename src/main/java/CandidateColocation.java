import java.io.*;
import java.math.BigInteger;
import java.util.*;

public class CandidateColocation {

    static void createCandidateColocations(List<String> list) throws IOException {
        long len= (long) Math.pow(2,list.size());

        HashMap<Integer,List<String>> candidateColocations=new HashMap<>();
        for(long c=1;c<len;c++)
        {
            int val=Long.bitCount(c);
            if(val>1)
            {
//                List<String> colocOfSizeC=new ArrayList<>();
                StringBuilder colocOfSizeC=new StringBuilder();
                for(int i=0;i<list.size();i++)
                {
                    if(BigInteger.valueOf(c).testBit(i))
                    {
                        colocOfSizeC.append(list.get(i));
                        colocOfSizeC.append(" ");
                    }
                }
                if(candidateColocations.containsKey(val))
                {
                    List<String> l=candidateColocations.get(val);
                    l.add(colocOfSizeC.toString());
                    candidateColocations.replace(val,l);
                }
                else
                {
                    List<String> l=new ArrayList<>();
                    l.add(colocOfSizeC.toString());
                    candidateColocations.put(val,l);
                }
            }
        }
        PrintWriter out=null;
        try
        {
            out=new PrintWriter(new FileWriter("/Users/gagan/Downloads/BTP_Data/Candidate_Colocations.txt"));
            for(int k:candidateColocations.keySet())
            {
                List<String> l=candidateColocations.get(k);
                for(String str:l)
                {
                    out.println(str);
                }
            }
        }
        finally {
            if(out!=null)
            {
                out.close();
            }
        }

    }
    public static void main(String[] args) throws IOException {
        TreeSet<String> distinctEventTypes=new TreeSet<>();
        BufferedReader in=null;
        try
        {
            in= new BufferedReader(new FileReader("/Users/gagan/Downloads/BTP_Data/sample.txt"));
            String l;
            while((l=in.readLine())!=null)
            {
                Object o=FractionScore.create_Object(l);
                distinctEventTypes.add(o.event_type);

            }
        }
        finally {
            if(in!=null)
            {
                in.close();
            }
        }

        List<String> list=new ArrayList<>(distinctEventTypes);
        for(String s:list)
        {
            System.out.println(s);
        }
        System.out.println(list.size());
        createCandidateColocations(list);
    }
}
