package benchmark;

import java.util.Random;

public class RandomUtils {
    private Random random;
    private static final char[] aStringChars = {'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K',
            'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd',
            'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w',
            'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};
    public RandomUtils(){
        this.random = new Random(System.nanoTime());
    }
    public String getAString(long x,long y){
        StringBuilder result = new StringBuilder();
        long len = nextLong(x, y);
        long have = 1;
        if (y <= 0)
            return result.toString();
        result.append(aStringChars[(int) nextLong(0, 51)]);
        while (have < len) {
            result.append(aStringChars[(int) nextLong(0, 61)]);
            have++;
        }
        return result.toString();
    }
    /*
     * nextLong(x, y)
     *
     * Produce a random number uniformly distributed in [x .. y]
     */
    public long nextLong(long x, long y) {
        return (long) (random.nextDouble() * (y - x + 1) + x);
    }
    /*
     * nextInt(x, y)
     *
     * Produce a random number uniformly distributed in [x .. y]
     */
    public int nextInt(int x, int y) {
        return (int) (random.nextDouble() * (y - x + 1) + x);
    }

    public boolean nextBoolean(){
        return random.nextBoolean();
    }
}
