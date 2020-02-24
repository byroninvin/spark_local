package mab;

/**
 * Created by Joe.Kwan on 2020/2/21
 */
public class BetaDistribution {
    /**
     * @param alpha: eg. click     * @param beta : eg. pv - click
     */

    public double BetaDist(double alpha, double beta) {
        double a = alpha + beta;
        double b = Math.sqrt((a - 2) / (2 * alpha * beta - a));
        if (Math.min(alpha, beta) <= 1) {
            b = Math.max(1 / alpha, 1 / beta);
        }
        double c = alpha + 1 / b;
        double W = 0;
        boolean reject = true;
        while (reject) {
            double U1 = Math.random();
            double U2 = Math.random();
            double V = b * Math.log(U1 / (1 - U1));
            W = alpha * Math.exp(V);
            reject = (a * Math.log(a / (beta + W)) + c * V - Math.log(4)) < Math.log(U1 * U1 * U2);
        }
        return (W / (beta + W));
    }

}


