package coocaa.test.common;

/**
 * 创建Url的String
 * Created by Administrator on 2017/8/15 0015.
 */
public class UrlBuilder {

    private String ip;
    private String port;

    private static final String URL_HEAD = "http://";
    private static final String COLON = ":";

    public UrlBuilder(String ip, String port) {
        this.ip = ip;
        this.port = port;
    }

    /**
     * 构建url（主要是构建不同的路径）
     * 这种形式：http://120.27.194.144:30013/userCrowidBind/v1/mac/1CA7702218D3/openid/-1/did/-1
     *
     * @param in_path 路径（以斜杠开头）
     * @return
     */
    public String buildUrl(String in_path) {
        StringBuffer url = new StringBuffer();
        if (this.ip == null || this.ip.trim().length() <= 0 ||
                this.port == null || this.port.trim().length() <= 0 ||
                in_path == null || in_path.trim().length() <= 0) {
            return url.toString();
        }
        url.append(URL_HEAD);
        url.append(this.ip);
        url.append(COLON);
        url.append(this.port);
        url.append(in_path);
        return url.toString();
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

}
