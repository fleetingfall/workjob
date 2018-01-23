package scala.firstwork;

public class OriginalMessageBean {
    String  client_ip;
    String is_blocked;
    String args;
    String status;
    String uid ;
    String host;

    public OriginalMessageBean(String client_ip, String is_blocked, String args, String status, String uid, String host) {
        this.client_ip = client_ip;
        this.is_blocked = is_blocked;
        this.args = args;
        this.status = status;
        this.uid = uid;
        this.host = host;
    }

    public OriginalMessageBean() {
    }

    public String getClient_ip() {
        return client_ip;
    }

    public String getIs_blocked() {
        return is_blocked;
    }

    public String getArgs() {
        return args;
    }

    public String getStatus() {
        return status;
    }

    public String getUid() {
        return uid;
    }

    public String getHost() {
        return host;
    }
}
