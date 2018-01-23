package scala.firstwork;

public class DealedMessageBean {
    String  client_ip;
    int is_blocked;
    String args;
    int status;
    String uid ;
    String host;

    public DealedMessageBean(String client_ip, int is_blocked, String args, int status, String uid, String host) {
        this.client_ip = client_ip;
        this.is_blocked = is_blocked;
        this.args = args;
        this.status = status;
        this.uid = uid;
        this.host = host;
    }

    public DealedMessageBean() {
    }
    public DealedMessageBean(OriginalMessageBean bean) {
        setArgs(bean.getArgs());
        setClient_ip(bean.getClient_ip());
        setHost(bean.getHost());
        setUid(bean.getUid());
        setStatus(Integer.parseInt(bean.getStatus()));
        setIs_blocked(Integer.parseInt(bean.getIs_blocked()));
    }

    public void setClient_ip(String client_ip) {
        this.client_ip = client_ip;
    }

    public void setIs_blocked(int is_blocked) {
        this.is_blocked = is_blocked;
    }

    public void setArgs(String args) {
        this.args = args;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public void setHost(String host) {
        this.host = host;
    }

    @Override
    public String toString() {
        return client_ip+"\t"+is_blocked+"\t"+args+"\t"+status+"\t"+uid+"\t"+host;
    }

    public String getClient_ip() {
        return client_ip;
    }

    public int getIs_blocked() {
        return is_blocked;
    }

    public String getArgs() {
        return args;
    }

    public int getStatus() {
        return status;
    }

    public String getUid() {
        return uid;
    }

    public String getHost() {
        return host;
    }
}
