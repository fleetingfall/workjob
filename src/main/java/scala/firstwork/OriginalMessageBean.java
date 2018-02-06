package scala.firstwork;

public class OriginalMessageBean {
    String client_ip;
    String is_blocked;
    String args;
    String status;
    String uid ;
    String host;
    String request_timestamp;

    public OriginalMessageBean(String client_ip, String is_blocked, String args, String status, String uid, String host, String request_timestamp) {
        this.client_ip = client_ip;
        this.is_blocked = is_blocked;
        this.args = args;
        this.status = status;
        this.uid = uid;
        this.host = host;
        this.request_timestamp = request_timestamp;
    }

    public void setClient_ip(String client_ip) {
        this.client_ip = client_ip;
    }

    public void setIs_blocked(String is_blocked) {
        this.is_blocked = is_blocked;
    }

    public void setArgs(String args) {
        this.args = args;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setRequest_timestamp(String request_timestamp) {
        this.request_timestamp = request_timestamp;
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

    public String getRequest_timestamp() {
        return request_timestamp;
    }
}
