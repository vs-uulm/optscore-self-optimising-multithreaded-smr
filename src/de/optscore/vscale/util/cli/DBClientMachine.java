package de.optscore.vscale.util.cli;

public class DBClientMachine {

    private int id;
    private String IP;
    private int port;
    private String description;

    public DBClientMachine() {
    }

    public DBClientMachine(int id, String IP, int port, String description) {
        this.id = id;
        this.IP = IP;
        this.port = port;
        this.description = description;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getIP() {
        return IP;
    }

    public void setIP(String IP) {
        this.IP = IP;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String toString() {
        return getId() + " | " + getIP() + ":" + getPort() + " | " + getDescription();
    }
}
