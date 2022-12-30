package de.optscore.vscale.util.cli;

import de.optscore.vscale.RequestProfile;

public class DBClientGroup {

    private int id;
    private int numOfClients;
    private RequestProfile requestProfile;
    private long sendDelayNs;
    private String description;

    public DBClientGroup() {

    }

    public DBClientGroup(int clientGroupId, int numOfClients, RequestProfile requestProfile, long sendDelayNs,
                         String description) {
        this.id = clientGroupId;
        this.numOfClients = numOfClients;
        this.requestProfile = requestProfile;
        this.sendDelayNs = sendDelayNs;
        this.description = description;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getNumOfClients() {
        return numOfClients;
    }

    public void setNumOfClients(int numOfClients) {
        this.numOfClients = numOfClients;
    }

    public RequestProfile getRequestProfile() {
        return requestProfile;
    }

    public void setRequestProfile(RequestProfile requestProfile) {
        this.requestProfile = requestProfile;
    }

    public long getSendDelayNs() {
        return sendDelayNs;
    }

    public void setSendDelayNs(long sendDelayNs) {
        this.sendDelayNs = sendDelayNs;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public String toString() {
        return getId() + " | " + getNumOfClients() + " clients | reqProfile " + getRequestProfile() + " | "
                + getSendDelayNs() + "ns sendDelay | " + getDescription();
    }
}
