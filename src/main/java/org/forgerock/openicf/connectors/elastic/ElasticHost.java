package org.forgerock.openicf.connectors.elastic;

public class ElasticHost {

    private final String server;

    private final Integer port;

    public ElasticHost() {
        this.server = "localhost";
        this.port = 9300;
    }

    public ElasticHost(String server, Integer port) {
        this.server = server;
        this.port = port;
    }

    public String getServer() {
        return server;
    }

    public Integer getPort() {
        return port;
    }
}
