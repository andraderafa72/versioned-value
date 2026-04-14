package br.ufrn.pdist.shared.contracts;

public record Instance(String instanceId, ServiceName serviceName, String host, int port) {
}
