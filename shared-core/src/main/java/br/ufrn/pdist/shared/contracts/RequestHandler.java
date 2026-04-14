package br.ufrn.pdist.shared.contracts;

@FunctionalInterface
public interface RequestHandler {
    Response handle(Request request);
}
