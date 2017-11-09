package io.enmasse.barnabas.controller.cluster.resources;

public interface Resource {
    void create();
    void delete();
}
