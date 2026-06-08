---
layout: default
title: Floe Documentation
description: Documentation for Floe, a single-node vectorized streaming SQL database.
---

# Floe documentation

<p class="lead">Floe is a single-node streaming SQL database built around a vectorized DBSP runtime, DataFusion planning, and SlateDB-backed state.</p>

<div class="actions">
  <a class="button" href="{{ site.baseurl }}/quickstart/">Start with the quickstart</a>
  <a class="button secondary" href="{{ site.baseurl }}/connectors/">Configure connectors</a>
</div>

<div class="grid">
  <section class="card">
    <h3>Run a node</h3>
    <p>Start the single-node runtime with CLI flags or a TOML/YAML/JSON config file.</p>
  </section>
  <section class="card">
    <h3>Serve a view</h3>
    <p>Create one materialized view per process, query it over pgwire, and stream deltas with <code>COPY (SUBSCRIBE ...)</code>.</p>
  </section>
  <section class="card">
    <h3>Connect and replicate</h3>
    <p>Ingest from generator, file, HTTP, Kafka, object storage, and native Postgres CDC sources. Send MV or CDC output to Kafka, files, HTTP, or Postgres.</p>
  </section>
  <section class="card">
    <h3>Operate one node</h3>
    <p>Use readiness endpoints, Prometheus metrics, CDC/DLQ operations, storage flush, and SlateDB persistence controls.</p>
  </section>
</div>

## Core pages

- [Quickstart]({{ site.baseurl }}/quickstart/)
- [SQL reference]({{ site.baseurl }}/sql/)
- [Connectors and sinks]({{ site.baseurl }}/connectors/)
- [Operations]({{ site.baseurl }}/operations/)
- [Roadmap]({{ site.baseurl }}/roadmap/)
