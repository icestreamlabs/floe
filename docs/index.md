---
layout: default
title: Floe Documentation
description: Documentation for Floe, a single-node vectorized streaming SQL database.
---

# Floe documentation

<p class="lead">Floe is a single-node streaming SQL database built around a vectorized DBSP runtime, DataFusion planning, and SlateDB-backed state.</p>

<div class="actions">
  <a class="button" href="{{ site.baseurl }}/quickstart/">Start with the quickstart</a>
  <a class="button secondary" href="{{ site.baseurl }}/roadmap/">View the roadmap</a>
</div>

<div class="grid">
  <section class="card">
    <h3>Run a node</h3>
    <p>Start Floe with the built-in Nexmark generator, create a materialized view, and query it with psql.</p>
  </section>
  <section class="card">
    <h3>Stream changes</h3>
    <p>Use <code>COPY (SUBSCRIBE ...)</code> from psql to consume materialized-view deltas as they arrive.</p>
  </section>
  <section class="card">
    <h3>Connect data</h3>
    <p>Ingest from files, Kafka, HTTP, object storage, generator data, or native Postgres CDC.</p>
  </section>
  <section class="card">
    <h3>Operate one node</h3>
    <p>Use readiness endpoints, Prometheus metrics, CDC ops endpoints, and SlateDB persistence controls.</p>
  </section>
</div>

## Core pages

- [Quickstart]({{ site.baseurl }}/quickstart/)
- [SQL reference]({{ site.baseurl }}/sql/)
- [Connectors and sinks]({{ site.baseurl }}/connectors/)
- [Operations]({{ site.baseurl }}/operations/)
- [Roadmap]({{ site.baseurl }}/roadmap/)
