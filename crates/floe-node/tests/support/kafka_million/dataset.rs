use super::*;

pub(super) fn generate_dataset_file(
    path: &Path,
    spec: MillionQuerySpec,
    build_samples: bool,
) -> Result<ExpectedDataset> {
    let file =
        File::create(path).with_context(|| format!("create dataset file {}", path.display()))?;
    let mut writer = BufWriter::with_capacity(8 * 1024 * 1024, file);
    let mut expected = ExpectedDataset::default();
    let output_rows = match spec.dataset {
        MillionDatasetKind::BidOnly { project } => {
            let mut output_rows = 0usize;
            for bid_idx in 1..=BID_ROW_COUNT {
                let input = BidInput::from_bid_idx(bid_idx);
                input.write_json_line(&mut writer, bid_idx)?;
                expected.generated_rows += 1;
                if let Some(row) = project(&input) {
                    expected.metrics.apply(&row, 1);
                    output_rows += 1;
                }
            }
            output_rows
        }
        MillionDatasetKind::BidAuctionJoin {
            auction_rows,
            project,
        } => {
            if auction_rows < JOIN_AUCTION_ROW_COUNT {
                bail!(
                    "join dataset requires at least {} auction rows, got {}",
                    JOIN_AUCTION_ROW_COUNT,
                    auction_rows
                );
            }
            let mut auctions = Vec::with_capacity(auction_rows);
            for auction_idx in 1..=auction_rows {
                let auction = AuctionInput::from_auction_idx(auction_idx);
                auction.write_json_line(&mut writer, auction_idx)?;
                expected.generated_rows += 1;
                auctions.push(auction);
            }

            let mut output_rows = 0usize;
            for bid_idx in 1..=BID_ROW_COUNT {
                let input = BidInput::from_bid_idx(bid_idx);
                input.write_json_line(&mut writer, bid_idx)?;
                expected.generated_rows += 1;
                let auction = auctions
                    .get((input.auction - 1).max(0) as usize)
                    .with_context(|| {
                        format!("missing auction row for join key {}", input.auction)
                    })?;
                if let Some(row) = project(&input, auction) {
                    expected.metrics.apply(&row, 1);
                    output_rows += 1;
                }
            }
            output_rows
        }
    };

    writer.flush().context("flush dataset writer")?;

    if !build_samples {
        return Ok(expected);
    }

    let sample_ordinals = compute_sample_ordinals(output_rows, spec.sample_selection);
    if sample_ordinals.is_empty() {
        return Ok(expected);
    }
    let sample_field_idx = sample_field_index(spec.output_fields, spec.sample_match_field)?;

    match spec.dataset {
        MillionDatasetKind::BidOnly { project } => {
            let mut output_ordinal = 0usize;
            for bid_idx in 1..=BID_ROW_COUNT {
                let input = BidInput::from_bid_idx(bid_idx);
                let Some(row) = project(&input) else {
                    continue;
                };
                output_ordinal += 1;
                maybe_record_sample_row(
                    &mut expected,
                    &sample_ordinals,
                    &mut output_ordinal,
                    sample_field_idx,
                    spec.sample_match_field,
                    row,
                )?;
                if expected.sample_rows_by_key.len() == sample_ordinals.len() {
                    break;
                }
            }
        }
        MillionDatasetKind::BidAuctionJoin {
            auction_rows,
            project,
        } => {
            let auctions: Vec<_> = (1..=auction_rows)
                .map(AuctionInput::from_auction_idx)
                .collect();
            let mut output_ordinal = 0usize;
            for bid_idx in 1..=BID_ROW_COUNT {
                let input = BidInput::from_bid_idx(bid_idx);
                let auction = auctions
                    .get((input.auction - 1).max(0) as usize)
                    .with_context(|| {
                        format!("missing auction row for join key {}", input.auction)
                    })?;
                let Some(row) = project(&input, auction) else {
                    continue;
                };
                output_ordinal += 1;
                maybe_record_sample_row(
                    &mut expected,
                    &sample_ordinals,
                    &mut output_ordinal,
                    sample_field_idx,
                    spec.sample_match_field,
                    row,
                )?;
                if expected.sample_rows_by_key.len() == sample_ordinals.len() {
                    break;
                }
            }
        }
    }

    if expected.sample_rows_by_key.len() != sample_ordinals.len() {
        bail!(
            "captured {} sample rows, expected {}",
            expected.sample_rows_by_key.len(),
            sample_ordinals.len()
        );
    }

    Ok(expected)
}

pub(super) fn compute_sample_ordinals(
    total_rows: usize,
    selection: SampleSelection,
) -> BTreeSet<usize> {
    let mut out = BTreeSet::new();
    if total_rows == 0 {
        return out;
    }

    match selection {
        SampleSelection::FirstN(count) => {
            let end = count.min(total_rows);
            for idx in 1..=end {
                out.insert(idx);
            }
        }
        SampleSelection::EvenlySpaced(sample_count) => {
            if sample_count == 0 {
                return out;
            }
            if sample_count == 1 {
                out.insert(total_rows);
                return out;
            }
            let denominator = sample_count - 1;
            for i in 0..sample_count {
                let idx = 1 + (i * (total_rows - 1)) / denominator;
                out.insert(idx);
            }
        }
    }

    out
}

pub(super) fn maybe_record_sample_row(
    expected: &mut ExpectedDataset,
    sample_ordinals: &BTreeSet<usize>,
    output_ordinal: &mut usize,
    sample_field_idx: usize,
    sample_match_field: &str,
    row: ExpectedRow,
) -> Result<()> {
    if !sample_ordinals.contains(output_ordinal) {
        return Ok(());
    }

    let key = expected_value_key(row.values.get(sample_field_idx).with_context(|| {
        format!(
            "sample field index {} out of bounds for field '{}'",
            sample_field_idx, sample_match_field
        )
    })?);
    if expected
        .sample_rows_by_key
        .insert(key.clone(), row)
        .is_some()
    {
        bail!(
            "duplicate sample key '{key}' for field '{}'; choose a unique sample_match_field",
            sample_match_field
        );
    }
    Ok(())
}

pub(super) fn produce_dataset_file(
    dataset_path: &Path,
    brokers: &str,
    topic: &str,
    expected_rows: usize,
) -> Result<()> {
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("queue.buffering.max.messages", "200000")
        .set("queue.buffering.max.kbytes", "524288")
        .create()
        .context("create kafka producer")?;

    let file = File::open(dataset_path)
        .with_context(|| format!("open dataset file {}", dataset_path.display()))?;
    let reader = BufReader::with_capacity(8 * 1024 * 1024, file);

    let mut produced = 0usize;
    for line in reader.lines() {
        let line = line.context("read dataset line")?;
        loop {
            match producer.send(BaseRecord::<(), _>::to(topic).payload(&line)) {
                Ok(_) => break,
                Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), _record)) => {
                    producer.poll(Duration::from_millis(50));
                }
                Err((err, _record)) => {
                    return Err(err).context("produce kafka message");
                }
            }
        }

        produced += 1;
        if produced.is_multiple_of(10_000) {
            producer.poll(Duration::from_millis(0));
        }
        if produced.is_multiple_of(100_000) {
            eprintln!("produced {produced} rows to topic={topic}");
        }
    }

    producer
        .flush(Duration::from_secs(120))
        .context("flush kafka producer")?;

    if produced != expected_rows {
        bail!("produced {produced} rows, expected {expected_rows}");
    }

    Ok(())
}
