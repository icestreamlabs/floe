use super::*;

pub(super) fn try_build_direct_join_output_projection(
    join: &dbsp::DbspJoinNode,
    steps: &[TransientSegmentStep],
) -> Option<Arc<Vec<EncodedRowProjectionColumn>>> {
    let mut project_expressions: Option<Arc<Vec<DbspProjectExpr>>> = None;
    for step in steps {
        match step {
            TransientSegmentStep::Passthrough => {}
            TransientSegmentStep::Select { .. } => return None,
            TransientSegmentStep::Project { expressions, .. } => {
                if project_expressions.is_some() {
                    return None;
                }
                project_expressions = Some(Arc::clone(expressions));
            }
        }
    }

    let expressions = project_expressions?;
    let left_width = join.left_schema.len();
    let columns = expressions
        .iter()
        .map(|expr| {
            let column_idx = projection_direct_column_index(expr, join.output_schema.as_ref())?;
            if column_idx < left_width {
                Some(EncodedRowProjectionColumn {
                    source: EncodedRowProjectionSource::Left,
                    index: column_idx,
                })
            } else {
                Some(EncodedRowProjectionColumn {
                    source: EncodedRowProjectionSource::Right,
                    index: column_idx - left_width,
                })
            }
        })
        .collect::<Option<Vec<_>>>()?;
    Some(Arc::new(columns))
}

pub(super) fn projection_direct_column_index(
    expr: &DbspProjectExpr,
    schema: &RowSchema,
) -> Option<usize> {
    match expr.expression().expr() {
        Expr::Alias(alias) => {
            projection_direct_column_index_expression(alias.expr.as_ref(), schema)
        }
        other => projection_direct_column_index_expression(other, schema),
    }
}

pub(super) fn projection_direct_column_index_expression(
    expr: &Expr,
    schema: &RowSchema,
) -> Option<usize> {
    match expr {
        Expr::Column(column) => projection_resolve_direct_column(schema, column),
        Expr::Alias(alias) => {
            projection_direct_column_index_expression(alias.expr.as_ref(), schema)
        }
        _ => None,
    }
}

pub(super) fn projection_resolve_direct_column(
    schema: &RowSchema,
    column: &Column,
) -> Option<usize> {
    let qualified = column.flat_name();
    schema
        .field_index(&qualified)
        .or_else(|| schema.field_index(&column.name))
}

pub(super) fn extract_encoded_row_int64_column(
    bytes: &[u8],
    target_index: usize,
) -> Result<Option<i64>> {
    if bytes.len() < 4 {
        bail!("encoded key too short");
    }
    let count = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    if target_index >= count {
        bail!("encoded row missing int64 column at index {target_index}");
    }

    let mut cursor = 4usize;
    for column_idx in 0..count {
        let tag = *bytes
            .get(cursor)
            .ok_or_else(|| anyhow!("unexpected end of key while decoding tag"))?;
        cursor += 1;
        if column_idx == target_index {
            return match tag {
                0x01 => {
                    let end = cursor + 8;
                    let chunk = bytes
                        .get(cursor..end)
                        .ok_or_else(|| anyhow!("truncated int64"))?;
                    Ok(Some(i64::from_le_bytes(chunk.try_into().unwrap())))
                }
                0x05 | 0x00 => Ok(None),
                other => Err(anyhow!(
                    "expected int64 encoded field at index {target_index}, found tag {other:#x}"
                )),
            };
        }
        cursor = skip_encoded_row_field(bytes, cursor, tag)?;
    }

    bail!("encoded row missing int64 column at index {target_index}")
}

pub(super) fn extract_encoded_row_i64_like_column(
    bytes: &[u8],
    target_index: usize,
) -> Result<Option<i64>> {
    if bytes.len() < 4 {
        bail!("encoded key too short");
    }
    let count = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    if target_index >= count {
        bail!("encoded row missing i64-like column at index {target_index}");
    }

    let mut cursor = 4usize;
    for column_idx in 0..count {
        let tag = *bytes
            .get(cursor)
            .ok_or_else(|| anyhow!("unexpected end of key while decoding tag"))?;
        cursor += 1;
        if column_idx == target_index {
            return match tag {
                0x01 | 0x03 => {
                    let end = cursor + 8;
                    let chunk = bytes
                        .get(cursor..end)
                        .ok_or_else(|| anyhow!("truncated fixed-width i64-like value"))?;
                    Ok(Some(i64::from_le_bytes(chunk.try_into().unwrap())))
                }
                0x05 | 0x07 | 0x00 => Ok(None),
                other => Err(anyhow!(
                    "expected i64-like encoded field at index {target_index}, found tag {other:#x}"
                )),
            };
        }
        cursor = skip_encoded_row_field(bytes, cursor, tag)?;
    }

    bail!("encoded row missing i64-like column at index {target_index}")
}

fn skip_encoded_row_field(bytes: &[u8], cursor: usize, tag: u8) -> Result<usize> {
    match tag {
        0x00 | 0x05 | 0x06 | 0x07 | 0x08 => Ok(cursor),
        0x01 | 0x03 => {
            let end = cursor + 8;
            bytes
                .get(cursor..end)
                .ok_or_else(|| anyhow!("truncated fixed-width value"))?;
            Ok(end)
        }
        0x02 => {
            let len_bytes = bytes
                .get(cursor..cursor + 4)
                .ok_or_else(|| anyhow!("truncated string length"))?;
            let len = u32::from_le_bytes(len_bytes.try_into().unwrap()) as usize;
            let end = cursor + 4 + len;
            bytes
                .get(cursor + 4..end)
                .ok_or_else(|| anyhow!("truncated string payload"))?;
            Ok(end)
        }
        0x04 => {
            bytes
                .get(cursor)
                .ok_or_else(|| anyhow!("missing boolean payload"))?;
            Ok(cursor + 1)
        }
        _ => Err(anyhow!("unknown column tag {tag:#x} in MV key")),
    }
}

pub(super) fn try_build_direct_row_projection(
    project: &DbspProjectNode,
) -> Option<Arc<Vec<usize>>> {
    let columns = project
        .expressions()
        .iter()
        .map(|expr| projection_direct_column_index(expr, project.input_schema().as_ref()))
        .collect::<Option<Vec<_>>>()?;
    Some(Arc::new(columns))
}

pub(super) fn compose_direct_row_projection(
    first: Option<Arc<Vec<usize>>>,
    second: Arc<Vec<usize>>,
) -> Result<Arc<Vec<usize>>> {
    let Some(first) = first else {
        return Ok(second);
    };
    let mut composed = Vec::with_capacity(second.len());
    for projected_idx in second.iter().copied() {
        let Some(&source_idx) = first.get(projected_idx) else {
            bail!(
                "direct projection index {projected_idx} out of bounds for prior width {}",
                first.len()
            );
        };
        composed.push(source_idx);
    }
    Ok(Arc::new(composed))
}
