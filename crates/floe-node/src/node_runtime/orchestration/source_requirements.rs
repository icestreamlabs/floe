use super::*;

pub(super) fn required_column_masks_by_source_id(
    definitions: &[SourceDefinition],
    all_required_sources: &BTreeSet<String>,
    circuit_plans: &[CircuitPlan],
    plan_required_sources: &[BTreeSet<String>],
    full_width_sources: &BTreeSet<String>,
) -> anyhow::Result<Vec<Option<Arc<[bool]>>>> {
    let source_id_by_name = definitions
        .iter()
        .enumerate()
        .map(|(idx, definition)| (definition.name().to_string(), idx))
        .collect::<HashMap<_, _>>();
    let mut force_all_columns = vec![false; definitions.len()];
    mark_required_sources_full_width(
        &source_id_by_name,
        &mut force_all_columns,
        full_width_sources,
    );
    let mut masks = definitions
        .iter()
        .map(|definition| {
            all_required_sources
                .contains(definition.name())
                .then(|| vec![false; definition.columns().len()])
        })
        .collect::<Vec<_>>();

    for (plan, required_sources) in circuit_plans.iter().zip(plan_required_sources) {
        let Some(requirements) = plan_source_requirements(plan)? else {
            mark_required_sources_full_width(
                &source_id_by_name,
                &mut force_all_columns,
                required_sources,
            );
            continue;
        };
        let mut exact_sources = HashSet::new();
        for requirement in requirements {
            exact_sources.insert(requirement.source_name.clone());
            let Some(source_id) = source_id_by_name.get(&requirement.source_name).copied() else {
                return Err(anyhow!(
                    "plan referenced unknown source '{}'",
                    requirement.source_name
                ));
            };
            let Some(mask) = masks[source_id].as_mut() else {
                continue;
            };
            for column_idx in requirement.required_columns {
                let Some(required) = mask.get_mut(column_idx) else {
                    return Err(anyhow!(
                        "plan required column {column_idx} outside source '{}' schema",
                        requirement.source_name
                    ));
                };
                *required = true;
            }
        }
        let missing_exact_sources = required_sources
            .iter()
            .filter(|source| !exact_sources.contains(source.as_str()))
            .cloned()
            .collect::<BTreeSet<_>>();
        mark_required_sources_full_width(
            &source_id_by_name,
            &mut force_all_columns,
            &missing_exact_sources,
        );
    }
    for (source_id, definition) in definitions.iter().enumerate() {
        if let Some(mask) = masks[source_id].as_mut() {
            mark_source_primary_key_columns_required(definition, mask)?;
        }
    }

    Ok(masks
        .into_iter()
        .enumerate()
        .map(|(source_id, mask)| {
            mask.map(|mut mask| {
                if force_all_columns[source_id] {
                    mask.fill(true);
                }
                Arc::from(mask)
            })
        })
        .collect())
}

fn mark_source_primary_key_columns_required(
    definition: &SourceDefinition,
    mask: &mut [bool],
) -> anyhow::Result<()> {
    let Some(primary_key) = definition.property(SOURCE_PRIMARY_KEY_PROPERTY) else {
        return Ok(());
    };
    for column in primary_key
        .split(',')
        .map(str::trim)
        .filter(|column| !column.is_empty())
    {
        let Some(column_idx) = definition
            .columns()
            .iter()
            .position(|candidate| candidate.name() == column)
        else {
            return Err(anyhow!(
                "source '{}' primary key column '{}' is not present in its schema",
                definition.name(),
                column
            ));
        };
        if let Some(required) = mask.get_mut(column_idx) {
            *required = true;
        }
    }
    Ok(())
}

fn mark_required_sources_full_width(
    source_id_by_name: &HashMap<String, usize>,
    force_all_columns: &mut [bool],
    sources: &BTreeSet<String>,
) {
    for source in sources {
        if let Some(source_id) = source_id_by_name.get(source).copied()
            && let Some(force_all) = force_all_columns.get_mut(source_id)
        {
            *force_all = true;
        }
    }
}
