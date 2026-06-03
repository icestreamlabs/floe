use std::collections::HashMap;
use std::str;

use anyhow::{Context, Result, anyhow, bail, ensure};
use bytes::Bytes;
use chrono::{DateTime, NaiveDateTime};
use floe_cdc_core::{
    CdcChange, CdcColumn, CdcPrimaryKey, CdcRow, CdcRowKey, CdcTableId, CdcTableSchema,
    UpstreamTableRef,
};
use floe_core::RowValue;
use floe_core::catalog::ColumnType;

use crate::PostgresLsn;

const PG_BOOL_OID: u32 = 16;
const PG_INT2_OID: u32 = 21;
const PG_INT4_OID: u32 = 23;
const PG_INT8_OID: u32 = 20;
const PG_TEXT_OID: u32 = 25;
const PG_BYTEA_OID: u32 = 17;
const PG_BPCHAR_OID: u32 = 1042;
const PG_VARCHAR_OID: u32 = 1043;
const PG_DATE_OID: u32 = 1082;
const PG_TIMESTAMP_OID: u32 = 1114;
const PG_TIMESTAMPTZ_OID: u32 = 1184;
const PG_NUMERIC_OID: u32 = 1700;
const PG_UUID_OID: u32 = 2950;
const PG_JSON_OID: u32 = 114;
const PG_JSONB_OID: u32 = 3802;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PostgresRelationId(u32);

impl PostgresRelationId {
    pub fn new(value: u32) -> Self {
        Self(value)
    }

    pub fn as_u32(self) -> u32 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostgresReplicaIdentity {
    Default,
    Nothing,
    Full,
    Index,
    Unknown(u8),
}

impl PostgresReplicaIdentity {
    fn from_wire(value: u8) -> Self {
        match value {
            b'd' => Self::Default,
            b'n' => Self::Nothing,
            b'f' => Self::Full,
            b'i' => Self::Index,
            other => Self::Unknown(other),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgOutputColumn {
    flags: u8,
    name: String,
    type_oid: u32,
    type_modifier: i32,
}

impl PgOutputColumn {
    pub fn is_key(&self) -> bool {
        self.flags & 1 == 1
    }

    pub fn flags(&self) -> u8 {
        self.flags
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn type_oid(&self) -> u32 {
        self.type_oid
    }

    pub fn type_modifier(&self) -> i32 {
        self.type_modifier
    }

    pub fn decimal_scale(&self) -> Option<i8> {
        match numeric_type_from_typmod(self.type_modifier) {
            Some(Ok(ColumnType::Decimal128 { scale, .. })) => Some(scale),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgOutputRelation {
    relation_id: PostgresRelationId,
    namespace: String,
    name: String,
    replica_identity: PostgresReplicaIdentity,
    columns: Vec<PgOutputColumn>,
}

impl PgOutputRelation {
    pub fn relation_id(&self) -> PostgresRelationId {
        self.relation_id
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn replica_identity(&self) -> PostgresReplicaIdentity {
        self.replica_identity
    }

    pub fn columns(&self) -> &[PgOutputColumn] {
        &self.columns
    }

    pub fn upstream_table_ref(&self) -> Result<UpstreamTableRef> {
        UpstreamTableRef::new(self.upstream_schema_name(), self.name())
    }

    pub fn to_cdc_schema(&self, table_id: CdcTableId) -> Result<CdcTableSchema> {
        let columns = self
            .columns
            .iter()
            .map(|column| {
                CdcColumn::new(
                    column.name(),
                    column_type_for_oid(column.type_oid(), column.type_modifier())?,
                    !column.is_key(),
                )
            })
            .collect::<Result<Vec<_>>>()?;
        let primary_key_columns = self
            .columns
            .iter()
            .filter(|column| column.is_key())
            .map(|column| column.name())
            .collect::<Vec<_>>();
        if primary_key_columns.is_empty() {
            bail!(
                "Postgres CDC relation '{}' has replica identity {:?} but no key columns; Floe requires a primary key or replica-identity key columns",
                self.qualified_name(),
                self.replica_identity
            );
        }
        let primary_key = CdcPrimaryKey::new(primary_key_columns)?;
        CdcTableSchema::new(table_id, self.upstream_table_ref()?, columns, primary_key)
    }

    pub fn tuple_to_cdc_row(&self, tuple: &PgOutputTuple) -> Result<CdcRow> {
        ensure!(
            tuple.values().len() == self.columns.len(),
            "pgoutput tuple column count {} does not match relation '{}' column count {}",
            tuple.values().len(),
            self.qualified_name(),
            self.columns.len()
        );
        let mut values = Vec::with_capacity(tuple.values().len());
        let mut unchanged_toast_indices = Vec::new();
        for (idx, (column, value)) in self.columns.iter().zip(tuple.values()).enumerate() {
            if matches!(value, PgOutputTupleValue::UnchangedToast) {
                values.push(None);
                unchanged_toast_indices.push(idx);
            } else {
                values.push(tuple_value_to_row_value(column, value)?);
            }
        }
        CdcRow::with_unchanged_toast_indices(values, unchanged_toast_indices)
    }

    pub fn tuple_to_cdc_key(&self, tuple: &PgOutputTuple) -> Result<CdcRowKey> {
        ensure!(
            tuple.values().len() == self.columns.len(),
            "pgoutput key tuple column count {} does not match relation '{}' column count {}",
            tuple.values().len(),
            self.qualified_name(),
            self.columns.len()
        );
        CdcRowKey::new(
            self.columns
                .iter()
                .zip(tuple.values())
                .filter(|(column, _)| column.is_key())
                .map(|(column, value)| {
                    tuple_value_to_row_value(column, value)?.ok_or_else(|| {
                        anyhow!(
                            "pgoutput key column '{}' in relation '{}' is NULL",
                            column.name(),
                            self.qualified_name()
                        )
                    })
                })
                .collect::<Result<Vec<_>>>()?,
        )
    }

    fn upstream_schema_name(&self) -> &str {
        if self.namespace.is_empty() {
            "pg_catalog"
        } else {
            self.namespace.as_str()
        }
    }

    fn qualified_name(&self) -> String {
        format!("{}.{}", self.upstream_schema_name(), self.name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgOutputType {
    type_oid: u32,
    namespace: String,
    name: String,
}

impl PgOutputType {
    pub fn type_oid(&self) -> u32 {
        self.type_oid
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PgOutputTupleValue {
    Null,
    UnchangedToast,
    Text(Bytes),
    Binary(Bytes),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgOutputTuple {
    values: Vec<PgOutputTupleValue>,
}

impl PgOutputTuple {
    pub fn values(&self) -> &[PgOutputTupleValue] {
        &self.values
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PgOutputMessage {
    Origin {
        commit_lsn: PostgresLsn,
        name: String,
    },
    Relation(PgOutputRelation),
    Type(PgOutputType),
    Insert {
        relation_id: PostgresRelationId,
        new: PgOutputTuple,
    },
    Update {
        relation_id: PostgresRelationId,
        old: Option<PgOutputTuple>,
        key: Option<PgOutputTuple>,
        new: PgOutputTuple,
    },
    Delete {
        relation_id: PostgresRelationId,
        old: Option<PgOutputTuple>,
        key: Option<PgOutputTuple>,
    },
    Truncate {
        relation_ids: Vec<PostgresRelationId>,
        cascade: bool,
        restart_identity: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgOutputCdcChange {
    relation: PgOutputRelation,
    change: CdcChange,
}

impl PgOutputCdcChange {
    pub fn relation(&self) -> &PgOutputRelation {
        &self.relation
    }

    pub fn change(&self) -> &CdcChange {
        &self.change
    }

    pub fn into_change(self) -> CdcChange {
        self.change
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgOutputDecodedChanges {
    relation: Option<PgOutputRelation>,
    changes: Vec<PgOutputCdcChange>,
}

impl PgOutputDecodedChanges {
    pub fn relation(&self) -> Option<&PgOutputRelation> {
        self.relation.as_ref()
    }

    pub fn changes(&self) -> &[PgOutputCdcChange] {
        &self.changes
    }

    pub fn into_changes(self) -> Vec<PgOutputCdcChange> {
        self.changes
    }
}

#[derive(Default)]
pub struct PgOutputDecoder {
    relations: HashMap<PostgresRelationId, PgOutputRelation>,
}

impl PgOutputDecoder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn relation(&self, relation_id: PostgresRelationId) -> Option<&PgOutputRelation> {
        self.relations.get(&relation_id)
    }

    pub fn decode_message(&mut self, data: Bytes) -> Result<PgOutputMessage> {
        let message = decode_pgoutput_message(data)?;
        if let PgOutputMessage::Relation(relation) = &message {
            self.relations
                .insert(relation.relation_id(), relation.clone());
        }
        Ok(message)
    }

    pub fn decode_cdc_change(&mut self, data: Bytes) -> Result<Option<PgOutputCdcChange>> {
        Ok(self.decode_cdc_changes(data)?.into_iter().next())
    }

    pub fn decode_cdc_changes(&mut self, data: Bytes) -> Result<Vec<PgOutputCdcChange>> {
        Ok(self.decode_cdc_changes_with_metadata(data)?.into_changes())
    }

    pub fn decode_cdc_changes_with_metadata(
        &mut self,
        data: Bytes,
    ) -> Result<PgOutputDecodedChanges> {
        let message = self.decode_message(data)?;
        let relation = match &message {
            PgOutputMessage::Relation(relation) => Some(relation.clone()),
            _ => None,
        };
        let changes = message_relation_changes(&message)
            .into_iter()
            .map(|(relation_id, change)| {
                let relation = self.relations.get(&relation_id).ok_or_else(|| {
                    anyhow!(
                        "pgoutput change references unknown relation id {}",
                        relation_id.as_u32()
                    )
                })?;
                Ok(PgOutputCdcChange {
                    relation: relation.clone(),
                    change: change_to_cdc(relation, change)?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(PgOutputDecodedChanges { relation, changes })
    }
}

pub fn decode_pgoutput_message(data: Bytes) -> Result<PgOutputMessage> {
    let mut reader = PgOutputReader::new(data);
    let tag = reader.take_u8().context("read pgoutput message tag")?;
    let message = match tag {
        b'O' => PgOutputMessage::Origin {
            commit_lsn: PostgresLsn::from_u64(reader.take_u64()?),
            name: reader.take_cstring()?,
        },
        b'R' => PgOutputMessage::Relation(parse_relation(&mut reader)?),
        b'Y' => PgOutputMessage::Type(parse_type(&mut reader)?),
        b'I' => parse_insert(&mut reader)?,
        b'U' => parse_update(&mut reader)?,
        b'D' => parse_delete(&mut reader)?,
        b'T' => parse_truncate(&mut reader)?,
        other => bail!(
            "unsupported pgoutput message tag 0x{other:02x} ('{}')",
            char::from(other)
        ),
    };
    reader.finish()?;
    Ok(message)
}

fn parse_relation(reader: &mut PgOutputReader) -> Result<PgOutputRelation> {
    let relation_id = PostgresRelationId::new(reader.take_u32()?);
    let namespace = reader.take_cstring()?;
    let name = reader.take_cstring()?;
    let replica_identity = PostgresReplicaIdentity::from_wire(reader.take_u8()?);
    let column_count = usize::from(reader.take_u16()?);
    let mut columns = Vec::with_capacity(column_count);
    for _ in 0..column_count {
        columns.push(PgOutputColumn {
            flags: reader.take_u8()?,
            name: reader.take_cstring()?,
            type_oid: reader.take_u32()?,
            type_modifier: reader.take_i32()?,
        });
    }
    ensure!(
        !columns.is_empty(),
        "pgoutput relation '{}' must have at least one column",
        name
    );
    Ok(PgOutputRelation {
        relation_id,
        namespace,
        name,
        replica_identity,
        columns,
    })
}

fn parse_type(reader: &mut PgOutputReader) -> Result<PgOutputType> {
    Ok(PgOutputType {
        type_oid: reader.take_u32()?,
        namespace: reader.take_cstring()?,
        name: reader.take_cstring()?,
    })
}

fn parse_insert(reader: &mut PgOutputReader) -> Result<PgOutputMessage> {
    let relation_id = PostgresRelationId::new(reader.take_u32()?);
    reader.expect_tag(b'N', "insert new tuple")?;
    Ok(PgOutputMessage::Insert {
        relation_id,
        new: parse_tuple(reader)?,
    })
}

fn parse_update(reader: &mut PgOutputReader) -> Result<PgOutputMessage> {
    let relation_id = PostgresRelationId::new(reader.take_u32()?);
    let mut old = None;
    let mut key = None;
    let tag = reader.take_u8()?;
    let tag = match tag {
        b'K' => {
            key = Some(parse_tuple(reader)?);
            reader.take_u8()?
        }
        b'O' => {
            old = Some(parse_tuple(reader)?);
            reader.take_u8()?
        }
        other => other,
    };
    ensure!(
        tag == b'N',
        "pgoutput update expected new tuple tag 'N', got 0x{tag:02x}"
    );
    Ok(PgOutputMessage::Update {
        relation_id,
        old,
        key,
        new: parse_tuple(reader)?,
    })
}

fn parse_delete(reader: &mut PgOutputReader) -> Result<PgOutputMessage> {
    let relation_id = PostgresRelationId::new(reader.take_u32()?);
    let tag = reader.take_u8()?;
    let tuple = parse_tuple(reader)?;
    match tag {
        b'K' => Ok(PgOutputMessage::Delete {
            relation_id,
            old: None,
            key: Some(tuple),
        }),
        b'O' => Ok(PgOutputMessage::Delete {
            relation_id,
            old: Some(tuple),
            key: None,
        }),
        other => bail!("pgoutput delete expected key or old tuple, got 0x{other:02x}"),
    }
}

fn parse_truncate(reader: &mut PgOutputReader) -> Result<PgOutputMessage> {
    let relation_count = reader.take_u32()? as usize;
    let options = reader.take_u8()?;
    let mut relation_ids = Vec::with_capacity(relation_count);
    for _ in 0..relation_count {
        relation_ids.push(PostgresRelationId::new(reader.take_u32()?));
    }
    Ok(PgOutputMessage::Truncate {
        relation_ids,
        cascade: options & 1 == 1,
        restart_identity: options & 2 == 2,
    })
}

fn parse_tuple(reader: &mut PgOutputReader) -> Result<PgOutputTuple> {
    let column_count = usize::from(reader.take_u16()?);
    let mut values = Vec::with_capacity(column_count);
    for _ in 0..column_count {
        let value = match reader.take_u8()? {
            b'n' => PgOutputTupleValue::Null,
            b'u' => PgOutputTupleValue::UnchangedToast,
            b't' => PgOutputTupleValue::Text(reader.take_len_bytes()?),
            b'b' => PgOutputTupleValue::Binary(reader.take_len_bytes()?),
            other => bail!("unsupported pgoutput tuple value tag 0x{other:02x}"),
        };
        values.push(value);
    }
    Ok(PgOutputTuple { values })
}

enum RelationChange<'a> {
    Insert(&'a PgOutputTuple),
    Update {
        old: Option<&'a PgOutputTuple>,
        key: Option<&'a PgOutputTuple>,
        new: &'a PgOutputTuple,
    },
    Delete {
        old: Option<&'a PgOutputTuple>,
        key: Option<&'a PgOutputTuple>,
    },
    Truncate,
}

fn message_relation_changes(
    message: &PgOutputMessage,
) -> Vec<(PostgresRelationId, RelationChange<'_>)> {
    match message {
        PgOutputMessage::Insert { relation_id, new } => {
            vec![(*relation_id, RelationChange::Insert(new))]
        }
        PgOutputMessage::Update {
            relation_id,
            old,
            key,
            new,
        } => vec![(
            *relation_id,
            RelationChange::Update {
                old: old.as_ref(),
                key: key.as_ref(),
                new,
            },
        )],
        PgOutputMessage::Delete {
            relation_id,
            old,
            key,
        } => vec![(
            *relation_id,
            RelationChange::Delete {
                old: old.as_ref(),
                key: key.as_ref(),
            },
        )],
        PgOutputMessage::Truncate { relation_ids, .. } => relation_ids
            .iter()
            .copied()
            .map(|relation_id| (relation_id, RelationChange::Truncate))
            .collect(),
        _ => Vec::new(),
    }
}

fn change_to_cdc(relation: &PgOutputRelation, change: RelationChange<'_>) -> Result<CdcChange> {
    match change {
        RelationChange::Insert(new) => {
            let row = relation.tuple_to_cdc_row(new)?;
            ensure!(
                !row.has_unchanged_toast(),
                "pgoutput insert for relation '{}' contains unchanged TOAST value",
                relation.qualified_name()
            );
            Ok(CdcChange::Insert { row })
        }
        RelationChange::Update { old, key, new } => Ok(CdcChange::Update {
            key: key
                .map(|tuple| relation.tuple_to_cdc_key(tuple))
                .transpose()?,
            before: old
                .map(|tuple| relation.tuple_to_cdc_row(tuple))
                .transpose()?,
            after: relation.tuple_to_cdc_row(new)?,
        }),
        RelationChange::Delete { old, key } => Ok(CdcChange::Delete {
            key: key
                .map(|tuple| relation.tuple_to_cdc_key(tuple))
                .transpose()?,
            before: old
                .map(|tuple| relation.tuple_to_cdc_row(tuple))
                .transpose()?,
        }),
        RelationChange::Truncate => Ok(CdcChange::Truncate),
    }
}

fn column_type_for_oid(type_oid: u32, type_modifier: i32) -> Result<ColumnType> {
    match type_oid {
        PG_BOOL_OID => Ok(ColumnType::Bool),
        PG_INT2_OID | PG_INT4_OID | PG_INT8_OID => Ok(ColumnType::Int64),
        PG_TEXT_OID | PG_BPCHAR_OID | PG_VARCHAR_OID | PG_UUID_OID | PG_JSON_OID | PG_JSONB_OID
        | PG_BYTEA_OID => Ok(ColumnType::Utf8),
        PG_DATE_OID => Ok(ColumnType::DateDays),
        PG_TIMESTAMP_OID | PG_TIMESTAMPTZ_OID => Ok(ColumnType::TimestampMillis),
        PG_NUMERIC_OID => {
            numeric_type_from_typmod(type_modifier).unwrap_or(Ok(ColumnType::Numeric))
        }
        _ => bail!("unsupported Postgres type OID {type_oid} in pgoutput relation metadata"),
    }
}

fn tuple_value_to_row_value(
    column: &PgOutputColumn,
    value: &PgOutputTupleValue,
) -> Result<Option<RowValue>> {
    match value {
        PgOutputTupleValue::Null => Ok(None),
        PgOutputTupleValue::Text(bytes) => {
            let value = str::from_utf8(bytes)
                .with_context(|| format!("decode text value for column '{}'", column.name()))?;
            parse_text_row_value(column, value).map(Some)
        }
        PgOutputTupleValue::Binary(_) => bail!(
            "binary pgoutput value for column '{}' is not supported yet",
            column.name()
        ),
        PgOutputTupleValue::UnchangedToast => bail!(
            "unchanged TOAST value for column '{}' must be handled by tuple_to_cdc_row",
            column.name()
        ),
    }
}

fn parse_text_row_value(column: &PgOutputColumn, value: &str) -> Result<RowValue> {
    match column.type_oid() {
        PG_BOOL_OID => match value {
            "t" | "true" | "1" => Ok(RowValue::Bool(true)),
            "f" | "false" | "0" => Ok(RowValue::Bool(false)),
            _ => bail!(
                "invalid boolean pgoutput value '{}' for column '{}'",
                value,
                column.name()
            ),
        },
        PG_INT2_OID | PG_INT4_OID | PG_INT8_OID => {
            value.parse::<i64>().map(RowValue::Int64).with_context(|| {
                format!(
                    "decode integer pgoutput value '{}' for column '{}'",
                    value,
                    column.name()
                )
            })
        }
        PG_TEXT_OID | PG_BPCHAR_OID | PG_VARCHAR_OID | PG_UUID_OID | PG_JSON_OID | PG_JSONB_OID
        | PG_BYTEA_OID => Ok(RowValue::Utf8(value.to_string())),
        PG_DATE_OID => parse_pg_date_days(value).map(RowValue::DateDays),
        PG_TIMESTAMP_OID => parse_pg_timestamp_millis(value).map(RowValue::TimestampMillis),
        PG_TIMESTAMPTZ_OID => parse_pg_timestamptz_millis(value).map(RowValue::TimestampMillis),
        PG_NUMERIC_OID => match numeric_type_from_typmod(column.type_modifier()) {
            Some(Ok(ColumnType::Decimal128 { scale, .. })) => {
                parse_decimal_text_to_i128(value, scale).map(RowValue::Decimal128)
            }
            Some(Ok(_)) | None => Ok(RowValue::Numeric(value.to_string())),
            Some(Err(err)) => Err(err),
        },
        _ => bail!(
            "unsupported Postgres type OID {} for column '{}'",
            column.type_oid(),
            column.name()
        ),
    }
}

fn numeric_type_from_typmod(type_modifier: i32) -> Option<Result<ColumnType>> {
    if type_modifier < 4 {
        return None;
    }
    let typmod = type_modifier - 4;
    let precision = (typmod >> 16) & 0xffff;
    let scale = typmod & 0xffff;
    if !(1..=38).contains(&precision) || !(0..=precision).contains(&scale) {
        return None;
    }
    Some(ColumnType::decimal128(
        precision as u8,
        i8::try_from(scale).expect("scale <= 38 fits i8"),
    ))
}

fn parse_decimal_text_to_i128(value: &str, scale: i8) -> Result<i128> {
    let scale = u32::try_from(scale).context("Decimal128 scale cannot be negative")?;
    let value = value.trim();
    let (negative, unsigned) = value
        .strip_prefix('-')
        .map(|rest| (true, rest))
        .unwrap_or((false, value));
    let unsigned = unsigned.strip_prefix('+').unwrap_or(unsigned);
    let (whole, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    let mut digits = String::with_capacity(whole.len() + scale as usize);
    digits.push_str(whole);
    let scale_usize = usize::try_from(scale).expect("u32 scale fits usize");
    ensure!(
        fraction.len() <= scale_usize,
        "decimal value '{value}' has more fractional digits than scale {scale}"
    );
    digits.push_str(fraction);
    digits.extend(std::iter::repeat_n('0', scale_usize - fraction.len()));
    let parsed = digits
        .parse::<i128>()
        .with_context(|| format!("decode decimal value '{value}'"))?;
    Ok(if negative { -parsed } else { parsed })
}

fn parse_pg_date_days(value: &str) -> Result<i32> {
    let (year, rest) = value
        .split_once('-')
        .ok_or_else(|| anyhow!("invalid Postgres date value '{value}'"))?;
    let (month, day) = rest
        .split_once('-')
        .ok_or_else(|| anyhow!("invalid Postgres date value '{value}'"))?;
    let year = year
        .parse::<i32>()
        .with_context(|| format!("decode Postgres date year from '{value}'"))?;
    let month = month
        .parse::<u32>()
        .with_context(|| format!("decode Postgres date month from '{value}'"))?;
    let day = day
        .parse::<u32>()
        .with_context(|| format!("decode Postgres date day from '{value}'"))?;
    ensure!(
        (1..=12).contains(&month) && (1..=31).contains(&day),
        "invalid Postgres date value '{value}'"
    );
    Ok(days_from_civil(year, month, day))
}

fn parse_pg_timestamp_millis(value: &str) -> Result<i64> {
    let parsed = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f")
        .with_context(|| format!("decode Postgres timestamp value '{value}'"))?;
    Ok(parsed.and_utc().timestamp_millis())
}

fn parse_pg_timestamptz_millis(value: &str) -> Result<i64> {
    if let Some(normalized) = normalize_pg_short_timezone_offset(value)
        && let Ok(parsed) = DateTime::parse_from_str(&normalized, "%Y-%m-%d %H:%M:%S%.f%:z")
    {
        return Ok(parsed.timestamp_millis());
    }
    for pattern in [
        "%Y-%m-%d %H:%M:%S%.f%:z",
        "%Y-%m-%d %H:%M:%S%.f%z",
        "%Y-%m-%d %H:%M:%S%.f %:z",
        "%Y-%m-%d %H:%M:%S%.f %z",
    ] {
        if let Ok(parsed) = DateTime::parse_from_str(value, pattern) {
            return Ok(parsed.timestamp_millis());
        }
    }
    bail!("decode Postgres timestamptz value '{value}'")
}

fn normalize_pg_short_timezone_offset(value: &str) -> Option<String> {
    let (idx, sign) = value
        .char_indices()
        .skip("YYYY-MM-DD".len())
        .find(|(_, ch)| *ch == '+' || *ch == '-')?;
    let suffix = &value[idx + sign.len_utf8()..];
    if suffix.len() == 2 && suffix.bytes().all(|byte| byte.is_ascii_digit()) {
        Some(format!("{}{}{}:00", &value[..idx], sign, suffix))
    } else {
        None
    }
}

fn days_from_civil(year: i32, month: u32, day: u32) -> i32 {
    let year = year - i32::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let month = month as i32;
    let day = day as i32;
    let day_of_year = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

struct PgOutputReader {
    data: Bytes,
    offset: usize,
}

impl PgOutputReader {
    fn new(data: Bytes) -> Self {
        Self { data, offset: 0 }
    }

    fn finish(&self) -> Result<()> {
        ensure!(
            self.remaining() == 0,
            "pgoutput message has {} trailing bytes",
            self.remaining()
        );
        Ok(())
    }

    fn expect_tag(&mut self, expected: u8, label: &str) -> Result<()> {
        let observed = self.take_u8()?;
        ensure!(
            observed == expected,
            "pgoutput {label} expected tag 0x{expected:02x}, got 0x{observed:02x}"
        );
        Ok(())
    }

    fn take_u8(&mut self) -> Result<u8> {
        let bytes = self.take_bytes(1)?;
        Ok(bytes[0])
    }

    fn take_u16(&mut self) -> Result<u16> {
        Ok(u16::from_be_bytes(self.take_array()?))
    }

    fn take_u32(&mut self) -> Result<u32> {
        Ok(u32::from_be_bytes(self.take_array()?))
    }

    fn take_i32(&mut self) -> Result<i32> {
        Ok(i32::from_be_bytes(self.take_array()?))
    }

    fn take_u64(&mut self) -> Result<u64> {
        Ok(u64::from_be_bytes(self.take_array()?))
    }

    fn take_len_bytes(&mut self) -> Result<Bytes> {
        let len = self.take_i32()?;
        ensure!(len >= 0, "pgoutput value length cannot be negative");
        self.take_bytes(len as usize)
    }

    fn take_cstring(&mut self) -> Result<String> {
        let Some(relative_end) = self.data[self.offset..].iter().position(|byte| *byte == 0) else {
            bail!("pgoutput string missing null terminator");
        };
        let start = self.offset;
        let end = self.offset + relative_end;
        self.offset = end + 1;
        str::from_utf8(&self.data[start..end])
            .context("decode pgoutput string")
            .map(str::to_string)
    }

    fn take_array<const N: usize>(&mut self) -> Result<[u8; N]> {
        let bytes = self.take_bytes(N)?;
        let mut out = [0_u8; N];
        out.copy_from_slice(bytes.as_ref());
        Ok(out)
    }

    fn take_bytes(&mut self, len: usize) -> Result<Bytes> {
        ensure!(
            self.remaining() >= len,
            "pgoutput message truncated: need {len} bytes, have {}",
            self.remaining()
        );
        let start = self.offset;
        let end = self.offset + len;
        self.offset = end;
        Ok(self.data.slice(start..end))
    }

    fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.offset)
    }
}

#[cfg(test)]
mod tests;
