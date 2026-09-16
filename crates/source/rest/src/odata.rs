//! OData `$metadata` (EDMX / CSDL) parsing → dataset discovery (#512).
//!
//! Pure, network-free: [`parse_edmx`] turns a `$metadata` XML document into the
//! entity types + sets it declares, and [`descriptors_from_edmx`] maps those
//! onto [`DatasetDescriptor`]s (one per entity set, each with a typed schema and
//! a `config_patch` that selects the entity).

use faucet_core::FaucetError;
use faucet_core::discover::{DatasetDescriptor, columns_to_schema, nullable_type};
use quick_xml::Reader;
use quick_xml::events::{BytesStart, Event};
use serde_json::{Value, json};
use std::collections::HashMap;

/// One EDM property (column) of an entity type.
#[derive(Debug, Clone, PartialEq)]
pub struct EdmProperty {
    /// Property name.
    pub name: String,
    /// EDM type (e.g. `Edm.String`, `Edm.Int32`).
    pub edm_type: String,
    /// Whether the property may be null (EDM default is `true`).
    pub nullable: bool,
}

/// An EDM entity type: its key columns + properties.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct EdmEntityType {
    /// Type name (local, namespace-stripped).
    pub name: String,
    /// Key property names.
    pub keys: Vec<String>,
    /// Properties in declaration order.
    pub properties: Vec<EdmProperty>,
}

/// An EDM entity set: the queryable collection name + the type it holds.
#[derive(Debug, Clone, PartialEq)]
pub struct EdmEntitySet {
    /// Entity-set name (the path segment you query).
    pub name: String,
    /// Local name of the entity type backing this set.
    pub type_name: String,
}

/// Map an EDM primitive type to a JSON-Schema type fragment.
pub fn edm_type_to_json(edm_type: &str) -> Value {
    let t = edm_type.strip_prefix("Edm.").unwrap_or(edm_type);
    match t {
        "Boolean" => json!({ "type": "boolean" }),
        "Byte" | "SByte" | "Int16" | "Int32" | "Int64" => json!({ "type": "integer" }),
        "Decimal" | "Double" | "Single" => json!({ "type": "number" }),
        // Temporal EDM types serialize as RFC3339 JSON strings but carry a real
        // date/time type — tag the format so a typed sink (BigQuery) declares
        // TIMESTAMP/DATE and coerces the string on load, rather than STRING.
        "DateTimeOffset" => json!({ "type": "string", "format": "date-time" }),
        "Date" => json!({ "type": "string", "format": "date" }),
        // Guid, TimeOfDay, Duration, Binary, Stream, Geography*, … stay strings.
        _ => json!({ "type": "string" }),
    }
}

/// Namespace-strip a possibly-prefixed XML name (`edm:Property` → `Property`).
fn local_name(qname: &[u8]) -> String {
    let s = String::from_utf8_lossy(qname);
    s.rsplit(':').next().unwrap_or(&s).to_string()
}

/// Read one attribute of an element by (namespace-stripped) name.
fn attr(e: &BytesStart, key: &str) -> Option<String> {
    e.attributes()
        .flatten()
        .find(|a| local_name(a.key.as_ref()) == key)
        .and_then(|a| a.unescape_value().ok().map(|v| v.to_string()))
}

/// Parse an EDMX / CSDL `$metadata` document into its entity types + sets.
pub fn parse_edmx(xml: &str) -> Result<(Vec<EdmEntityType>, Vec<EdmEntitySet>), FaucetError> {
    let mut reader = Reader::from_str(xml);
    let mut types: Vec<EdmEntityType> = Vec::new();
    let mut sets: Vec<EdmEntitySet> = Vec::new();
    let mut current: Option<EdmEntityType> = None;
    let mut in_key = false;

    // Handle a start/empty element's attributes; `is_empty` self-closing tags
    // (`<Property .../>`, `<EntitySet .../>`) never get a matching `End`.
    let open = |e: &BytesStart,
                is_empty: bool,
                current: &mut Option<EdmEntityType>,
                in_key: &mut bool,
                types: &mut Vec<EdmEntityType>,
                sets: &mut Vec<EdmEntitySet>| {
        match local_name(e.name().as_ref()).as_str() {
            "EntityType" => {
                let t = EdmEntityType {
                    name: attr(e, "Name").unwrap_or_default(),
                    ..Default::default()
                };
                if is_empty {
                    types.push(t);
                } else {
                    *current = Some(t);
                }
            }
            "Key" => {
                if !is_empty {
                    *in_key = true;
                }
            }
            "PropertyRef" => {
                if *in_key && let (Some(cur), Some(n)) = (current.as_mut(), attr(e, "Name")) {
                    cur.keys.push(n);
                }
            }
            "Property" => {
                if let Some(cur) = current.as_mut() {
                    let name = attr(e, "Name").unwrap_or_default();
                    if !name.is_empty() {
                        cur.properties.push(EdmProperty {
                            name,
                            edm_type: attr(e, "Type").unwrap_or_else(|| "Edm.String".to_owned()),
                            // EDM `Nullable` defaults to true when absent.
                            nullable: attr(e, "Nullable").map(|v| v != "false").unwrap_or(true),
                        });
                    }
                }
            }
            "EntitySet" => {
                if let (Some(name), Some(ty)) = (attr(e, "Name"), attr(e, "EntityType")) {
                    let type_name = ty.rsplit('.').next().unwrap_or(&ty).to_owned();
                    sets.push(EdmEntitySet { name, type_name });
                }
            }
            _ => {}
        }
    };

    loop {
        match reader
            .read_event()
            .map_err(|e| FaucetError::Source(format!("odata: invalid $metadata XML: {e}")))?
        {
            Event::Eof => break,
            Event::Start(e) => open(&e, false, &mut current, &mut in_key, &mut types, &mut sets),
            Event::Empty(e) => open(&e, true, &mut current, &mut in_key, &mut types, &mut sets),
            Event::End(e) => match local_name(e.name().as_ref()).as_str() {
                "EntityType" => {
                    if let Some(t) = current.take() {
                        types.push(t);
                    }
                }
                "Key" => in_key = false,
                _ => {}
            },
            _ => {}
        }
    }
    Ok((types, sets))
}

/// Parse `$metadata` and produce one [`DatasetDescriptor`] per entity set,
/// each carrying a typed schema and a `config_patch` selecting the entity.
pub fn descriptors_from_edmx(xml: &str) -> Result<Vec<DatasetDescriptor>, FaucetError> {
    let (types, sets) = parse_edmx(xml)?;
    let by_name: HashMap<&str, &EdmEntityType> =
        types.iter().map(|t| (t.name.as_str(), t)).collect();
    let mut out = Vec::with_capacity(sets.len());
    for set in &sets {
        let schema = by_name.get(set.type_name.as_str()).map(|t| {
            let cols = t.properties.iter().map(|p| {
                let frag = edm_type_to_json(&p.edm_type);
                let frag = if p.nullable {
                    nullable_type(frag)
                } else {
                    frag
                };
                (p.name.clone(), frag)
            });
            columns_to_schema(cols)
        });
        let mut d = DatasetDescriptor::new(
            set.name.clone(),
            "entity",
            json!({ "odata": { "entity": set.name.clone() } }),
        );
        if let Some(s) = schema {
            d = d.with_schema(s);
        }
        out.push(d);
    }
    Ok(out)
}

/// Build one [`DatasetDescriptor`] per entity set from an **explicit** object
/// list (the run-time fan-out path, #647-family). Unlike
/// [`descriptors_from_edmx`], this needs no `$metadata` round-trip — OData
/// `/data/<EntitySet>` returns every column by default, so there is no field
/// list to discover. Each descriptor selects the entity and renders its sink
/// `table_id` from `table_template` (`${name}`/`${name_snake}`/`${name_lower}`,
/// e.g. `"raw_${name_snake}"`).
pub fn descriptors_from_objects(
    objects: &[String],
    table_template: &str,
) -> Vec<DatasetDescriptor> {
    objects
        .iter()
        .map(|entity| {
            let table_id = crate::discovery::render_template(table_template, entity, &[]);
            DatasetDescriptor::new(
                entity.clone(),
                "entity",
                json!({ "odata": { "entity": entity } }),
            )
            .with_sink_patch(json!({ "table_id": table_id }))
        })
        .collect()
}

/// Like [`descriptors_from_objects`] but **typed**: parse `$metadata` and attach
/// each requested entity's column schema (from its EDM entity type) to the sink
/// patch as `schema`, so a table-based sink (BigQuery) can declare authoritative
/// column types instead of autodetecting them — the fix for a load job that
/// autodetects a column and then fails on one non-conforming row.
///
/// **Schema property names are kept verbatim** (raw EDM PascalCase) so the declared
/// schema lines up with the untransformed record keys that reach the sink, and with
/// the temporal `format` hints so a typed sink declares TIMESTAMP/DATE. An entity
/// absent from `$metadata` (or with no declared type) still gets a `table_id`-only
/// patch (autodetect fallback for that one).
pub fn descriptors_from_edmx_for_objects(
    xml: &str,
    objects: &[String],
    table_template: &str,
) -> Result<Vec<DatasetDescriptor>, FaucetError> {
    let (types, sets) = parse_edmx(xml)?;
    let set_by_name: HashMap<&str, &EdmEntitySet> =
        sets.iter().map(|s| (s.name.as_str(), s)).collect();
    let type_by_name: HashMap<&str, &EdmEntityType> =
        types.iter().map(|t| (t.name.as_str(), t)).collect();

    let mut out = Vec::with_capacity(objects.len());
    for obj in objects {
        let table_id = crate::discovery::render_template(table_template, obj, &[]);
        let mut sink_patch = json!({ "table_id": table_id });
        if let Some(set) = set_by_name.get(obj.as_str())
            && let Some(t) = type_by_name.get(set.type_name.as_str())
            && !t.properties.is_empty()
        {
            let cols = t.properties.iter().map(|p| {
                let frag = edm_type_to_json(&p.edm_type);
                let frag = if p.nullable {
                    nullable_type(frag)
                } else {
                    frag
                };
                // Raw EDM property name (PascalCase) — the records reach the sink
                // untransformed, so the declared schema keys must match verbatim.
                (p.name.clone(), frag)
            });
            sink_patch["schema"] = columns_to_schema(cols);
        }
        out.push(
            DatasetDescriptor::new(obj.clone(), "entity", json!({ "odata": { "entity": obj } }))
                .with_sink_patch(sink_patch),
        );
    }
    Ok(out)
}

/// The single **integer** primary key of an entity set (for key-range
/// partitioning), or `None` if `$metadata` lacks the set, the type has no single
/// key, or that key is not an integer EDM type. Mirrors the reference tap's rule:
/// exactly one integer key to range-split on; anything else extracts sequentially.
pub fn single_int_key_from_edmx(xml: &str, entity: &str) -> Option<String> {
    let (types, sets) = parse_edmx(xml).ok()?;
    let set = sets.iter().find(|s| s.name == entity)?;
    let ty = types.iter().find(|t| t.name == set.type_name)?;
    if ty.keys.len() != 1 {
        return None;
    }
    let key = &ty.keys[0];
    let is_int = ty
        .properties
        .iter()
        .find(|p| &p.name == key)
        .is_some_and(|p| {
            matches!(
                p.edm_type.strip_prefix("Edm.").unwrap_or(&p.edm_type),
                "Byte" | "SByte" | "Int16" | "Int32" | "Int64"
            )
        });
    is_int.then(|| key.clone())
}

/// Render one planned key range as an OData `$filter` clause. Range planning is
/// [`faucet_core::shard::plan_pk_shards`] (the same primitive the SQL sources
/// shard with — i128 width math, so full-range i64 keys cannot overflow); this
/// renders its [`PkShardBounds`](faucet_core::shard::PkShardBounds) in OData
/// syntax. The first/last ranges are **unbounded** on their outer edge, so rows
/// inserted below MIN / above MAX while the run is in flight are still read.
/// Returns `None` for a single whole-range shard (no filter needed).
pub fn key_range_filter(bounds: &faucet_core::shard::PkShardBounds) -> Option<String> {
    let key = &bounds.key;
    match (bounds.lo_unbounded, bounds.hi_unbounded) {
        (true, true) => None,
        (true, false) => Some(format!("{key} lt {}", bounds.hi)),
        (false, true) => Some(format!("{key} ge {}", bounds.lo)),
        (false, false) => Some(format!("{key} ge {} and {key} lt {}", bounds.lo, bounds.hi)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="Sales" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Order">
        <Key><PropertyRef Name="DocEntry"/></Key>
        <Property Name="DocEntry" Type="Edm.Int32" Nullable="false"/>
        <Property Name="DocDate" Type="Edm.DateTimeOffset"/>
        <Property Name="Total" Type="Edm.Decimal" Nullable="false"/>
        <Property Name="Posted" Type="Edm.Boolean"/>
      </EntityType>
      <EntityContainer Name="Container">
        <EntitySet Name="Orders" EntityType="Sales.Order"/>
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>"#;

    #[test]
    fn edm_types_map_to_json_types() {
        assert_eq!(edm_type_to_json("Edm.Boolean"), json!({"type": "boolean"}));
        assert_eq!(edm_type_to_json("Edm.Int64"), json!({"type": "integer"}));
        assert_eq!(edm_type_to_json("Edm.Decimal"), json!({"type": "number"}));
        assert_eq!(edm_type_to_json("Edm.String"), json!({"type": "string"}));
        // Temporal EDM types carry a format hint so typed sinks declare
        // TIMESTAMP/DATE instead of STRING.
        assert_eq!(
            edm_type_to_json("Edm.DateTimeOffset"),
            json!({"type": "string", "format": "date-time"})
        );
        assert_eq!(
            edm_type_to_json("Edm.Date"),
            json!({"type": "string", "format": "date"})
        );
        assert_eq!(
            edm_type_to_json("Something.Custom"),
            json!({"type": "string"})
        );
    }

    #[test]
    fn parse_edmx_extracts_types_and_sets() {
        let (types, sets) = parse_edmx(SAMPLE).unwrap();
        assert_eq!(types.len(), 1);
        assert_eq!(types[0].name, "Order");
        assert_eq!(types[0].keys, vec!["DocEntry".to_string()]);
        assert_eq!(types[0].properties.len(), 4);
        assert_eq!(types[0].properties[0].name, "DocEntry");
        assert!(!types[0].properties[0].nullable);
        assert!(types[0].properties[1].nullable); // DocDate has no Nullable attr
        assert_eq!(sets.len(), 1);
        assert_eq!(sets[0].name, "Orders");
        assert_eq!(sets[0].type_name, "Order");
    }

    #[test]
    fn descriptors_carry_schema_and_config_patch() {
        let ds = descriptors_from_edmx(SAMPLE).unwrap();
        assert_eq!(ds.len(), 1);
        let d = &ds[0];
        assert_eq!(d.name, "Orders");
        assert_eq!(d.kind, "entity");
        assert_eq!(d.config_patch, json!({ "odata": { "entity": "Orders" } }));
        let schema = d.schema.as_ref().unwrap();
        assert_eq!(schema["type"], "object");
        assert_eq!(schema["properties"]["DocEntry"]["type"], "integer");
        assert_eq!(schema["properties"]["Total"]["type"], "number");
        assert_eq!(schema["properties"]["Posted"]["type"][0], "boolean");
        assert_eq!(schema["properties"]["Posted"]["type"][1], "null");
    }

    #[test]
    fn descriptors_from_objects_select_entity_and_route_sink() {
        let objs = vec![
            "MainAccountBiEntities".to_string(),
            "CustomersV3".to_string(),
        ];
        let ds = descriptors_from_objects(&objs, "fno_${name_snake}");
        assert_eq!(ds.len(), 2);
        assert_eq!(ds[0].name, "MainAccountBiEntities");
        assert_eq!(ds[0].kind, "entity");
        assert_eq!(
            ds[0].config_patch,
            json!({ "odata": { "entity": "MainAccountBiEntities" } })
        );
        assert_eq!(
            ds[0].sink_patch,
            Some(json!({ "table_id": "fno_main_account_bi_entities" }))
        );
        // The template applies to every entity; CamelCase+digit snake-cases correctly.
        assert_eq!(
            ds[1].sink_patch,
            Some(json!({ "table_id": "fno_customers_v3" }))
        );
    }

    #[test]
    fn descriptors_from_objects_default_template_is_bare_snake() {
        let ds = descriptors_from_objects(&["Departments".to_string()], "${name_snake}");
        assert_eq!(ds[0].sink_patch, Some(json!({ "table_id": "departments" })));
        // Verbatim and lowercase template variables render too.
        let ds = descriptors_from_objects(&["CustomersV3".to_string()], "raw_${name_lower}");
        assert_eq!(
            ds[0].sink_patch,
            Some(json!({ "table_id": "raw_customersv3" }))
        );
    }

    #[test]
    fn edmx_for_objects_attaches_verbatim_typed_schema() {
        let ds =
            descriptors_from_edmx_for_objects(SAMPLE, &["Orders".to_string()], "fno_${name_snake}")
                .unwrap();
        assert_eq!(ds.len(), 1);
        let patch = ds[0].sink_patch.as_ref().unwrap();
        assert_eq!(patch["table_id"], "fno_orders");
        let schema = &patch["schema"];
        // Column names kept verbatim (raw EDM PascalCase) so the declared schema
        // matches the untransformed record keys; EDM types mapped, temporal
        // types tagged with a format hint.
        assert_eq!(schema["properties"]["DocEntry"]["type"], "integer"); // non-null Int32
        assert_eq!(schema["properties"]["Total"]["type"], "number"); // non-null Decimal
        assert_eq!(schema["properties"]["DocDate"]["type"][0], "string"); // nullable DateTimeOffset
        assert_eq!(schema["properties"]["DocDate"]["format"], "date-time");
        assert_eq!(schema["properties"]["Posted"]["type"][0], "boolean"); // nullable Boolean
    }

    #[test]
    fn edmx_for_objects_unknown_entity_gets_table_id_only() {
        let ds = descriptors_from_edmx_for_objects(
            SAMPLE,
            &["NotInMetadata".to_string()],
            "fno_${name_snake}",
        )
        .unwrap();
        assert_eq!(
            ds[0].sink_patch,
            Some(json!({ "table_id": "fno_not_in_metadata" }))
        );
    }

    #[test]
    fn empty_entity_type_and_missing_type_are_tolerated() {
        let xml = r#"<Schema>
            <EntityType Name="Empty"/>
            <EntitySet Name="Ghosts" EntityType="ns.NotDeclared"/>
        </Schema>"#;
        let ds = descriptors_from_edmx(xml).unwrap();
        assert_eq!(ds.len(), 1);
        // No matching type → no schema, but the set is still discoverable.
        assert!(ds[0].schema.is_none());
        assert_eq!(ds[0].name, "Ghosts");
    }

    #[test]
    fn invalid_xml_errors() {
        assert!(parse_edmx("<Schema><EntityType Name=").is_err());
    }

    #[test]
    fn property_without_name_is_skipped() {
        // A `<Property>` with no `Name` is skipped (not added as an empty-named
        // column); an explicit `Nullable="true"` is honoured.
        let xml = r#"<Schema>
          <EntityType Name="T">
            <Property Type="Edm.String"/>
            <Property Name="ok" Type="Edm.String" Nullable="true"/>
          </EntityType>
        </Schema>"#;
        let (types, _sets) = parse_edmx(xml).unwrap();
        assert_eq!(types[0].properties.len(), 1);
        assert_eq!(types[0].properties[0].name, "ok");
        assert!(types[0].properties[0].nullable);
    }

    #[test]
    fn key_range_filter_renders_all_bound_combinations() {
        use faucet_core::shard::PkShardBounds;
        let b = |lo, hi, lo_u, hi_u| PkShardBounds {
            key: "SourceKey".into(),
            lo,
            hi,
            lo_unbounded: lo_u,
            hi_unbounded: hi_u,
            // Irrelevant for OData: an EDM entity key is non-nullable by
            // definition, so there are no NULL-key rows to include.
            include_null: false,
        };
        // Middle range: half-open [lo, hi).
        assert_eq!(
            key_range_filter(&b(10, 20, false, false)).as_deref(),
            Some("SourceKey ge 10 and SourceKey lt 20")
        );
        // First range: no lower bound — rows below the enumerated MIN are read.
        assert_eq!(
            key_range_filter(&b(0, 20, true, false)).as_deref(),
            Some("SourceKey lt 20")
        );
        // Last range: no upper bound — rows above the enumerated MAX are read.
        assert_eq!(
            key_range_filter(&b(10, 0, false, true)).as_deref(),
            Some("SourceKey ge 10")
        );
        // Single whole-range shard: no filter at all.
        assert_eq!(key_range_filter(&b(0, 0, true, true)), None);
    }

    #[test]
    fn range_planning_delegates_to_core_and_survives_extreme_keys() {
        use faucet_core::shard::{PkShardBounds, plan_pk_shards};
        // The very case a hand-rolled `hi - lo` i64 subtraction overflows on:
        // a full-range 64-bit key. Planning + rendering must not panic.
        let shards = plan_pk_shards("k", i64::MIN, i64::MAX, 8);
        assert!(!shards.is_empty());
        let bounds: Vec<_> = shards.iter().filter_map(PkShardBounds::from_spec).collect();
        assert_eq!(bounds.len(), shards.len());
        // Outer edges are unbounded; every range renders (only a single
        // whole-range shard would render None).
        assert!(bounds.first().unwrap().lo_unbounded);
        assert!(bounds.last().unwrap().hi_unbounded);
        assert!(
            bounds
                .iter()
                .all(|b| key_range_filter(b).is_some() || bounds.len() == 1)
        );
    }

    #[test]
    fn single_int_key_detection() {
        // SAMPLE's Order has a single Int32 key (DocEntry) → partitionable.
        assert_eq!(
            single_int_key_from_edmx(SAMPLE, "Orders").as_deref(),
            Some("DocEntry")
        );
        // Unknown entity set → None.
        assert_eq!(single_int_key_from_edmx(SAMPLE, "Nope"), None);
        // Composite key → None (range-splitting needs exactly one column).
        let composite = r#"<Schema>
            <EntityType Name="Pair">
              <Key><PropertyRef Name="A"/><PropertyRef Name="B"/></Key>
              <Property Name="A" Type="Edm.Int64" Nullable="false"/>
              <Property Name="B" Type="Edm.Int64" Nullable="false"/>
            </EntityType>
            <EntityContainer Name="C"><EntitySet Name="Pairs" EntityType="ns.Pair"/></EntityContainer>
        </Schema>"#;
        assert_eq!(single_int_key_from_edmx(composite, "Pairs"), None);
        // Single but non-integer key → None (a string key cannot range-split).
        let strkey = r#"<Schema>
            <EntityType Name="Doc">
              <Key><PropertyRef Name="Id"/></Key>
              <Property Name="Id" Type="Edm.String" Nullable="false"/>
            </EntityType>
            <EntityContainer Name="C"><EntitySet Name="Docs" EntityType="ns.Doc"/></EntityContainer>
        </Schema>"#;
        assert_eq!(single_int_key_from_edmx(strkey, "Docs"), None);
    }
}
