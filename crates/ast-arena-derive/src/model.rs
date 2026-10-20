//! What the mirror of one owned type depends on, read from its definition
//! and its `serde` and `arena` attributes. Attributes the mirror cannot honour
//! exactly are rejected here, so a mirror never silently disagrees with the
//! owned type's serde contract.

use syn::{Attribute, Data, DeriveInput, Fields, Ident, LitStr, Token, Type, Visibility};

/// One owned type and the mirror the derive emits for it.
pub struct Item {
    pub ident: Ident,
    pub vis: Visibility,
    pub docs: Vec<Attribute>,
    /// The type writes its own `ArenaDeserialize` (for validation).
    pub manual_deserialize: bool,
    pub shape: Shape,
}

pub enum Shape {
    /// A struct with named fields.
    Struct(Vec<Field>),
    /// An externally tagged enum with at least one variant that holds data.
    Enum(Vec<Variant>),
    /// An enum of unit variants only, which is `Copy` and mirrors itself.
    Leaf,
}

pub struct Variant {
    pub ident: Ident,
    /// The tag on the wire, after `rename_all`.
    pub name: String,
    pub docs: Vec<Attribute>,
    pub fields: VariantFields,
}

pub enum VariantFields {
    Unit,
    Newtype(Box<Field>),
    Tuple(Vec<Field>),
    Struct(Vec<Field>),
}

pub struct Field {
    /// `None` for tuple and newtype fields.
    pub ident: Option<Ident>,
    /// The key on the wire, for named fields.
    pub name: String,
    pub vis: Visibility,
    pub docs: Vec<Attribute>,
    pub ty: Type,
    /// `#[serde(default)]`: a missing field is `Default::default()`.
    pub default: bool,
    pub mode: Mode,
}

/// How a field's mirror type relates to its owned type.
pub enum Mode {
    /// Strings, boxes, vectors and maps borrow from the arena, and every
    /// other type is replaced by its mirror.
    Mirror,
    /// `#[arena(copy)]`: like `Mirror`, except named types are kept as they
    /// are because they are already `Copy` (for example the `u64` alias
    /// `NodeId`).
    Copy,
    /// `#[arena(owned)]`: the field keeps its owned type and its own serde
    /// impl; the mirror holding it is then not `Copy`.
    Owned,
}

/// serde's `rename_all` rules that the request types use.
#[derive(Clone, Copy)]
enum RenameRule {
    None,
    SnakeCase,
    Lowercase,
}

impl RenameRule {
    /// serde's rule for variant names, which are PascalCase.
    fn variant(self, variant: &str) -> String {
        match self {
            Self::None => variant.to_owned(),
            Self::Lowercase => variant.to_ascii_lowercase(),
            Self::SnakeCase => {
                variant
                    .char_indices()
                    .fold(String::new(), |mut snake, (index, character)| {
                        if index > 0 && character.is_uppercase() {
                            snake.push('_');
                        }
                        snake.push(character.to_ascii_lowercase());
                        snake
                    })
            }
        }
    }
}

impl Item {
    pub fn parse(input: &DeriveInput) -> syn::Result<Self> {
        if !input.generics.params.is_empty() {
            return Err(syn::Error::new_spanned(
                &input.generics,
                "ArenaMirror does not support generic types",
            ));
        }
        let rename = container_rename(&input.attrs)?;
        let manual_deserialize = arena_flags(&input.attrs)?
            .iter()
            .try_fold(false, |_, flag| match flag.as_str() {
                "manual_deserialize" => Ok(true),
                other => Err(syn::Error::new_spanned(
                    &input.ident,
                    format!("unknown item arena attribute `{other}`"),
                )),
            })?;
        let shape = match &input.data {
            Data::Struct(data) => {
                let Fields::Named(fields) = &data.fields else {
                    return Err(syn::Error::new_spanned(
                        &input.ident,
                        "ArenaMirror supports structs with named fields only",
                    ));
                };
                Shape::Struct(
                    fields
                        .named
                        .iter()
                        .map(Field::parse)
                        .collect::<syn::Result<_>>()?,
                )
            }
            Data::Enum(data) => {
                let variants = data
                    .variants
                    .iter()
                    .map(|variant| Variant::parse(variant, rename))
                    .collect::<syn::Result<Vec<_>>>()?;
                match variants
                    .iter()
                    .all(|variant| matches!(variant.fields, VariantFields::Unit))
                {
                    true => Shape::Leaf,
                    false => Shape::Enum(variants),
                }
            }
            Data::Union(_) => {
                return Err(syn::Error::new_spanned(
                    &input.ident,
                    "ArenaMirror does not support unions",
                ));
            }
        };
        Ok(Self {
            ident: input.ident.clone(),
            vis: input.vis.clone(),
            docs: docs(&input.attrs),
            manual_deserialize,
            shape,
        })
    }
}

impl Variant {
    fn parse(variant: &syn::Variant, rename: RenameRule) -> syn::Result<Self> {
        variant
            .attrs
            .iter()
            .find(|attr| attr.path().is_ident("serde"))
            .map_or(Ok(()), |attr| {
                Err(syn::Error::new_spanned(
                    attr,
                    "ArenaMirror does not support serde attributes on variants",
                ))
            })?;
        let fields = match &variant.fields {
            Fields::Unit => VariantFields::Unit,
            Fields::Named(fields) => VariantFields::Struct(
                fields
                    .named
                    .iter()
                    .map(Field::parse)
                    .collect::<syn::Result<_>>()?,
            ),
            Fields::Unnamed(fields) => {
                let mut fields = fields
                    .unnamed
                    .iter()
                    .map(Field::parse)
                    .collect::<syn::Result<Vec<_>>>()?;
                match fields.len() {
                    1 => VariantFields::Newtype(Box::new(fields.remove(0))),
                    _ => VariantFields::Tuple(fields),
                }
            }
        };
        Ok(Self {
            ident: variant.ident.clone(),
            name: rename.variant(&variant.ident.to_string()),
            docs: docs(&variant.attrs),
            fields,
        })
    }
}

impl Field {
    fn parse(field: &syn::Field) -> syn::Result<Self> {
        let mut default = false;
        field
            .attrs
            .iter()
            .filter(|attr| attr.path().is_ident("serde"))
            .try_for_each(|attr| {
                attr.parse_nested_meta(|meta| {
                    if meta.path.is_ident("default") {
                        if meta.input.peek(Token![=]) {
                            return Err(meta.error("ArenaMirror supports a bare `default` only"));
                        }
                        default = true;
                        Ok(())
                    } else if meta.path.is_ident("skip_serializing_if") {
                        // Serialization only.
                        meta.value()?.parse::<LitStr>().map(|_| ())
                    } else {
                        Err(meta.error("ArenaMirror does not support this serde field attribute"))
                    }
                })
            })?;
        let mode =
            arena_flags(&field.attrs)?
                .iter()
                .try_fold(Mode::Mirror, |_, flag| match flag.as_str() {
                    "copy" => Ok(Mode::Copy),
                    "owned" => Ok(Mode::Owned),
                    other => Err(syn::Error::new_spanned(
                        &field.ty,
                        format!("unknown field arena attribute `{other}`"),
                    )),
                })?;
        Ok(Self {
            ident: field.ident.clone(),
            // Field names are snake_case already, which every supported
            // `rename_all` rule keeps as they are.
            name: field
                .ident
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_default(),
            vis: field.vis.clone(),
            docs: docs(&field.attrs),
            ty: field.ty.clone(),
            default,
            mode,
        })
    }
}

fn docs(attrs: &[Attribute]) -> Vec<Attribute> {
    attrs
        .iter()
        .filter(|attr| attr.path().is_ident("doc"))
        .cloned()
        .collect()
}

fn container_rename(attrs: &[Attribute]) -> syn::Result<RenameRule> {
    let mut rule = RenameRule::None;
    attrs
        .iter()
        .filter(|attr| attr.path().is_ident("serde"))
        .try_for_each(|attr| {
            attr.parse_nested_meta(|meta| {
                if !meta.path.is_ident("rename_all") {
                    return Err(meta.error("ArenaMirror does not support this serde attribute"));
                }
                let value = meta.value()?.parse::<LitStr>()?;
                rule = match value.value().as_str() {
                    "snake_case" => RenameRule::SnakeCase,
                    "lowercase" => RenameRule::Lowercase,
                    other => {
                        return Err(meta.error(format!(
                            "ArenaMirror does not support rename_all = \"{other}\""
                        )));
                    }
                };
                Ok(())
            })
        })?;
    Ok(rule)
}

fn arena_flags(attrs: &[Attribute]) -> syn::Result<Vec<String>> {
    let mut flags = Vec::new();
    attrs
        .iter()
        .filter(|attr| attr.path().is_ident("arena"))
        .try_for_each(|attr| {
            attr.parse_nested_meta(|meta| {
                let Some(flag) = meta.path.get_ident() else {
                    return Err(meta.error("expected an arena flag"));
                };
                flags.push(flag.to_string());
                Ok(())
            })
        })?;
    Ok(flags)
}

#[cfg(test)]
mod tests {
    use super::RenameRule;

    #[test]
    fn rename_rules_match_serde() {
        [
            ("F32Array", "f32_array"),
            ("OutE", "out_e"),
            ("DateTimeNow", "date_time_now"),
            ("VectorSearchNodesWithin", "vector_search_nodes_within"),
            ("Id", "id"),
        ]
        .into_iter()
        .for_each(|(variant, wire)| assert_eq!(RenameRule::SnakeCase.variant(variant), wire));
        assert_eq!(RenameRule::Lowercase.variant("ReadOnly"), "readonly");
        assert_eq!(RenameRule::None.variant("Read"), "Read");
    }
}
