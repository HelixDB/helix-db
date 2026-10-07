//! Code for one mirror: its type, its `ArenaDeserialize` impl and its
//! `IntoOwned` impl.
//!
//! The deserializer is what serde_derive emits for the owned type, with every
//! nested read going through a `crate::arena::Seed` so it allocates in the
//! arena: the same `deserialize_struct`/`deserialize_enum` calls, the same
//! sequence and map forms, defaults, `None` for missing options, duplicate,
//! missing and unknown handling, and the same expecting strings, so the two
//! accept the same inputs and report the same errors.

use proc_macro2::{Span, TokenStream};
use quote::{format_ident, quote};
use syn::{Ident, LitByteStr};

use crate::mirror::{is_option, mirror_type};
use crate::model::{Field, Item, Mode, Shape, VariantFields};

pub fn expand(item: &Item) -> syn::Result<TokenStream> {
    let owned = &item.ident;
    let mirror = format_ident!("Arena{}", owned);
    let vis = &item.vis;
    let (definition, deserialize, into_owned) = match &item.shape {
        Shape::Leaf => {
            let doc =
                format!("[`{owned}`] is `Copy` and owns no heap, so it is its own arena mirror.");
            return Ok(quote! {
                #[doc = #doc]
                #vis type #mirror<'a> = #owned;

                #[automatically_derived]
                impl<'a> crate::arena::ArenaDeserialize<'a> for #owned {
                    fn deserialize_in<'de, D: ::serde::Deserializer<'de>>(
                        _bump: &'a crate::arena::Bump,
                        deserializer: D,
                    ) -> ::core::result::Result<Self, D::Error> {
                        <Self as ::serde::Deserialize<'de>>::deserialize(deserializer)
                    }
                }

                #[automatically_derived]
                impl crate::arena::IntoOwned<#owned> for #owned {
                    fn into_owned(self) -> #owned {
                        self
                    }
                }
            });
        }
        Shape::Struct(fields) => (
            struct_definition(&mirror, vis, fields)?,
            struct_deserialize(owned, &mirror, fields)?,
            struct_into_owned(owned, fields),
        ),
        Shape::Enum(variants) => (
            enum_definition(&mirror, vis, variants)?,
            enum_deserialize(owned, &mirror, variants)?,
            enum_into_owned(owned, &mirror, variants),
        ),
    };
    let fields = match &item.shape {
        Shape::Struct(fields) => fields.iter().collect::<Vec<_>>(),
        Shape::Enum(variants) => variants
            .iter()
            .flat_map(|variant| variant_fields(&variant.fields))
            .collect(),
        Shape::Leaf => Vec::new(),
    };
    // A mirror holding an owned field cannot be `Copy`; it is only ever a
    // value on the stack, never allocated in the arena.
    let derives = match fields.iter().any(|field| matches!(field.mode, Mode::Owned)) {
        true => TokenStream::new(),
        false => quote!(#[derive(Debug, Clone, Copy, PartialEq)]),
    };
    let header = format!(
        "Arena-backed mirror of [`{owned}`]: `Copy`, with its strings, boxes, vectors and \
         maps borrowed from a [`crate::arena::Bump`]. See [`crate::arena`]."
    );
    let docs = &item.docs;
    let deserialize = match item.manual_deserialize {
        true => TokenStream::new(),
        false => deserialize,
    };
    Ok(quote! {
        #[doc = #header]
        #[doc = ""]
        #(#docs)*
        #derives
        #definition

        #deserialize

        #[automatically_derived]
        impl<'a> crate::arena::IntoOwned<#owned> for #mirror<'a> {
            fn into_owned(self) -> #owned {
                #into_owned
            }
        }
    })
}

fn variant_fields(fields: &VariantFields) -> Vec<&Field> {
    match fields {
        VariantFields::Unit => Vec::new(),
        VariantFields::Newtype(field) => vec![field],
        VariantFields::Tuple(fields) | VariantFields::Struct(fields) => fields.iter().collect(),
    }
}

fn struct_definition(
    mirror: &Ident,
    vis: &syn::Visibility,
    fields: &[Field],
) -> syn::Result<TokenStream> {
    let fields = fields
        .iter()
        .map(|field| {
            let Field {
                ident,
                vis,
                docs,
                ty,
                mode,
                ..
            } = field;
            let ty = mirror_type(ty, mode)?;
            Ok(quote!(#(#docs)* #vis #ident: #ty))
        })
        .collect::<syn::Result<Vec<_>>>()?;
    Ok(quote!(#vis struct #mirror<'a> { #(#fields,)* }))
}

fn enum_definition(
    mirror: &Ident,
    vis: &syn::Visibility,
    variants: &[crate::model::Variant],
) -> syn::Result<TokenStream> {
    let variants = variants
        .iter()
        .map(|variant| {
            let ident = &variant.ident;
            let docs = &variant.docs;
            let body = match &variant.fields {
                VariantFields::Unit => TokenStream::new(),
                VariantFields::Newtype(field) => {
                    let ty = mirror_type(&field.ty, &field.mode)?;
                    quote!((#ty))
                }
                VariantFields::Tuple(fields) => {
                    let types = fields
                        .iter()
                        .map(|field| mirror_type(&field.ty, &field.mode))
                        .collect::<syn::Result<Vec<_>>>()?;
                    quote!((#(#types),*))
                }
                VariantFields::Struct(fields) => {
                    let fields = fields
                        .iter()
                        .map(|field| {
                            let ident = &field.ident;
                            let docs = &field.docs;
                            let ty = mirror_type(&field.ty, &field.mode)?;
                            Ok(quote!(#(#docs)* #ident: #ty))
                        })
                        .collect::<syn::Result<Vec<_>>>()?;
                    quote!({ #(#fields,)* })
                }
            };
            Ok(quote!(#(#docs)* #ident #body))
        })
        .collect::<syn::Result<Vec<_>>>()?;
    Ok(quote!(#vis enum #mirror<'a> { #(#variants,)* }))
}

/// Where a visitor reads a field's value from.
enum Access {
    /// The next element of `seq`, for the sequence form.
    Seq,
    /// The next value of `map`, for the map form.
    Map,
}

/// Read `field`'s value, whose mirror type is `ty`, from `access`.
fn read(field: &Field, ty: &TokenStream, access: Access) -> TokenStream {
    match (&field.mode, access) {
        (Mode::Owned, Access::Seq) => {
            quote!(::serde::de::SeqAccess::next_element::<#ty>(&mut seq)?)
        }
        (Mode::Owned, Access::Map) => quote!(::serde::de::MapAccess::next_value::<#ty>(&mut map)?),
        (Mode::Mirror | Mode::Copy, Access::Seq) => quote! {
            ::serde::de::SeqAccess::next_element_seed(
                &mut seq,
                crate::arena::Seed::<#ty>::new(self.bump),
            )?
        },
        (Mode::Mirror | Mode::Copy, Access::Map) => quote! {
            ::serde::de::MapAccess::next_value_seed(
                &mut map,
                crate::arena::Seed::<#ty>::new(self.bump),
            )?
        },
    }
}

/// The items serde_derive emits to read a struct or struct variant: its
/// field identifier and a visitor named `visitor` building `constructor`.
fn struct_visitor(
    visitor: &Ident,
    constructor: &TokenStream,
    value: &TokenStream,
    expecting: &str,
    fields: &[Field],
) -> syn::Result<TokenStream> {
    let length = format!("{expecting} with {} elements", fields.len());
    let types = fields
        .iter()
        .map(|field| mirror_type(&field.ty, &field.mode))
        .collect::<syn::Result<Vec<_>>>()?;
    let slots = (0..fields.len())
        .map(|index| format_ident!("__field{index}"))
        .collect::<Vec<_>>();
    let idents = fields.iter().map(|field| &field.ident).collect::<Vec<_>>();
    let names = fields
        .iter()
        .map(|field| field.name.as_str())
        .collect::<Vec<_>>();
    let bytes = names
        .iter()
        .map(|name| LitByteStr::new(name.as_bytes(), Span::call_site()))
        .collect::<Vec<_>>();
    let indices = (0..fields.len() as u64).collect::<Vec<_>>();
    let seq_reads = fields
        .iter()
        .zip(&types)
        .zip(&slots)
        .enumerate()
        .map(|(index, ((field, ty), slot))| {
            let element = read(field, ty, Access::Seq);
            let missing = match field.default {
                true => quote!(::core::default::Default::default()),
                false => quote! {
                    return ::core::result::Result::Err(
                        ::serde::de::Error::invalid_length(#index, &#length),
                    )
                },
            };
            quote! {
                let #slot = match #element {
                    ::core::option::Option::Some(value) => value,
                    ::core::option::Option::None => #missing,
                };
            }
        })
        .collect::<Vec<_>>();
    let map_arms = fields
        .iter()
        .zip(&types)
        .zip(&slots)
        .map(|((field, ty), slot)| {
            let name = &field.name;
            let value = read(field, ty, Access::Map);
            quote! {
                __Field::#slot => {
                    if #slot.is_some() {
                        return ::core::result::Result::Err(
                            <__A::Error as ::serde::de::Error>::duplicate_field(#name),
                        );
                    }
                    #slot = ::core::option::Option::Some(#value);
                }
            }
        })
        .collect::<Vec<_>>();
    let map_missing = fields
        .iter()
        .map(|field| {
            let name = &field.name;
            match (field.default, is_option(&field.ty)) {
                (true, _) => quote!(::core::default::Default::default()),
                (false, true) => quote!(::core::option::Option::None),
                (false, false) => quote! {
                    return ::core::result::Result::Err(
                        <__A::Error as ::serde::de::Error>::missing_field(#name),
                    )
                },
            }
        })
        .collect::<Vec<_>>();
    Ok(quote! {
        #[allow(non_camel_case_types)]
        enum __Field {
            #(#slots,)*
            __ignore,
        }

        struct __FieldVisitor;

        impl<'de> ::serde::de::Visitor<'de> for __FieldVisitor {
            type Value = __Field;

            fn expecting(&self, formatter: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                formatter.write_str("field identifier")
            }

            fn visit_u64<__E: ::serde::de::Error>(self, value: u64) -> ::core::result::Result<__Field, __E> {
                ::core::result::Result::Ok(match value {
                    #(#indices => __Field::#slots,)*
                    _ => __Field::__ignore,
                })
            }

            fn visit_str<__E: ::serde::de::Error>(self, value: &str) -> ::core::result::Result<__Field, __E> {
                ::core::result::Result::Ok(match value {
                    #(#names => __Field::#slots,)*
                    _ => __Field::__ignore,
                })
            }

            fn visit_bytes<__E: ::serde::de::Error>(self, value: &[u8]) -> ::core::result::Result<__Field, __E> {
                ::core::result::Result::Ok(match value {
                    #(#bytes => __Field::#slots,)*
                    _ => __Field::__ignore,
                })
            }
        }

        impl<'de> ::serde::Deserialize<'de> for __Field {
            fn deserialize<__D: ::serde::Deserializer<'de>>(
                deserializer: __D,
            ) -> ::core::result::Result<Self, __D::Error> {
                ::serde::Deserializer::deserialize_identifier(deserializer, __FieldVisitor)
            }
        }

        struct #visitor<'a> {
            bump: &'a crate::arena::Bump,
        }

        impl<'a, 'de> ::serde::de::Visitor<'de> for #visitor<'a> {
            type Value = #value;

            fn expecting(&self, formatter: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                formatter.write_str(#expecting)
            }

            fn visit_seq<__A: ::serde::de::SeqAccess<'de>>(
                self,
                mut seq: __A,
            ) -> ::core::result::Result<Self::Value, __A::Error> {
                #(#seq_reads)*
                ::core::result::Result::Ok(#constructor { #(#idents: #slots),* })
            }

            fn visit_map<__A: ::serde::de::MapAccess<'de>>(
                self,
                mut map: __A,
            ) -> ::core::result::Result<Self::Value, __A::Error> {
                #(let mut #slots: ::core::option::Option<#types> = ::core::option::Option::None;)*
                while let ::core::option::Option::Some(key) =
                    ::serde::de::MapAccess::next_key::<__Field>(&mut map)?
                {
                    match key {
                        #(#map_arms)*
                        __Field::__ignore => {
                            ::serde::de::MapAccess::next_value::<::serde::de::IgnoredAny>(&mut map)?;
                        }
                    }
                }
                #(let #slots = match #slots {
                    ::core::option::Option::Some(value) => value,
                    ::core::option::Option::None => #map_missing,
                };)*
                ::core::result::Result::Ok(#constructor { #(#idents: #slots),* })
            }
        }
    })
}

fn struct_deserialize(owned: &Ident, mirror: &Ident, fields: &[Field]) -> syn::Result<TokenStream> {
    let items = struct_visitor(
        &format_ident!("__Visitor"),
        &quote!(#mirror),
        &quote!(#mirror<'a>),
        &format!("struct {owned}"),
        fields,
    )?;
    let names = fields.iter().map(|field| field.name.as_str());
    let owned = owned.to_string();
    Ok(quote! {
        #[automatically_derived]
        impl<'a> crate::arena::ArenaDeserialize<'a> for #mirror<'a> {
            fn deserialize_in<'de, D: ::serde::Deserializer<'de>>(
                bump: &'a crate::arena::Bump,
                deserializer: D,
            ) -> ::core::result::Result<Self, D::Error> {
                #items
                ::serde::Deserializer::deserialize_struct(
                    deserializer,
                    #owned,
                    &[#(#names),*],
                    __Visitor { bump },
                )
            }
        }
    })
}

fn enum_deserialize(
    owned: &Ident,
    mirror: &Ident,
    variants: &[crate::model::Variant],
) -> syn::Result<TokenStream> {
    let ids = (0..variants.len())
        .map(|index| format_ident!("__variant{index}"))
        .collect::<Vec<_>>();
    let names = variants
        .iter()
        .map(|variant| variant.name.as_str())
        .collect::<Vec<_>>();
    let bytes = names
        .iter()
        .map(|name| LitByteStr::new(name.as_bytes(), Span::call_site()))
        .collect::<Vec<_>>();
    let indices = (0..variants.len() as u64).collect::<Vec<_>>();
    let index_message = format!("variant index 0 <= i < {}", variants.len());
    let arms = variants
        .iter()
        .zip(&ids)
        .map(|(variant, id)| {
            let ident = &variant.ident;
            let expecting = |kind: &str| format!("{kind} variant {owned}::{ident}");
            match &variant.fields {
                VariantFields::Unit => Ok(quote! {
                    (__Variant::#id, variant) => {
                        ::serde::de::VariantAccess::unit_variant(variant)?;
                        ::core::result::Result::Ok(#mirror::#ident)
                    }
                }),
                VariantFields::Newtype(field) => {
                    let ty = mirror_type(&field.ty, &field.mode)?;
                    let read = match field.mode {
                        Mode::Owned => quote! {
                            ::serde::de::VariantAccess::newtype_variant::<#ty>(variant)
                        },
                        Mode::Mirror | Mode::Copy => quote! {
                            ::serde::de::VariantAccess::newtype_variant_seed(
                                variant,
                                crate::arena::Seed::<#ty>::new(self.bump),
                            )
                        },
                    };
                    Ok(quote!((__Variant::#id, variant) => #read.map(#mirror::#ident),))
                }
                VariantFields::Tuple(fields) => {
                    let expecting = expecting("tuple");
                    let length = format!("{expecting} with {} elements", fields.len());
                    let count = fields.len();
                    let slots = (0..fields.len())
                        .map(|index| format_ident!("__field{index}"))
                        .collect::<Vec<_>>();
                    let reads = fields
                        .iter()
                        .zip(&slots)
                        .enumerate()
                        .map(|(index, (field, slot))| {
                            let ty = mirror_type(&field.ty, &field.mode)?;
                            let element = read(field, &ty, Access::Seq);
                            Ok(quote! {
                                let #slot = match #element {
                                    ::core::option::Option::Some(value) => value,
                                    ::core::option::Option::None => {
                                        return ::core::result::Result::Err(
                                            ::serde::de::Error::invalid_length(#index, &#length),
                                        );
                                    }
                                };
                            })
                        })
                        .collect::<syn::Result<Vec<_>>>()?;
                    Ok(quote! {
                        (__Variant::#id, variant) => {
                            struct __TupleVisitor<'a> {
                                bump: &'a crate::arena::Bump,
                            }

                            impl<'a, 'de> ::serde::de::Visitor<'de> for __TupleVisitor<'a> {
                                type Value = #mirror<'a>;

                                fn expecting(
                                    &self,
                                    formatter: &mut ::core::fmt::Formatter<'_>,
                                ) -> ::core::fmt::Result {
                                    formatter.write_str(#expecting)
                                }

                                fn visit_seq<__A: ::serde::de::SeqAccess<'de>>(
                                    self,
                                    mut seq: __A,
                                ) -> ::core::result::Result<Self::Value, __A::Error> {
                                    #(#reads)*
                                    ::core::result::Result::Ok(#mirror::#ident(#(#slots),*))
                                }
                            }

                            ::serde::de::VariantAccess::tuple_variant(
                                variant,
                                #count,
                                __TupleVisitor { bump: self.bump },
                            )
                        }
                    })
                }
                VariantFields::Struct(fields) => {
                    let items = struct_visitor(
                        &format_ident!("__VariantVisitor"),
                        &quote!(#mirror::#ident),
                        &quote!(#mirror<'a>),
                        &expecting("struct"),
                        fields,
                    )?;
                    let field_names = fields.iter().map(|field| field.name.as_str());
                    Ok(quote! {
                        (__Variant::#id, variant) => {
                            #items
                            ::serde::de::VariantAccess::struct_variant(
                                variant,
                                &[#(#field_names),*],
                                __VariantVisitor { bump: self.bump },
                            )
                        }
                    })
                }
            }
        })
        .collect::<syn::Result<Vec<_>>>()?;
    let expecting = format!("enum {owned}");
    let owned = owned.to_string();
    Ok(quote! {
        #[automatically_derived]
        impl<'a> crate::arena::ArenaDeserialize<'a> for #mirror<'a> {
            fn deserialize_in<'de, D: ::serde::Deserializer<'de>>(
                bump: &'a crate::arena::Bump,
                deserializer: D,
            ) -> ::core::result::Result<Self, D::Error> {
                const __VARIANTS: &[&str] = &[#(#names),*];

                #[allow(non_camel_case_types)]
                enum __Variant {
                    #(#ids,)*
                }

                struct __VariantIdVisitor;

                impl<'de> ::serde::de::Visitor<'de> for __VariantIdVisitor {
                    type Value = __Variant;

                    fn expecting(&self, formatter: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                        formatter.write_str("variant identifier")
                    }

                    fn visit_u64<__E: ::serde::de::Error>(
                        self,
                        value: u64,
                    ) -> ::core::result::Result<__Variant, __E> {
                        match value {
                            #(#indices => ::core::result::Result::Ok(__Variant::#ids),)*
                            _ => ::core::result::Result::Err(::serde::de::Error::invalid_value(
                                ::serde::de::Unexpected::Unsigned(value),
                                &#index_message,
                            )),
                        }
                    }

                    fn visit_str<__E: ::serde::de::Error>(
                        self,
                        value: &str,
                    ) -> ::core::result::Result<__Variant, __E> {
                        match value {
                            #(#names => ::core::result::Result::Ok(__Variant::#ids),)*
                            _ => ::core::result::Result::Err(
                                ::serde::de::Error::unknown_variant(value, __VARIANTS),
                            ),
                        }
                    }

                    fn visit_bytes<__E: ::serde::de::Error>(
                        self,
                        value: &[u8],
                    ) -> ::core::result::Result<__Variant, __E> {
                        match value {
                            #(#bytes => ::core::result::Result::Ok(__Variant::#ids),)*
                            _ => ::core::result::Result::Err(::serde::de::Error::unknown_variant(
                                &::std::string::String::from_utf8_lossy(value),
                                __VARIANTS,
                            )),
                        }
                    }
                }

                impl<'de> ::serde::Deserialize<'de> for __Variant {
                    fn deserialize<__D: ::serde::Deserializer<'de>>(
                        deserializer: __D,
                    ) -> ::core::result::Result<Self, __D::Error> {
                        ::serde::Deserializer::deserialize_identifier(deserializer, __VariantIdVisitor)
                    }
                }

                struct __Visitor<'a> {
                    bump: &'a crate::arena::Bump,
                }

                impl<'a, 'de> ::serde::de::Visitor<'de> for __Visitor<'a> {
                    type Value = #mirror<'a>;

                    fn expecting(&self, formatter: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                        formatter.write_str(#expecting)
                    }

                    fn visit_enum<__A: ::serde::de::EnumAccess<'de>>(
                        self,
                        data: __A,
                    ) -> ::core::result::Result<Self::Value, __A::Error> {
                        match ::serde::de::EnumAccess::variant::<__Variant>(data)? {
                            #(#arms)*
                        }
                    }
                }

                ::serde::Deserializer::deserialize_enum(
                    deserializer,
                    #owned,
                    __VARIANTS,
                    __Visitor { bump },
                )
            }
        }
    })
}

/// Convert one mirror field back to its owned type.
fn owned_value(field: &Field, value: &TokenStream) -> TokenStream {
    let ty = &field.ty;
    match field.mode {
        Mode::Owned => quote!(#value),
        Mode::Mirror | Mode::Copy => {
            quote!(<_ as crate::arena::IntoOwned<#ty>>::into_owned(#value))
        }
    }
}

fn struct_into_owned(owned: &Ident, fields: &[Field]) -> TokenStream {
    let values = fields.iter().map(|field| {
        let ident = &field.ident;
        let value = owned_value(field, &quote!(self.#ident));
        quote!(#ident: #value)
    });
    quote!(#owned { #(#values,)* })
}

fn enum_into_owned(
    owned: &Ident,
    mirror: &Ident,
    variants: &[crate::model::Variant],
) -> TokenStream {
    let arms = variants.iter().map(|variant| {
        let ident = &variant.ident;
        match &variant.fields {
            VariantFields::Unit => quote!(#mirror::#ident => #owned::#ident,),
            VariantFields::Newtype(field) => {
                let value = owned_value(field, &quote!(value));
                quote!(#mirror::#ident(value) => #owned::#ident(#value),)
            }
            VariantFields::Tuple(fields) => {
                let slots = (0..fields.len())
                    .map(|index| format_ident!("__field{index}"))
                    .collect::<Vec<_>>();
                let values = fields
                    .iter()
                    .zip(&slots)
                    .map(|(field, slot)| owned_value(field, &quote!(#slot)));
                quote!(#mirror::#ident(#(#slots),*) => #owned::#ident(#(#values),*),)
            }
            VariantFields::Struct(fields) => {
                let idents = fields.iter().map(|field| &field.ident).collect::<Vec<_>>();
                let values = fields.iter().map(|field| {
                    let ident = &field.ident;
                    owned_value(field, &quote!(#ident))
                });
                quote!(#mirror::#ident { #(#idents),* } => #owned::#ident { #(#idents: #values),* },)
            }
        }
    });
    quote!(match self { #(#arms)* })
}
