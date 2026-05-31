use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{
    FnArg, GenericArgument, ItemFn, Pat, PathArguments, Type, parse_macro_input, parse_quote,
};

enum QueryKind {
    Read,
    Write,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ParamTypeSpec {
    Bool,
    I64,
    F64,
    F32,
    String,
    DateTime,
    Bytes,
    Value,
    Object(Box<ParamTypeSpec>),
    Array(Box<ParamTypeSpec>),
}

impl ParamTypeSpec {
    fn to_tokens(&self) -> proc_macro2::TokenStream {
        match self {
            Self::Bool => quote! { ::helix_db::QueryParamType::Bool },
            Self::I64 => quote! { ::helix_db::QueryParamType::I64 },
            Self::F64 => quote! { ::helix_db::QueryParamType::F64 },
            Self::F32 => quote! { ::helix_db::QueryParamType::F32 },
            Self::String => quote! { ::helix_db::QueryParamType::String },
            Self::DateTime => quote! { ::helix_db::QueryParamType::DateTime },
            Self::Bytes => quote! { ::helix_db::QueryParamType::Bytes },
            Self::Value => quote! { ::helix_db::QueryParamType::Value },
            Self::Object(_) => quote! { ::helix_db::QueryParamType::Object },
            Self::Array(inner) => {
                let inner = inner.to_tokens();
                quote! { ::helix_db::QueryParamType::Array(Box::new(#inner)) }
            }
        }
    }

    fn to_dynamic_value_tokens(
        &self,
        value: proc_macro2::TokenStream,
        path: proc_macro2::TokenStream,
        depth: usize,
    ) -> proc_macro2::TokenStream {
        match self {
            Self::Bool => quote! {
                ::std::result::Result::<
                    ::helix_db::DynamicQueryValue,
                    ::helix_db::DynamicQueryError,
                >::Ok(::helix_db::DynamicQueryValue::Bool(#value))
            },
            Self::I64 => quote! {
                ::std::result::Result::<
                    ::helix_db::DynamicQueryValue,
                    ::helix_db::DynamicQueryError,
                >::Ok(::helix_db::DynamicQueryValue::I64(#value))
            },
            Self::F64 => quote! {
                ::std::result::Result::<
                    ::helix_db::DynamicQueryValue,
                    ::helix_db::DynamicQueryError,
                >::Ok(::helix_db::DynamicQueryValue::F64(#value))
            },
            Self::F32 => quote! {
                ::std::result::Result::<
                    ::helix_db::DynamicQueryValue,
                    ::helix_db::DynamicQueryError,
                >::Ok(::helix_db::DynamicQueryValue::F32(#value))
            },
            Self::String => quote! {
                ::std::result::Result::<
                    ::helix_db::DynamicQueryValue,
                    ::helix_db::DynamicQueryError,
                >::Ok(::helix_db::DynamicQueryValue::String(#value))
            },
            Self::DateTime => quote! {
                ::std::result::Result::<
                    ::helix_db::DynamicQueryValue,
                    ::helix_db::DynamicQueryError,
                >::Ok(::helix_db::DynamicQueryValue::String(
                    (#value)
                        .to_rfc3339()
                        .ok_or_else(|| ::helix_db::DynamicQueryError::invalid_datetime(#path, (#value).millis()))?
                ))
            },
            Self::Bytes => quote! {
                ::std::result::Result::<
                    ::helix_db::DynamicQueryValue,
                    ::helix_db::DynamicQueryError,
                >::Err(::helix_db::DynamicQueryError::unsupported_bytes(#path))
            },
            Self::Value => quote! {
                ::helix_db::__private::dynamic_query_value_from_property_value(#value, #path)
            },
            Self::Object(inner) => {
                let key_ident = format_ident!("__helix_param_key_{depth}");
                let value_ident = format_ident!("__helix_param_value_{depth}");
                let path_ident = format_ident!("__helix_param_path_{depth}");
                let inner_tokens = inner.to_dynamic_value_tokens(
                    quote! { #value_ident },
                    quote! { #path_ident },
                    depth + 1,
                );

                quote! {
                    ::std::result::Result::<
                        ::helix_db::DynamicQueryValue,
                        ::helix_db::DynamicQueryError,
                    >::Ok(::helix_db::DynamicQueryValue::Object(
                        (#value)
                            .into_iter()
                            .map(|(#key_ident, #value_ident)| {
                                let #path_ident = ::std::format!("{}.{}", #path, #key_ident);
                                ::std::result::Result::Ok((#key_ident, #inner_tokens?))
                            })
                            .collect::<::std::result::Result<
                                ::std::collections::BTreeMap<_, _>,
                                ::helix_db::DynamicQueryError,
                            >>()?,
                    ))
                }
            }
            Self::Array(inner) => {
                let index_ident = format_ident!("__helix_param_index_{depth}");
                let value_ident = format_ident!("__helix_param_value_{depth}");
                let path_ident = format_ident!("__helix_param_path_{depth}");
                let inner_tokens = inner.to_dynamic_value_tokens(
                    quote! { #value_ident },
                    quote! { #path_ident },
                    depth + 1,
                );

                quote! {
                    ::std::result::Result::<
                        ::helix_db::DynamicQueryValue,
                        ::helix_db::DynamicQueryError,
                    >::Ok(::helix_db::DynamicQueryValue::Array(
                        (#value)
                            .into_iter()
                            .enumerate()
                            .map(|(#index_ident, #value_ident)| {
                                let #path_ident = ::std::format!("{}[{}]", #path, #index_ident);
                                #inner_tokens
                            })
                            .collect::<::std::result::Result<
                                ::std::vec::Vec<_>,
                                ::helix_db::DynamicQueryError,
                            >>()?,
                    ))
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParamSpec {
    ident: syn::Ident,
    ty: ParamTypeSpec,
}

/// Infer whether a registered function is a read or write query by inspecting its body for a
/// call to `read_batch()` or `write_batch()`. The return type is not used (and may be omitted).
fn infer_query_kind(fn_item: &ItemFn) -> syn::Result<QueryKind> {
    fn mentions(tokens: proc_macro2::TokenStream, target: &str) -> bool {
        tokens.into_iter().any(|tt| match tt {
            proc_macro2::TokenTree::Ident(ident) => ident == target,
            proc_macro2::TokenTree::Group(group) => mentions(group.stream(), target),
            _ => false,
        })
    }

    let body = &fn_item.block;
    let tokens = quote! { #body };
    if mentions(tokens.clone(), "write_batch") {
        Ok(QueryKind::Write)
    } else if mentions(tokens, "read_batch") {
        Ok(QueryKind::Read)
    } else {
        Err(syn::Error::new_spanned(
            &fn_item.sig,
            "could not infer query kind: function body must call `read_batch()` or `write_batch()`",
        ))
    }
}

const TYPE_ERROR_MSG: &str = "\
#[register] parameter type must be a supported query parameter type: \
bool, i64, f64, f32, String, DateTime, Vec<u8>, PropertyValue, ParamValue, ParamObject, \
Vec<T> for supported T, or BTreeMap<String, T>/HashMap<String, T> for supported T";

fn ensure_no_args(segment: &syn::PathSegment, ty: &Type) -> syn::Result<()> {
    if matches!(segment.arguments, PathArguments::None) {
        Ok(())
    } else {
        Err(syn::Error::new_spanned(ty, TYPE_ERROR_MSG))
    }
}

fn single_type_arg<'a>(segment: &'a syn::PathSegment, ty: &Type) -> syn::Result<&'a Type> {
    let PathArguments::AngleBracketed(args) = &segment.arguments else {
        return Err(syn::Error::new_spanned(ty, TYPE_ERROR_MSG));
    };
    if args.args.len() != 1 {
        return Err(syn::Error::new_spanned(ty, TYPE_ERROR_MSG));
    }
    match args.args.first() {
        Some(GenericArgument::Type(inner)) => Ok(inner),
        _ => Err(syn::Error::new_spanned(ty, TYPE_ERROR_MSG)),
    }
}

fn two_type_args<'a>(
    segment: &'a syn::PathSegment,
    ty: &Type,
) -> syn::Result<(&'a Type, &'a Type)> {
    let PathArguments::AngleBracketed(args) = &segment.arguments else {
        return Err(syn::Error::new_spanned(ty, TYPE_ERROR_MSG));
    };
    if args.args.len() != 2 {
        return Err(syn::Error::new_spanned(ty, TYPE_ERROR_MSG));
    }
    let first = match args.args.first() {
        Some(GenericArgument::Type(inner)) => inner,
        _ => return Err(syn::Error::new_spanned(ty, TYPE_ERROR_MSG)),
    };
    let second = match args.args.iter().nth(1) {
        Some(GenericArgument::Type(inner)) => inner,
        _ => return Err(syn::Error::new_spanned(ty, TYPE_ERROR_MSG)),
    };
    Ok((first, second))
}

fn is_string_type(ty: &Type) -> bool {
    let Type::Path(type_path) = ty else {
        return false;
    };
    let Some(segment) = type_path.path.segments.last() else {
        return false;
    };
    segment.ident == "String" && matches!(segment.arguments, PathArguments::None)
}

/// Parse a supported registered parameter type into an owned schema shape.
fn parse_param_type(ty: &Type) -> syn::Result<ParamTypeSpec> {
    let Type::Path(type_path) = ty else {
        return Err(syn::Error::new_spanned(ty, TYPE_ERROR_MSG));
    };

    let Some(segment) = type_path.path.segments.last() else {
        return Err(syn::Error::new_spanned(ty, TYPE_ERROR_MSG));
    };

    let type_name = segment.ident.to_string();

    match type_name.as_str() {
        "bool" => {
            ensure_no_args(segment, ty)?;
            Ok(ParamTypeSpec::Bool)
        }
        "i64" => {
            ensure_no_args(segment, ty)?;
            Ok(ParamTypeSpec::I64)
        }
        "f64" => {
            ensure_no_args(segment, ty)?;
            Ok(ParamTypeSpec::F64)
        }
        "f32" => {
            ensure_no_args(segment, ty)?;
            Ok(ParamTypeSpec::F32)
        }
        "String" => {
            ensure_no_args(segment, ty)?;
            Ok(ParamTypeSpec::String)
        }
        "DateTime" => {
            ensure_no_args(segment, ty)?;
            Ok(ParamTypeSpec::DateTime)
        }
        "PropertyValue" | "ParamValue" => {
            ensure_no_args(segment, ty)?;
            Ok(ParamTypeSpec::Value)
        }
        "ParamObject" => {
            ensure_no_args(segment, ty)?;
            Ok(ParamTypeSpec::Object(Box::new(ParamTypeSpec::Value)))
        }
        "Vec" => {
            let inner = single_type_arg(segment, ty)?;
            if let Type::Path(inner_path) = inner
                && let Some(inner_seg) = inner_path.path.segments.last()
                && inner_seg.ident == "u8"
                && matches!(inner_seg.arguments, PathArguments::None)
            {
                return Ok(ParamTypeSpec::Bytes);
            }
            Ok(ParamTypeSpec::Array(Box::new(parse_param_type(inner)?)))
        }
        "BTreeMap" | "HashMap" => {
            let (key_ty, value_ty) = two_type_args(segment, ty)?;
            if !is_string_type(key_ty) {
                return Err(syn::Error::new_spanned(key_ty, TYPE_ERROR_MSG));
            }
            Ok(ParamTypeSpec::Object(Box::new(parse_param_type(value_ty)?)))
        }
        _ => Err(syn::Error::new_spanned(ty, TYPE_ERROR_MSG)),
    }
}

/// Extract and validate parameter declarations from function arguments.
fn extract_param_specs(fn_item: &ItemFn) -> syn::Result<Vec<ParamSpec>> {
    let mut params = Vec::new();
    for arg in &fn_item.sig.inputs {
        match arg {
            FnArg::Receiver(recv) => {
                return Err(syn::Error::new_spanned(
                    recv,
                    "#[register] functions cannot take self",
                ));
            }
            FnArg::Typed(pat_type) => {
                if let Pat::Ident(pat_ident) = &*pat_type.pat {
                    params.push(ParamSpec {
                        ident: pat_ident.ident.clone(),
                        ty: parse_param_type(&pat_type.ty)?,
                    });
                } else {
                    return Err(syn::Error::new_spanned(
                        &pat_type.pat,
                        "#[register] function parameters must be simple identifiers",
                    ));
                }
            }
        }
    }
    Ok(params)
}

#[proc_macro_attribute]
pub fn register(attr: TokenStream, item: TokenStream) -> TokenStream {
    if !attr.is_empty() {
        return syn::Error::new(
            proc_macro2::Span::call_site(),
            "#[register] does not accept arguments",
        )
        .to_compile_error()
        .into();
    }

    let fn_item = parse_macro_input!(item as ItemFn);

    if fn_item.sig.asyncness.is_some() {
        return syn::Error::new_spanned(&fn_item.sig, "#[register] functions cannot be async")
            .to_compile_error()
            .into();
    }

    if !fn_item.sig.generics.params.is_empty() {
        return syn::Error::new_spanned(
            &fn_item.sig.generics,
            "#[register] functions cannot be generic",
        )
        .to_compile_error()
        .into();
    }

    let query_kind = match infer_query_kind(&fn_item) {
        Ok(kind) => kind,
        Err(err) => return err.to_compile_error().into(),
    };

    let param_specs = match extract_param_specs(&fn_item) {
        Ok(params) => params,
        Err(err) => return err.to_compile_error().into(),
    };

    let fn_name = fn_item.sig.ident.clone();
    let fn_attrs = fn_item.attrs.clone();
    let fn_visibility = fn_item.vis.clone();
    let fn_body = &fn_item.block;
    let params_fn_name = format_ident!("__helix_dsl_params_{}", fn_name);

    // Generate `let name = Expr::param("name");` bindings for each parameter
    let param_name_strs: Vec<String> = param_specs
        .iter()
        .map(|param| param.ident.to_string())
        .collect();
    let let_bindings = param_specs
        .iter()
        .zip(param_name_strs.iter())
        .map(|(param, name_str)| {
            let ident = &param.ident;
            quote! {
                let #ident = ::helix_db::Expr::param(#name_str);
            }
        });

    let parameter_entries = param_specs
        .iter()
        .zip(param_name_strs.iter())
        .map(|(param, name)| {
            let ty = param.ty.to_tokens();
            quote! {
                ::helix_db::QueryParameter {
                    name: #name.to_string(),
                    ty: #ty,
                }
            }
        });

    let parameters_fn = quote! {
        #[allow(non_snake_case)]
        fn #params_fn_name() -> ::std::vec::Vec<::helix_db::QueryParameter> {
            vec![#(#parameter_entries),*]
        }
    };

    let decomposed_fn_name = format_ident!("{}_decomposed", fn_name);

    // Decompose: strip params, prepend let bindings to body
    let decomposed_fn = match query_kind {
        QueryKind::Read => quote! {
            fn #decomposed_fn_name() -> ::helix_db::ReadBatch {
                #(#let_bindings)*
                #fn_body
            }
        },
        QueryKind::Write => quote! {
            fn #decomposed_fn_name() -> ::helix_db::WriteBatch {
                #(#let_bindings)*
                #fn_body
            }
        },
    };

    // The user-callable function always returns a bare `DynamicQueryRequest`, regardless of
    // visibility: calling `query1(args)` builds the request directly. Parameter coercion that can
    // fail (DateTime, bytes, Value, collections) is wrapped in a closure and `.expect()`-ed so the
    // `?` inside `to_dynamic_value_tokens` has a `Result` context; infallible scalars never panic.
    let callable_fn = {
        let mut request_sig = fn_item.sig.clone();
        request_sig.output = parse_quote!(-> ::helix_db::DynamicQueryRequest);
        let request_ctor = match query_kind {
            QueryKind::Read => quote! { ::helix_db::DynamicQueryRequest::read },
            QueryKind::Write => quote! { ::helix_db::DynamicQueryRequest::write },
        };
        let request_param_inserts =
            param_specs
                .iter()
                .zip(param_name_strs.iter())
                .map(|(param, name)| {
                    let ident = &param.ident;
                    let value_tokens =
                        param
                            .ty
                            .to_dynamic_value_tokens(quote! { #ident }, quote! { #name }, 0);
                    let type_tokens = param.ty.to_tokens();
                    let expect_msg = format!("failed to coerce parameter `{name}`");
                    quote! {
                        request.insert_parameter_value(
                            #name,
                            (|| -> ::std::result::Result<
                                ::helix_db::DynamicQueryValue,
                                ::helix_db::DynamicQueryError,
                            > { #value_tokens })()
                            .expect(#expect_msg),
                        );
                        request.insert_parameter_type(#name, #type_tokens);
                    }
                });

        quote! {
            #(#fn_attrs)*
            #fn_visibility #request_sig {
                let mut request = #request_ctor(#decomposed_fn_name());
                #(#request_param_inserts)*
                request
            }
        }
    };

    let submit_item = match query_kind {
        QueryKind::Read => {
            quote! {
                ::helix_db::__private::inventory::submit! {
                    ::helix_db::RegisteredReadQuery {
                        name: stringify!(#fn_name),
                        build: #decomposed_fn_name,
                        parameters: #params_fn_name,
                    }
                }
            }
        }
        QueryKind::Write => {
            quote! {
                ::helix_db::__private::inventory::submit! {
                    ::helix_db::RegisteredWriteQuery {
                        name: stringify!(#fn_name),
                        build: #decomposed_fn_name,
                        parameters: #params_fn_name,
                    }
                }
            }
        }
    };

    quote! {
        #callable_fn
        #decomposed_fn
        #parameters_fn
        #submit_item
    }
    .into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use syn::{Type, parse_str};

    fn parse_type(input: &str) -> ParamTypeSpec {
        let ty: Type = parse_str(input).expect("parse type");
        parse_param_type(&ty).expect("supported param type")
    }

    #[test]
    fn accepts_nested_batch_object_types() {
        assert_eq!(
            parse_type("ParamObject"),
            ParamTypeSpec::Object(Box::new(ParamTypeSpec::Value))
        );
        assert_eq!(
            parse_type("Vec<ParamObject>"),
            ParamTypeSpec::Array(Box::new(ParamTypeSpec::Object(Box::new(
                ParamTypeSpec::Value
            ))))
        );
        assert_eq!(
            parse_type("Vec<Vec<ParamObject>>"),
            ParamTypeSpec::Array(Box::new(ParamTypeSpec::Array(Box::new(
                ParamTypeSpec::Object(Box::new(ParamTypeSpec::Value))
            ))))
        );
    }

    #[test]
    fn accepts_property_value_aliases_and_maps() {
        assert_eq!(parse_type("PropertyValue"), ParamTypeSpec::Value);
        assert_eq!(parse_type("ParamValue"), ParamTypeSpec::Value);
        assert_eq!(
            parse_type("BTreeMap<String, PropertyValue>"),
            ParamTypeSpec::Object(Box::new(ParamTypeSpec::Value))
        );
        assert_eq!(
            parse_type("std::collections::HashMap<String, ParamValue>"),
            ParamTypeSpec::Object(Box::new(ParamTypeSpec::Value))
        );
        assert_eq!(
            parse_type("BTreeMap<String, String>"),
            ParamTypeSpec::Object(Box::new(ParamTypeSpec::String))
        );
    }

    #[test]
    fn accepts_existing_scalar_and_array_types() {
        assert_eq!(parse_type("bool"), ParamTypeSpec::Bool);
        assert_eq!(parse_type("i64"), ParamTypeSpec::I64);
        assert_eq!(parse_type("DateTime"), ParamTypeSpec::DateTime);
        assert_eq!(parse_type("Vec<u8>"), ParamTypeSpec::Bytes);
        assert_eq!(
            parse_type("Vec<String>"),
            ParamTypeSpec::Array(Box::new(ParamTypeSpec::String))
        );
    }

    #[test]
    fn rejects_unsupported_types() {
        let ty: Type = parse_str("UserBatchRow").expect("parse type");
        assert!(parse_param_type(&ty).is_err());

        let ty: Type = parse_str("Vec<UserBatchRow>").expect("parse type");
        assert!(parse_param_type(&ty).is_err());

        let ty: Type = parse_str("BTreeMap<i64, String>").expect("parse type");
        assert!(parse_param_type(&ty).is_err());
    }
}
