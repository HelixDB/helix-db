//! The mirror of an owned field type: strings, boxes, vectors and string-keyed
//! maps borrow from the arena, and every named type becomes its `Arena`
//! mirror. The rewrite is syntactic, so the mirrors stay covariant in `'a`.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{GenericArgument, PathArguments, Type};

use crate::model::Mode;

/// Types that are their own mirror: `Copy`, own no heap, and deserialize
/// through serde.
const PLAIN: &[&str] = &[
    "bool",
    "u8",
    "u16",
    "u32",
    "u64",
    "usize",
    "i8",
    "i16",
    "i32",
    "i64",
    "isize",
    "f32",
    "f64",
    "NonZeroUsize",
];

/// The mirror of `ty` in `mode`, using the lifetime `'a`.
pub fn mirror_type(ty: &Type, mode: &Mode) -> syn::Result<TokenStream> {
    let keep_named = match mode {
        Mode::Owned => return Ok(quote!(#ty)),
        Mode::Mirror => false,
        Mode::Copy => true,
    };
    let Type::Path(path) = ty else {
        let Type::Tuple(tuple) = ty else {
            return Err(syn::Error::new_spanned(
                ty,
                "ArenaMirror supports paths and tuples only",
            ));
        };
        let elements = tuple
            .elems
            .iter()
            .map(|element| mirror_type(element, mode))
            .collect::<syn::Result<Vec<_>>>()?;
        return Ok(quote!((#(#elements),*)));
    };
    let unsupported = || syn::Error::new_spanned(ty, "ArenaMirror cannot mirror this type");
    let (Some(last), None) = (path.path.segments.last(), &path.qself) else {
        return Err(unsupported());
    };
    let arguments = match &last.arguments {
        PathArguments::None => Vec::new(),
        PathArguments::AngleBracketed(arguments) => arguments
            .args
            .iter()
            .map(|argument| {
                let GenericArgument::Type(ty) = argument else {
                    return Err(unsupported());
                };
                Ok(ty)
            })
            .collect::<syn::Result<Vec<_>>>()?,
        PathArguments::Parenthesized(_) => return Err(unsupported()),
    };
    let name = last.ident.to_string();
    match (name.as_str(), arguments.as_slice()) {
        ("String", []) => Ok(quote!(&'a str)),
        ("Box", [inner]) => {
            let inner = mirror_type(inner, mode)?;
            Ok(quote!(&'a #inner))
        }
        ("Vec", [inner]) => {
            let inner = mirror_type(inner, mode)?;
            Ok(quote!(&'a [#inner]))
        }
        ("Option", [inner]) => {
            let inner = mirror_type(inner, mode)?;
            Ok(quote!(::core::option::Option<#inner>))
        }
        ("BTreeMap", [key, value]) if is_string(key) => {
            let value = mirror_type(value, mode)?;
            Ok(quote!(crate::arena::Map<'a, #value>))
        }
        (plain, []) if keep_named || PLAIN.contains(&plain) => Ok(quote!(#ty)),
        (_, []) => {
            let mut mirror = path.path.clone();
            let segment = mirror
                .segments
                .last_mut()
                .expect("the path has a last segment");
            segment.ident = format_ident!("Arena{}", segment.ident);
            Ok(quote!(#mirror<'a>))
        }
        _ => Err(unsupported()),
    }
}

/// Whether a field of type `ty` deserializes as `None` when it is missing,
/// as serde's `Option` impl does.
pub fn is_option(ty: &Type) -> bool {
    let Type::Path(path) = ty else {
        return false;
    };
    path.path
        .segments
        .last()
        .is_some_and(|segment| segment.ident == "Option")
}

fn is_string(ty: &Type) -> bool {
    let Type::Path(path) = ty else {
        return false;
    };
    path.path.is_ident("String")
}
