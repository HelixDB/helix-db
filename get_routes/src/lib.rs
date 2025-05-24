extern crate proc_macro;
extern crate quote;
extern crate syn;

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemFn};

#[proc_macro_attribute]
pub fn handler(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input_fn = parse_macro_input!(item as ItemFn);
    let fn_name = &input_fn.sig.ident;
    let fn_name_str = fn_name.to_string();
    println!("fn_name_str: {}", fn_name_str);
    // Create a unique static name for each handler
    let static_name = quote::format_ident!("__HANDLER_REGISTRATION_{}", fn_name.to_string().to_uppercase());

    let expanded = quote! {
        #input_fn

        #[doc(hidden)]
        #[used]
        static #static_name: () = {
            inventory::submit! {
                ::helixdb::helix_gateway::router::router::HandlerSubmission(
                    ::helixdb::helix_gateway::router::router::Handler::new(
                        #fn_name_str,
                        #fn_name
                    )
                )
            }
        };
    };
    expanded.into()
}


#[proc_macro_attribute]
pub fn local_handler(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input_fn = parse_macro_input!(item as ItemFn);
    let fn_name = &input_fn.sig.ident;
    let fn_name_str = fn_name.to_string();
    println!("fn_name_str: {}", fn_name_str);
    // Create a unique static name for each handler
    let static_name = quote::format_ident!("__HANDLER_REGISTRATION_{}", fn_name.to_string().to_uppercase());

    let expanded = quote! {
        #input_fn

        #[doc(hidden)]
        #[used]
        static #static_name: () = {
            inventory::submit! {
                ::helix_gateway::router::router::HandlerSubmission(
                    ::helix_gateway::router::router::Handler::new(
                        #fn_name_str,
                        #fn_name
                    )
                )
            }
        };
    };
    expanded.into()
}

