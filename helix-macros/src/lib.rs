extern crate proc_macro;
extern crate quote;
extern crate syn;

use proc_macro::TokenStream;
use quote::quote;
use syn::{
    Data, DeriveInput, Expr, FnArg, Ident, ItemFn, ItemStruct, ItemTrait, LitInt, Pat, Stmt, Token,
    TraitItem,
    parse::{Parse, ParseStream},
    parse_macro_input,
};

struct HandlerArgs {
    is_write: bool,
}

impl Parse for HandlerArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        if input.is_empty() {
            return Ok(HandlerArgs { is_write: false });
        }
        let ident: Ident = input.parse()?;
        if ident == "is_write" {
            Ok(HandlerArgs { is_write: true })
        } else {
            Err(syn::Error::new(ident.span(), "expected `is_write`"))
        }
    }
}

#[proc_macro_attribute]
pub fn handler(args: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as HandlerArgs);
    let input_fn = parse_macro_input!(item as ItemFn);
    let fn_name = &input_fn.sig.ident;
    let fn_name_str = fn_name.to_string();
    let is_write = args.is_write;
    // Create a unique static name for each handler
    let static_name = quote::format_ident!(
        "_MAIN_HANDLER_REGISTRATION_{}",
        fn_name.to_string().to_uppercase()
    );

    let expanded = quote! {
        #input_fn

        #[doc(hidden)]
        #[used]
        static #static_name: () = {
            inventory::submit! {
                ::helix_db::helix_gateway::router::router::HandlerSubmission(
                    ::helix_db::helix_gateway::router::router::Handler::new(
                        #fn_name_str,
                        #fn_name,
                        #is_write
                    )
                )
            }
        };
    };
    expanded.into()
}

#[proc_macro_attribute]
pub fn mcp_handler(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input_fn = parse_macro_input!(item as ItemFn);
    let fn_name = &input_fn.sig.ident;
    let fn_name_str = fn_name.to_string();
    // Create a unique static name for each handler
    let static_name = quote::format_ident!(
        "_MCP_HANDLER_REGISTRATION_{}",
        fn_name.to_string().to_uppercase()
    );

    let expanded = quote! {
        #input_fn

        #[doc(hidden)]
        #[used]
        static #static_name: () = {
            inventory::submit! {
                MCPHandlerSubmission(
                    MCPHandler::new(
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
pub fn get_handler(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input_fn = parse_macro_input!(item as ItemFn);
    let fn_name = &input_fn.sig.ident;
    let fn_name_str = fn_name.to_string();
    let static_name = quote::format_ident!(
        "__GET_HANDLER_REGISTRATION_{}",
        fn_name.to_string().to_uppercase()
    );

    let expanded = quote! {
        #input_fn

        #[doc(hidden)]
        #[used]
        static #static_name: () = {
            inventory::submit! {
                ::helix_db::helix_gateway::router::router::HandlerSubmission(
                    ::helix_db::helix_gateway::router::router::Handler::new(
                        #fn_name_str,
                        #fn_name,
                        false
                    )
                )
            }
        };
    };
    expanded.into()
}

#[proc_macro_attribute]
pub fn tool_calls(_attr: TokenStream, input: TokenStream) -> TokenStream {
    let input_trait = parse_macro_input!(input as ItemTrait);
    let mut impl_methods = Vec::new();

    for item in input_trait.clone().items {
        if let TraitItem::Fn(method) = item {
            let fn_name = &method.sig.ident;

            // Extract method parameters (skip &self and txn)
            let method_params: Vec<_> = method.sig.inputs.iter().skip(3).collect();
            let (field_names, struct_fields): (Vec<_>, Vec<_>) = method_params
                .iter()
                .filter_map(|param| {
                    if let FnArg::Typed(pat_type) = param {
                        let field_name = if let Pat::Ident(pat_ident) = &*pat_type.pat {
                            &pat_ident.ident
                        } else {
                            return None;
                        };

                        let field_type = &pat_type.ty;
                        Some((quote! { #field_name }, quote! { #field_name: #field_type }))
                    } else {
                        None
                    }
                })
                .collect();

            let struct_name = quote::format_ident!("{}Data", fn_name);
            let mcp_struct_name = quote::format_ident!("{}McpInput", fn_name);
            let expanded = quote! {

                #[derive(Debug, Deserialize)]
                #[allow(non_camel_case_types)]
                pub struct #mcp_struct_name {
                    #(#struct_fields),*
                }

                #[derive(Debug, Deserialize)]
                #[allow(non_camel_case_types)]
                struct #struct_name {
                    connection_id: String,
                    data: #mcp_struct_name,
                }

                #[mcp_handler]
                #[allow(non_camel_case_types)]
                pub fn #fn_name<'a>(
                    input: &'a mut MCPToolInput,
                ) -> Result<Response, GraphError> {
                    let data = input.request.in_fmt.deserialize_owned::<#struct_name>(&input.request.body)?;

                    let mut connections = input.mcp_connections.lock().unwrap();
                    let mut connection = match connections.remove_connection(&data.connection_id) {
                        Some(conn) => conn,
                        None => return Err(GraphError::Default),
                    };
                    drop(connections);

                    let txn = input.mcp_backend.db.graph_env.read_txn()?;

                    let result = input.mcp_backend.#fn_name(&txn, &connection, #(data.data.#field_names),*)?;

                    let first = result.first().unwrap_or(&TraversalValue::Empty).clone();

                    connection.iter = result.into_iter();
                    let mut connections = input.mcp_connections.lock().unwrap();
                    connections.add_connection(connection);
                    drop(connections);

                    Ok(crate::protocol::format::Format::Json.create_response(&ReturnValue::from(first)))
                }
            };

            impl_methods.push(expanded);
        }
    }

    let expanded = quote! {
        #(#impl_methods)*
        #input_trait
    };

    TokenStream::from(expanded)
}

struct ToolCallArgs {
    name: Ident,
    _comma: Token![,],
    txn_type: Ident,
}
impl Parse for ToolCallArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        Ok(ToolCallArgs {
            name: input.parse()?,
            _comma: input.parse()?,
            txn_type: input.parse()?,
        })
    }
}

#[proc_macro_attribute]
pub fn tool_call(args: TokenStream, input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as ToolCallArgs);
    let method = parse_macro_input!(input as ItemFn);

    let name = args.name;
    let txn_type = match args.txn_type.to_string().as_str() {
        "with_read" => quote! { let txn = db.graph_env.read_txn().unwrap(); },
        "with_write" => quote! { let mut txn = db.graph_env.write_txn().unwrap(); },
        _ => panic!("Invalid transaction type: expected 'with_read' or 'with_write'"),
    };

    let fn_name = &method.sig.ident;
    let fn_block = &method.block.stmts;

    let struct_name = quote::format_ident!("{}Input", fn_name);
    let mcp_function_name = quote::format_ident!("{}Mcp", fn_name);
    let mcp_struct_name = quote::format_ident!("{}McpInput", fn_name);

    let query_stmts = match fn_block.first() {
        Some(Stmt::Expr(Expr::Block(block), _)) => block.block.stmts.clone(),
        _ => panic!("Query block not found"),
    };

    let mcp_query_block = quote! {
        {

            let mut remapping_vals = RemappingMap::new();
            let db = Arc::clone(&input.mcp_backend.db);
            #txn_type
            let data: #struct_name = data.data;
            #(#query_stmts)*
            txn.commit().unwrap();
            #name.into_iter()
        }
    };

    let new_method = quote! {

        #[derive(Deserialize)]
        #[allow(non_camel_case_types)]
        struct #mcp_struct_name{
            connection_id: String,
            data: #struct_name,
        }

        #[mcp_handler]
        #[allow(non_camel_case_types)]
        pub fn #mcp_function_name<'a>(
            input: &'a mut MCPToolInput,
        ) -> Result<Response, GraphError> {
            let data = &*input.request.in_fmt.deserialize::<#mcp_struct_name>(&input.request.body)?;

            let mut connections = input.mcp_connections.lock().unwrap();
            let mut connection = match connections.remove_connection(&data.connection_id) {
                Some(conn) => conn,
                None => return Err(GraphError::Default),
            };
            drop(connections);

            let mut result = #mcp_query_block;

            let first = result.next().unwrap_or(TraversalValue::Empty);

            connection.iter = result.into_iter();
            let mut connections = input.mcp_connections.lock().unwrap();
            connections.add_connection(connection);
            drop(connections);
            Ok(crate::protocol::format::Format::Json.create_response(&ReturnValue::from(first)))
        }
    };

    let expanded = quote! {
        #method
        #new_method
    };

    TokenStream::from(expanded)
}

// example:
// #[migration(User, 1 -> 2)]
// pub fn __migration_User_1_2(props: HashMap<String, Value>) -> HashMap<String, Value> {
//     field_addition_from_old_field!(props, "username", "username");
//     props
// }

impl Parse for MigrationArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        Ok(MigrationArgs {
            item: input.parse()?,
            _comma: input.parse()?,
            from_version: input.parse()?,
            _arrow: input.parse()?,
            to_version: input.parse()?,
        })
    }
}

struct MigrationArgs {
    item: Ident,
    _comma: Token![,],
    from_version: LitInt,
    _arrow: Token![->],
    to_version: LitInt,
}

#[proc_macro_attribute]
pub fn migration(args: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as MigrationArgs);

    let input_fn = parse_macro_input!(item as ItemFn);
    let fn_name = &input_fn.sig.ident;

    // Create a unique static name for each handler
    let static_name = quote::format_ident!(
        "_MAIN_HANDLER_REGISTRATION_{}",
        fn_name.to_string().to_uppercase()
    );

    let item = &args.item;
    let from_version = &args.from_version;
    let to_version = &args.to_version;

    let expanded = quote! {
        #input_fn


        #[doc(hidden)]
        #[used]
        static #static_name: () = {
            inventory::submit! {
                ::helix_db::helix_engine::graph_core::ops::version_info::TransitionSubmission(
                    ::helix_db::helix_engine::graph_core::ops::version_info::Transition::new(
                        stringify!(#item),
                        #from_version,
                        #to_version,
                        #fn_name
                    )
                )
            }
        };
    };
    expanded.into()
}

#[proc_macro_attribute]
pub fn helix_node(_attr: TokenStream, input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as ItemStruct);
    let name = &input.ident;
    let fields = input.fields.iter();

    let expanded = quote! {
        pub struct #name {
            id: String,
            #(#fields),*
        }
    };

    TokenStream::from(expanded)
}

#[proc_macro_derive(Traversable)]
pub fn traversable_derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    // Verify that the struct has an 'id' field
    let has_id_field = match &input.data {
        Data::Struct(data) => data
            .fields
            .iter()
            .any(|field| field.ident.as_ref().map(|i| i == "id").unwrap_or(false)),
        _ => false,
    };

    if !has_id_field {
        return TokenStream::from(quote! {
            compile_error!("Traversable can only be derived for structs with an 'id: &'a str' field");
        });
    }

    // Extract lifetime parameter if present
    let lifetime = if let Some(param) = input.generics.lifetimes().next() {
        let lifetime = &param.lifetime;
        quote! { #lifetime }
    } else {
        quote! { 'a }
    };

    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let expanded = quote! {
        impl #impl_generics #name #ty_generics #where_clause {
            pub fn id(&self) -> &#lifetime str {
                self.id
            }
        }
    };

    TokenStream::from(expanded)
}
