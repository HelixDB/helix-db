extern crate proc_macro;
extern crate quote;
extern crate syn;

use proc_macro::TokenStream;
use quote::quote;
use syn::{
    Expr, FnArg, Ident, ItemFn, ItemTrait, LitInt, Pat, Stmt, Token, TraitItem,
    parse::{Parse, ParseStream},
    parse_macro_input,
};

struct HandlerArgs {
    txn_type: Ident,
}
impl Parse for HandlerArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        Ok(HandlerArgs {
            txn_type: input.parse()?,
        })
    }
}

#[proc_macro_attribute]
pub fn handler(args: TokenStream, item: TokenStream) -> TokenStream {
    let input_fn = parse_macro_input!(item as ItemFn);
    let args = parse_macro_input!(args as HandlerArgs);
    let input_fn_block_contents = &input_fn.block.stmts;
    let fn_name = &input_fn.sig.ident;
    let fn_name_str = fn_name.to_string();
    let vis = &input_fn.vis;
    let sig = &input_fn.sig;
    println!("fn_name_str: {fn_name_str}");
    // Create a unique static name for each handler
    let static_name = quote::format_ident!(
        "_MAIN_HANDLER_REGISTRATION_{}",
        fn_name.to_string().to_uppercase()
    );
    let input_data_name = quote::format_ident!("{}Input", fn_name);

    let query_stmts = match input_fn_block_contents.first() {
        Some(Stmt::Expr(Expr::Block(block), _)) => block.block.stmts.clone(),
        _ => panic!("Query block not found"),
    };

    let txn_type = match args.txn_type.to_string().as_str() {
        "with_read" => quote! { let txn = db.graph_env.read_txn().unwrap(); },
        "with_write" => quote! { let mut txn = db.graph_env.write_txn().unwrap(); },
        _ => panic!("Invalid transaction type: expected 'with_read' or 'with_write'"),
    };

    let expanded = quote! {
        #[allow(non_camel_case_types)]
        #vis #sig {
            let data = input.request.in_fmt.deserialize::<#input_data_name>(&input.request.body)?;

            let mut remapping_vals = RemappingMap::new();
            let db = Arc::clone(&input.graph.storage);
            #txn_type


            #(#query_stmts)*

            txn.commit().unwrap();

            Ok(input.request.out_fmt.create_response(&return_vals))
        }

        #[doc(hidden)]
        #[used]
        static #static_name: () = {
            inventory::submit! {
                ::helix_db::helix_gateway::router::router::HandlerSubmission(
                    ::helix_db::helix_gateway::router::router::Handler::new(
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
                        #fn_name
                    )
                )
            }
        };
    };
    expanded.into()
}

#[proc_macro_attribute]
pub fn debug_trace(args: TokenStream, input: TokenStream) -> TokenStream {
    let prefix = if args.is_empty() {
        "DEBUG".to_string()
    } else {
        args.to_string().trim_matches('"').to_string()
    };

    let input_fn = parse_macro_input!(input as ItemFn);

    let fn_name = &input_fn.sig.ident;
    let fn_vis = &input_fn.vis;
    let fn_sig = &input_fn.sig;
    let fn_block = &input_fn.block;
    let fn_attrs = &input_fn.attrs;

    let expanded = quote! {
        #(#fn_attrs)*
        #fn_vis #fn_sig {
            let __debug_result = (|| #fn_block)();

            #[cfg(feature = "debug-output")]
            {
                println!("[{} @ line: {}]", #prefix, line!());
                let lhs = format!("  └── {}() -> ", stringify!(#fn_name));
                let debug_str = format!("{:?}", __debug_result);
                let lines: Vec<&str> = debug_str.lines().collect();

                if lines.len() > 1 {
                    println!("{}{}", lhs, lines[0]);
                }
                // Add padding equal to lhs length to all subsequent lines
                let padding = " ".repeat(lhs.len());
                for line in lines.iter().skip(1) {
                    println!("{}{}", padding, line);
                }
            }
            __debug_result
        }
    };

    TokenStream::from(expanded)
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

                    let first = result.first().unwrap_or(&TraversalVal::Empty).clone();

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

            let first = result.next().unwrap_or(TraversalVal::Empty);

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
    let fn_name_str = fn_name.to_string();

    println!("fn_name_str: {fn_name_str}");
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
