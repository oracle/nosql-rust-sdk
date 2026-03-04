//
// Copyright (c) 2024, 2026 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//
extern crate proc_macro;
extern crate proc_macro2;
extern crate syn;
#[macro_use]
extern crate quote;

use proc_macro::TokenStream;
use proc_macro2::{TokenStream as TokenStream2, TokenTree};
use syn::{
    parse::Parser, parse_macro_input, Data, DeriveInput, GenericArgument, Meta, PathArguments,
    Type, TypePath,
};

/// Derive macro to specify a struct that can be written directly into, and read directly from, a
/// NoSQL table row.
///
/// The single `nosql` attribute can be used to rename a field using the `column` key, and/or to specify
/// its NoSQL Database field type using the `type` key (for example, from a Rust `i32` to a NoSQL `long`).
///
/// See the documentation of [`PutRequest::put()`](../struct.PutRequest.html#method.put) for
/// example usage of this macro to put and get native structs to and from a NoSQL Database table.
#[proc_macro_derive(NoSQLRow, attributes(nosql))]
pub fn to_from_map_value(input: TokenStream) -> TokenStream {
    // Parse input tokens into a syntax tree
    let input = parse_macro_input!(input as DeriveInput);

    // Build the trait implementation
    impl_to_from_map_value(input)
}

fn impl_to_from_map_value(input: DeriveInput) -> TokenStream {
    //println!(" impl_to_from_map_value: DeriveInput = {:#?}", input);
    let name = &input.ident;
    let name_string = name.to_string();

    // check that input.data is Struct (vs Enum vs Union)
    let ds;
    if let Data::Struct(d) = input.data {
        ds = d;
    } else {
        panic!("NoSQLRow only supports Struct datatypes");
    }

    #[derive(Debug)]
    struct FieldNameType {
        fname: String,
        alias: Option<String>,
        // TODO: ftype: String,
    }

    let mut fntypes: Vec<FieldNameType> = Vec::new();

    for field in ds.fields {
        // get column name from field name
        // if "column" attribute given, use that

        let mut alias: Option<String> = None;
        for a in field.attrs {
            let mut good: bool = false;
            if let Meta::List(l) = a.meta {
                for s in l.path.segments {
                    if s.ident == "nosql" {
                        good = true;
                        break;
                    }
                }
                if good == false {
                    continue;
                }
                // we now have a "nosql" attribute list
                // TODO
                let mut is_column: bool = false;
                for t in l.tokens {
                    //println!(" token={:?}", t);
                    match t {
                        TokenTree::Ident(i) => {
                            if is_column {
                                alias = Some(i.to_string());
                                break;
                            }
                            if i.to_string() == "column" {
                                is_column = true;
                            } else {
                                is_column = false;
                            }
                        }
                        //TokenTree::Punct(p) => println!("found a punct"),
                        _ => (),
                    }
                }
                if alias.is_some() {
                    break;
                }
            }
        }

        let fname = if let Some(id) = field.ident {
            id.to_string()
        } else {
            panic!("Field in NoSQLRow is missing ident");
        };

        // get field type, put in ftypes vector
        // if "type" attribute given, use that
        // otherwise, infer from rust type
        let _ftype = if let Type::Path(p) = field.ty {
            //ftypes.push(get_path_segment(&p, ""));
            get_path_segment(&p, "")
        } else {
            panic!("Field type in NoSQLRow does not have Path element");
        };
        fntypes.push(FieldNameType { fname, alias });
        //fntypes.push(FieldNameType{fname, alias, ftype});
    }

    //println!("fntypes: {:?}", fntypes);

    let mut tbody = TokenStream2::default();
    let mut fbody = TokenStream2::default();
    for f in fntypes {
        let fname = format_ident!("{}", f.fname);
        let fnameq: String;
        match f.alias {
            Some(s) => fnameq = s,
            None => fnameq = f.fname,
        }
        tbody.extend(quote! {
            m.put(#fnameq, &self.#fname);
        });
        fbody.extend(quote! {
            self.#fname = self.#fname.from_map(#fnameq, value)?;
        });
    }

    let expanded = quote! {
        impl NoSQLRow for #name {
            fn to_map_value(&self) -> Result<MapValue, oracle_nosql_rust_sdk::NoSQLError> {
                let mut m = MapValue::new();
                #tbody
                Ok(m)
            }

            fn from_map_value(&mut self, value: &MapValue) -> Result<(), oracle_nosql_rust_sdk::NoSQLError> {
                #fbody
                Ok(())
            }
        }

        impl NoSQLColumnToFieldValue for #name {
            fn to_field_value(&self) -> FieldValue {
                let m = self.to_map_value();
                if let Ok(mv) = m {
                    return FieldValue::Map(mv);
                }
                // TODO: How to expose this error??
                FieldValue::Null
            }
        }

        impl NoSQLColumnFromFieldValue for #name {
            fn from_field(fv: &FieldValue) -> Result<Self, oracle_nosql_rust_sdk::NoSQLError> {
                if let FieldValue::Map(v) = fv {
                    let mut s: #name = Default::default();
                    s.from_map_value(v)?;
                    return Ok(s);
                }
                Err(oracle_nosql_rust_sdk::NoSQLError::new(
                    oracle_nosql_rust_sdk::NoSQLErrorCode::IllegalArgument,
                    format!("NoSQL: Error converting field into {}: expected FieldValue::Map, actual: {:?}", #name_string, fv).as_str()))
            }
        }

    };

    //println!("expanded=\n{}\n", expanded);
    // Return the generated impl
    TokenStream::from(expanded)
}

fn get_path_segment(p: &TypePath, val: &str) -> String {
    for elem in &p.path.segments {
        let mut s = elem.ident.to_string();
        //println!("gps: elem={}", s);
        if let PathArguments::AngleBracketed(args) = &elem.arguments {
            for a in &args.args {
                if let GenericArgument::Type(tp) = a {
                    if let Type::Path(p1) = tp {
                        //println!("gps: recursing");
                        s = val.to_string() + &s;
                        return get_path_segment(&p1, &s);
                    }
                }
            }
            // TODO: error message?
            //println!("gps: error");
            return val.to_string();
        }
        // return after first element
        s = val.to_string() + &s;
        //println!("gps: returning {}", s);
        return s.to_string();
    }
    //println!("gps: returning empty={}", val);
    val.to_string()
}

/// (internal use only)
#[proc_macro_attribute]
pub fn add_planiter_fields(args: TokenStream, input: TokenStream) -> TokenStream {
    let mut item_struct = parse_macro_input!(input as syn::ItemStruct);
    let _ = parse_macro_input!(args as syn::parse::Nothing);

    if let syn::Fields::Named(ref mut fields) = item_struct.fields {
        //fields.named.push(syn::Field::parse_named.parse2(quote! { state: PlanIterState }).unwrap());
        fields.named.push(
            syn::Field::parse_named
                .parse2(quote! { result_reg: i32 })
                .unwrap(),
        );
        fields.named.push(
            syn::Field::parse_named
                .parse2(quote! { loc: Location })
                .unwrap(),
        );
    }

    return quote! {
        #item_struct
    }
    .into();
}
