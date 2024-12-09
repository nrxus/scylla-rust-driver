// #![feature(prelude_import)]
// #[prelude_import]
// use std::prelude::rust_2021::*;
// #[macro_use]
// extern crate std;
// use scylla::{serialize::value::SerializeValue, SerializeRow};

// // #[derive(SerializeRow)]
// // struct Entry {
// //     a: i32,
// //     b: Option<i32>,
// //     #[scylla(flatten)]
// //     inner: Inner,
// // }

// // #[derive(SerializeRow)]
// // struct Inner {
// //     c: i32,
// //     x: bool,
// // }

// struct TestRowWithGenerics<'a, T: SerializeValue> {
//     a: &'a str,
//     b: T,
// }
// impl<'a, T: SerializeValue> ::scylla::_macro_internal::SerializeRow for TestRowWithGenerics<'a, T> {
//     fn serialize<'b>(
//         &self,
//         ctx: &::scylla::_macro_internal::RowSerializationContext,
//         writer: &mut ::scylla::_macro_internal::RowWriter<'b>,
//     ) -> ::std::result::Result<(), ::scylla::_macro_internal::SerializationError> {
//         struct _TestRowWithGenericsScyllaSerPartial<'a, 'scylla_ser_partial, T: SerializeValue> {
//             a: &'scylla_ser_partial &'a str,
//             b: &'scylla_ser_partial T,
//             missing: ::std::collections::HashSet<&'static str>,
//         }
//         impl<'a, 'scylla_ser_partial, T: SerializeValue>
//             ::scylla::_macro_internal::PartialSerializeRowByName
//             for _TestRowWithGenericsScyllaSerPartial<'a, 'scylla_ser_partial, T>
//         {
//             fn serialize_field<'b>(
//                 &mut self,
//                 spec: &::scylla::_macro_internal::ColumnSpec,
//                 writer: &mut ::scylla::_macro_internal::RowWriter<'b>,
//             ) -> ::std::result::Result<
//                 ::scylla::_macro_internal::FieldRowSerializationStatus,
//                 ::scylla::_macro_internal::SerializationError,
//             > {
//                 {
//                     match spec.name() {
//                         "a" => {
//                             let sub_writer =
//                                 ::scylla::_macro_internal::RowWriter::make_cell_writer(writer);
//                             <&'a str as ::scylla::_macro_internal::SerializeValue>::serialize(
//                                 &self.a,
//                                 spec.typ(),
//                                 sub_writer,
//                             )?;
//                             self.missing.remove("a");
//                         }
//                         "b" => {
//                             let sub_writer =
//                                 ::scylla::_macro_internal::RowWriter::make_cell_writer(writer);
//                             <T as ::scylla::_macro_internal::SerializeValue>::serialize(
//                                 &self.b,
//                                 spec.typ(),
//                                 sub_writer,
//                             )?;
//                             self.missing.remove("b");
//                         }
//                         _ => {
//                             let mk_err = |err| {
//                                 ::scylla::_macro_internal::SerializationError::new(::scylla::_macro_internal::BuiltinRowSerializationError {
//                                                 rust_name: std::any::type_name::<TestRowWithGenerics<'a,
//                                                         T>>(),
//                                                 kind: ::scylla::_macro_internal::BuiltinRowSerializationErrorKind::ColumnSerializationFailed {
//                                                     name: <_ as ::std::borrow::ToOwned>::to_owned(spec.name()),
//                                                     err,
//                                                 },
//                                             })
//                             };
//                             return Ok(
//                                 ::scylla::_macro_internal::FieldRowSerializationStatus::NotUsed,
//                             );
//                         }
//                     }
//                     if self.missing.is_empty() {
//                         Ok(::scylla::_macro_internal::FieldRowSerializationStatus::Done)
//                     } else {
//                         Ok(::scylla::_macro_internal::FieldRowSerializationStatus::NotDone)
//                     }
//                 }
//             }
//             fn check_missing(
//                 self,
//             ) -> ::std::result::Result<(), ::scylla::_macro_internal::SerializationError>
//             {
//                 let Some(missing) = self.missing.into_iter().nth(0) else {
//                     return Ok(());
//                 };
//                 match missing {
//                     _ =>
//                         ::std::result::Result::Err(::scylla::_macro_internal::SerializationError::new(::scylla::_macro_internal::BuiltinRowTypeCheckError {
//                                     rust_name: ::std::any::type_name::<TestRowWithGenerics<'a,
//                                             T>>(),
//                                     kind: ::scylla::_macro_internal::BuiltinRowTypeCheckErrorKind::ValueMissingForColumn {
//                                         name: missing.to_owned(),
//                                     },
//                                 })),
//                 }
//             }
//         }
//         #[allow(non_local_definitions)]
//         impl<'a, T: SerializeValue> ::scylla::_macro_internal::SerializeRowByName
//             for TestRowWithGenerics<'a, T>
//         {
//             type Partial<'scylla_ser_partial>
//                 = _TestRowWithGenericsScyllaSerPartial<'a, 'scylla_ser_partial, T>
//             where
//                 Self: 'scylla_ser_partial;
//             fn partial(&self) -> Self::Partial<'_> {
//                 _TestRowWithGenericsScyllaSerPartial {
//                     a: &self.a,
//                     b: &self.b,
//                     missing: ::std::collections::HashSet::from_iter(["a", "b"]),
//                 }
//             }
//         }
//         <Self as ::scylla::_macro_internal::SerializeRowByName>::serialize_by_name(
//             self, ctx, writer,
//         )
//     }
//     #[inline]
//     fn is_empty(&self) -> bool {
//         false
//     }
// }

fn main() {}
