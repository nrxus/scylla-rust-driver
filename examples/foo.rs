use scylla::{
    frame::response::result::ColumnSpec,
    serialize::{
        row::{BuiltinTypeCheckErrorKind, RowSerializationContext, SerializeRow},
        value::SerializeValue,
        writers::RowWriter,
        SerializationError,
    },
};

use scylla::_macro_internal as krate;

pub struct NextSerializer<I> {
    columns: I,
}

impl<'i, 'c: 'i, I: Iterator<Item = &'i ColumnSpec<'c>>> NextSerializer<I> {
    pub fn next(
        &mut self,
        expected: &str,
    ) -> Result<&'i ColumnSpec<'c>, BuiltinTypeCheckErrorKind> {
        let spec = self.columns.next().ok_or_else(|| {
            BuiltinTypeCheckErrorKind::ValueMissingForColumn {
                name: expected.to_owned(),
            }
        })?;
        if spec.name() == expected {
            Ok(spec)
        } else {
            Err(BuiltinTypeCheckErrorKind::ColumnNameMismatch {
                rust_column_name: expected.to_owned(),
                db_column_name: spec.name().to_owned(),
            })
        }
    }

    pub fn next_for<T, V: SerializeValue>(
        &mut self,
        expected: &str,
        value: &V,
        writer: &mut RowWriter<'_>,
    ) -> Result<(), SerializationError> {
        let spec = self
            .next(expected)
            .map_err(krate::ser::row::mk_typck_err::<T>)?;
        value
            .serialize_spec(&spec, writer)
            .map_err(krate::ser::row::mk_ser_err::<T>)?;

        Ok(())
    }

    pub fn check_missing(&mut self) -> Result<(), BuiltinTypeCheckErrorKind> {
        let Some(spec) = self.columns.next() else {
            return Ok(());
        };

        Err(BuiltinTypeCheckErrorKind::NoColumnWithName {
            name: spec.name().to_owned(),
        })
    }
}

pub struct InOrderSerializer<'t, T: SerializeRowInOrder>(&'t T);

impl<T: SerializeRowInOrder> InOrderSerializer<'_, T> {
    pub fn serialize(
        self,
        ctx: &RowSerializationContext,
        writer: &mut RowWriter<'_>,
    ) -> Result<(), SerializationError> {
        let mut next_serializer = NextSerializer {
            columns: ctx.columns().iter(),
        };

        self.0.serialize_in_order(&mut next_serializer, writer)?;
        next_serializer
            .check_missing()
            .map_err(krate::ser::row::mk_typck_err::<T>)
    }
}

pub trait SerializeRowInOrder {
    fn serialize_in_order<'i, 'c: 'i, I: Iterator<Item = &'i ColumnSpec<'c>>>(
        &self,
        columns: &mut NextSerializer<I>,
        writer: &mut RowWriter<'_>,
    ) -> Result<(), SerializationError>;
}

struct Entry {
    a: i32,
    inner: Inner,
    b: Option<i32>,
}

impl SerializeRow for Entry {
    fn serialize<'b>(
        &self,
        ctx: &RowSerializationContext,
        writer: &mut RowWriter<'b>,
    ) -> Result<(), SerializationError> {
        #[allow(non_local_definitions)]
        impl SerializeRowInOrder for Entry {
            fn serialize_in_order<'i, 'c: 'i, I: Iterator<Item = &'i ColumnSpec<'c>>>(
                &self,
                columns: &mut NextSerializer<I>,
                writer: &mut RowWriter<'_>,
            ) -> Result<(), SerializationError> {
                columns.next_for::<Self, _>("a", &self.a, writer)?;
                self.inner.serialize_in_order(columns, writer)?;
                columns.next_for::<Self, _>("b", &self.b, writer)?;

                Ok(())
            }
        }

        InOrderSerializer(self).serialize(ctx, writer)
    }
    #[inline]
    fn is_empty(&self) -> bool {
        false
    }
}

struct Inner {
    c: i32,
    x: bool,
}
impl SerializeRow for Inner {
    fn serialize<'b>(
        &self,
        ctx: &RowSerializationContext,
        writer: &mut RowWriter<'b>,
    ) -> Result<(), SerializationError> {
        #[allow(non_local_definitions)]
        impl SerializeRowInOrder for Inner {
            fn serialize_in_order<'i, 'c: 'i, I: Iterator<Item = &'i ColumnSpec<'c>>>(
                &self,
                columns: &mut NextSerializer<I>,
                writer: &mut RowWriter<'_>,
            ) -> Result<(), SerializationError> {
                columns.next_for::<Self, _>("c", &self.c, writer)?;
                columns.next_for::<Self, _>("x", &self.x, writer)?;

                Ok(())
            }
        }

        InOrderSerializer(self).serialize(ctx, writer)
    }
    #[inline]
    fn is_empty(&self) -> bool {
        false
    }
}

fn main() {}
