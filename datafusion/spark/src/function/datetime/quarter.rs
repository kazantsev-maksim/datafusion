// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use arrow::array::{ArrayRef, AsArray, Int32Array};
use arrow::datatypes::{DataType, Date32Type, Field, FieldRef};
use chrono::Datelike;
use datafusion::logical_expr::{ColumnarValue, Signature, Volatility};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, ScalarValue, internal_err};
use datafusion_expr::{ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl};
use std::any::Any;
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkQuarter {
    signature: Signature,
}

impl Default for SparkQuarter {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkQuarter {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Date32], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkQuarter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "quarter"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be used instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Int32,
            args.arg_fields[0].is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [arg] = take_function_args("quarter", args.args)?;
        match arg {
            ColumnarValue::Scalar(ScalarValue::Date32(days)) => {
                if let Some(days) = days {
                    Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(
                        spark_quarter(days)?,
                    ))))
                } else {
                    Ok(ColumnarValue::Scalar(ScalarValue::Int32(None)))
                }
            }
            ColumnarValue::Array(array) => {
                let result = match array.data_type() {
                    DataType::Date32 => {
                        let result: Int32Array = array
                            .as_primitive::<Date32Type>()
                            .try_unary(spark_quarter)?
                            .with_data_type(DataType::Int32);
                        Ok(Arc::new(result) as ArrayRef)
                    }
                    other => {
                        internal_err!(
                            "Unsupported data type {other:?} for Spark function `quarter`"
                        )
                    }
                }?;
                Ok(ColumnarValue::Array(result))
            }
            other => {
                internal_err!("Unsupported arg {other:?} for Spark function `quarter")
            }
        }
    }
}

fn spark_quarter(days: i32) -> Result<i32> {
    let quarter = Date32Type::to_naive_date_opt(days).unwrap().quarter();
    Ok(quarter as i32)
}
