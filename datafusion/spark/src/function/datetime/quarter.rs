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

use arrow::array::{Array, ArrayRef};
use arrow::compute::{CastOptions, DatePart, cast_with_options, date_part};
use arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
use datafusion::logical_expr::{ColumnarValue, Signature, TypeSignature, Volatility};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, internal_err};
use datafusion_expr::{ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_functions::utils::make_scalar_function;
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
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::Utf8View]),
                    TypeSignature::Exact(vec![DataType::LargeUtf8]),
                    TypeSignature::Exact(vec![DataType::Date32]),
                    TypeSignature::Exact(vec![DataType::Timestamp(
                        TimeUnit::Millisecond,
                        None,
                    )]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkQuarter {
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
        make_scalar_function(spark_quarter, vec![])(&args.args)
    }
}

fn spark_quarter(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [array] = take_function_args("quarter", args)?;
    match array.data_type() {
        DataType::Date32 | DataType::Timestamp(_, _) => {
            let quarter = date_part(array, DatePart::Quarter)?;
            Ok(quarter)
        }
        DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => {
            let date_array =
                cast_with_options(array, &DataType::Date32, &CastOptions::default())?;
            let quarter = date_part(&date_array, DatePart::Quarter)?;
            Ok(quarter)
        }
        data_type => {
            internal_err!("quarter does not support: {data_type}")
        }
    }
}
