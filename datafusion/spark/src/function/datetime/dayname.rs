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

use arrow::array::{ArrayRef, AsArray, StringArray};
use arrow::datatypes::{DataType, Date32Type, Field, FieldRef};
use chrono::Datelike;
use datafusion::logical_expr::{ColumnarValue, Signature, Volatility};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, ScalarValue, internal_err};
use datafusion_expr::{ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl};
use std::any::Any;
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkDayName {
    signature: Signature,
}

impl Default for SparkDayName {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkDayName {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Date32], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkDayName {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "dayname"
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
            DataType::Utf8,
            args.arg_fields[0].is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [arg] = take_function_args("dayname", args.args)?;
        match arg {
            ColumnarValue::Scalar(ScalarValue::Date32(days)) => {
                if let Some(days) = days {
                    Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                        spark_day_name(days),
                    ))))
                } else {
                    Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
                }
            }
            ColumnarValue::Array(array) => {
                let result = match array.data_type() {
                    DataType::Date32 => {
                        let result: StringArray = array
                            .as_primitive::<Date32Type>()
                            .iter()
                            .map(|x| x.map(spark_day_name))
                            .collect();
                        Ok(Arc::new(result) as ArrayRef)
                    }
                    other => {
                        internal_err!(
                            "Unsupported data type {other:?} for Spark function `dayname`"
                        )
                    }
                }?;
                Ok(ColumnarValue::Array(result))
            }
            other => {
                internal_err!("Unsupported arg {other:?} for Spark function `dayname`")
            }
        }
    }
}

fn spark_day_name(days: i32) -> String {
    let weekday = Date32Type::to_naive_date_opt(days).unwrap().weekday();
    let display_name = get_display_name(weekday.num_days_from_monday());
    display_name.unwrap()
}

fn get_display_name(day: u32) -> Option<String> {
    match day {
        0 => Some(String::from("Mon")),
        1 => Some(String::from("Tue")),
        2 => Some(String::from("Wed")),
        3 => Some(String::from("Thu")),
        4 => Some(String::from("Fri")),
        5 => Some(String::from("Sat")),
        6 => Some(String::from("Sun")),
        _ => None,
    }
}
