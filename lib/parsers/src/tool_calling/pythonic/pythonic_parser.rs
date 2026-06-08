// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
// SPDX-License-Identifier: Apache-2.0

//! # tool_calling::pythonic::pythonic_parser
//!
//! ## 设计意图
//! 解析「Pythonic」工具调用：模型以 `[tool1(arg1=val1, arg2=val2), tool2(...)]` 这种
//! 类 Python 字面量数组表达调用。先用正则定位整段调用，再用 rustpython 解析 AST
//! 取出函数名与关键字参数。
//!
//! ## 外部契约
//! - `parse_tool_calls(src)`：把一段 Python 列表表达式解析为 `Vec<ToolCallResponse>`，
//!   id 形如 `call-<序号>`（从 1 起）。
//! - `try_tool_call_parse_pythonic(message, tools)`：返回 `(calls, normal_text)`。
//! - `detect_tool_call_start_pythonic(chunk)`：含 `[` 即视为可能起始。
//!
//! ## 实现要点
//! - 仅接受常量、列表、字典作为参数值；`**kwargs` 与非常量参数被跳过。
//! - 大整数无法落入 i64/u64 时退化为字符串，避免精度丢失。

use super::super::ToolDefinition;
use super::response::{CalledFunction, ToolCallResponse, ToolCallType};
use regex::Regex;
use rustpython_parser::{
    Mode,
    ast::{Constant, Expr, Mod},
    parse,
};
use serde_json::{Number, Value, json};
use std::sync::OnceLock;

static PYTHONIC_REGEX: OnceLock<Regex> = OnceLock::new();


// === SECTION: 正则与文本预处理 ===

/// 取得（惰性编译一次）匹配 Pythonic 工具调用的正则。
fn get_pythonic_regex() -> &'static Regex {
    PYTHONIC_REGEX.get_or_init(|| {
        let pattern = r"\[([a-zA-Z]+\w*\(([a-zA-Z]+\w*=.*?,\s*)*([a-zA-Z]+\w*=.*?\s?)?\),\s*)*([a-zA-Z]+\w*\(([a-zA-Z]+\w*=.*?,\s*)*([a-zA-Z]+\w*=.*?\s*)?\)\s*)+\]";
        Regex::new(pattern).expect("Failed to compile pythonic regex pattern")
    })
}

/// 去除可能出现的 python 包裹标记。
fn strip_text(message: &str) -> String {
    message
        .replace("<|python_start|>", "")
        .replace("<|python_end|>", "")
}

/// 返回正则在文本中命中的所有片段。
fn get_regex_matches(message: &str) -> Vec<String> {
    get_pythonic_regex()
        .find_iter(message)
        .map(|cap| cap.as_str().to_string())
        .collect()
}

// === SECTION: AST 解析 ===

pub fn parse_tool_calls(src: &str) -> anyhow::Result<Vec<ToolCallResponse>> {
    let ast = parse(src, Mode::Expression, "<input>")?;

    /*
    AST: Expression(ModExpression {
        range: (),
        body: List(ExprList {
            range: 0..25,
            elts: [Call(...), Call(...)]
            ctx: Load
        })
    })
    */
    // 仅接受顶层为表达式且其 body 为列表的形态
    let Mod::Expression(mod_expr) = ast else {
        return Ok(vec![]);
    };
    let Expr::List(expr_list) = *mod_expr.body else {
        return Ok(vec![]);
    };

    let mut res = Vec::with_capacity(expr_list.elts.len());
    for (idx, elt) in expr_list.elts.iter().enumerate() {
        // 每个元素须是函数调用
        let Expr::Call(call) = elt else {
            continue;
        };
        // 被调对象须是裸标识符（函数名）
        let Expr::Name(name) = call.func.as_ref() else {
            continue;
        };
        let name = name.id.clone();

        let mut obj = serde_json::Map::new();
        for keyword in call.keywords.iter() {
            let Some(arg_ident) = keyword.arg.as_ref() else {
                tracing::debug!(
                    "Skipping **kwargs in pythonic tool call for function {}",
                    name
                );
                continue;
            };

            match const_expr(&keyword.value) {
                Ok(value) => {
                    obj.insert(arg_ident.to_string(), value);
                }
                Err(e) => {
                    tracing::debug!("Skipping non-constant argument {}: {}", arg_ident, e);
                }
            }
        }

        res.push(ToolCallResponse {
            id: format!("call-{}", idx + 1),
            tp: ToolCallType::Function,
            function: CalledFunction {
                name: name.to_string(),
                arguments: serde_json::to_string(&Value::Object(obj))?,
            },
        });
    }
    Ok(res)
}

/// 把单个表达式求值为 JSON 值，仅支持常量、列表、字典。
fn const_expr(e: &Expr) -> Result<Value, Box<dyn std::error::Error>> {
    match e {
        Expr::Constant(constant) => Ok(match &constant.value {
            Constant::Bool(b) => json!(b),
            Constant::None => Value::Null,
            Constant::Int(i) => {
                // 优先落入 i64/u64，越界则退化为字符串
                use num_traits::ToPrimitive;
                if let Some(v) = i.to_i64() {
                    Value::Number(Number::from(v))
                } else if let Some(v) = i.to_u64() {
                    Value::Number(Number::from(v))
                } else {
                    Value::String(i.to_string())
                }
            }
            Constant::Float(f) => json!(f),
            Constant::Str(s) => json!(s),
            _ => return Err("unsupported constant type".into()),
        }),
        // Python 列表按表达式处理
        Expr::List(expr_list) => {
            let list_values = expr_list
                .elts
                .iter()
                .map(const_expr)
                .collect::<Result<Vec<Value>, _>>()?;
            Ok(json!(list_values))
        }
        // Python 字典按表达式处理
        Expr::Dict(expr_dict) => {
            let mut dict_map = std::collections::HashMap::new();
            for (key_expr, value_expr) in expr_dict.keys.iter().zip(expr_dict.values.iter()) {
                // JSON 键须为字符串；非字符串键退化为其字符串表示
                let key = match key_expr {
                    Some(k) => match const_expr(k)? {
                        Value::String(s) => s,
                        other => other.to_string(),
                    },
                    None => {
                        return Err(
                            "dictionary unpacking (**kwargs) not supported in constants".into()
                        );
                    }
                };
                dict_map.insert(key, const_expr(value_expr)?);
            }
            Ok(json!(dict_map))
        }
        _ => Err("only constant values, lists, and dicts are allowed".into()),
    }
}

// === SECTION: 顶层解析入口与探测 ===

pub fn try_tool_call_parse_pythonic(
    message: &str,
    _tools: Option<&[ToolDefinition]>,
) -> anyhow::Result<(Vec<ToolCallResponse>, Option<String>)> {
    let stripped = strip_text(message).trim().to_string();

    // 空输入直接返回
    if stripped.is_empty() {
        return Ok((vec![], Some(String::new())));
    }

    let matches = get_regex_matches(&stripped);
    let Some(first) = matches.first() else {
        return Ok((vec![], Some(stripped)));
    };

    let tool_response = parse_tool_calls(first);

    // 普通文本为首个命中之前的内容
    let normal_text = stripped
        .split(first.as_str())
        .next()
        .unwrap_or("") // Safety: `split()` always returns at least one element
        .trim()
        .to_string();

    Ok((tool_response?, Some(normal_text)))
}

pub fn detect_tool_call_start_pythonic(chunk: &str) -> bool {
    let trimmed = chunk.trim();
    if trimmed.is_empty() {
        return false;
    }
    trimmed.contains('[')
}

#[cfg(test)]
mod tests {
    //! ## 测试过程
    //! 围绕公开 API（`try_tool_call_parse_pythonic`、`detect_tool_call_start_pythonic`）
    //! 与若干内部辅助（`strip_text`/`get_regex_matches`）展开：覆盖纯文本剥离、正则定位、
    //! 基本/前后文/换行/无调用/python 标记包裹、列表与字典参数，以及流式起始探测。
    //!
    //! ## 意义
    //! 保证 Pythonic 形态的工具调用在各类噪声与嵌套结构下都能稳定抽取函数名与参数，
    //! 并正确分离普通文本。
    use super::*;

    fn extract_name_and_args(call: ToolCallResponse) -> (String, serde_json::Value) {
        let args: serde_json::Value = serde_json::from_str(&call.function.arguments).unwrap();
        (call.function.name, args)
    }

    #[test] // helper
    fn test_strip_text() {
        let message = "Hello, world!";
        let stripped = strip_text(message);
        assert_eq!(stripped, "Hello, world!");

        let message = "<|python_start|>foo(a=1, b=2)<|python_end|>";
        let stripped = strip_text(message);
        assert_eq!(stripped, "foo(a=1, b=2)");

        let message = "<|python_start|>foo(a=1, b=2)";
        let stripped = strip_text(message);
        assert_eq!(stripped, "foo(a=1, b=2)");

        let message = "foo(a=1, b=2)<|python_end|>";
        let stripped = strip_text(message);
        assert_eq!(stripped, "foo(a=1, b=2)");
    }

    #[test] // helper
    fn test_get_regex_matches_simple_case() {
        let message = "[foo(a=1, b=2), bar(x=3)]";
        let matches = get_regex_matches(message);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0], "[foo(a=1, b=2), bar(x=3)]");
    }

    #[test] // helper
    fn test_get_regex_matches_text_before_and_after() {
        let message = "Hey yo ! [foo(a=1, b=2), bar(x= 3)] Hey yo";
        let matches = get_regex_matches(message);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0], "[foo(a=1, b=2), bar(x= 3)]");
    }

    #[test] // helper, PARSER.batch.7
    fn test_get_regex_matches_new_line_in_arg_and_value() {
        let message = "Hey \n yo ! [foo(a=1,b=2), \n bar(x=3)] Hey yo";
        let matches = get_regex_matches(message);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0], "[foo(a=1,b=2), \n bar(x=3)]");
    }

    #[test] // helper
    fn test_get_regex_matches_no_call() {
        let message = "Hey yo !";
        let matches = get_regex_matches(message);
        assert_eq!(matches.len(), 0);
    }

    #[test] // PARSER.batch.2
    fn test_parse_tool_call_parse_pythonic_basic() {
        let message = "[foo(a=1, b=2), bar(x=3)]";
        let (result, content) = try_tool_call_parse_pythonic(message, None).unwrap();
        assert_eq!(content, Some("".to_string()));
        assert!(!result.is_empty());
        assert_eq!(result.len(), 2);
        let (name, args) = extract_name_and_args(result[0].clone()); // TODO: Add support for normal text
        assert_eq!(name, "foo");
        assert_eq!(args["a"], 1);
        assert_eq!(args["b"], 2);
        let (name, args) = extract_name_and_args(result[1].clone());
        assert_eq!(name, "bar");
        assert_eq!(args["x"], 3);
    }

    #[test] // PARSER.batch.2, PARSER.batch.8
    fn test_parse_tool_call_parse_pythonic_with_text() {
        let message = "Hey yo ! [foo(a=1, b=2), bar(x=3)] Hey yo";
        let (result, content) = try_tool_call_parse_pythonic(message, None).unwrap();
        assert_eq!(content, Some("Hey yo !".to_string()));
        assert!(!result.is_empty());
        assert_eq!(result.len(), 2);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "foo");
        assert_eq!(args["a"], 1);
        assert_eq!(args["b"], 2);
        let (name, args) = extract_name_and_args(result[1].clone());
        assert_eq!(name, "bar");
        assert_eq!(args["x"], 3);
    }

    #[test] // PARSER.batch.2, PARSER.batch.8, PARSER.fmt.2
    fn test_parse_tool_call_parse_pythonic_with_text_and_new_line() {
        let message = "Hey \n yo ! [foo(a=1, b=2), bar(x=3)] Hey yo";
        let (result, content) = try_tool_call_parse_pythonic(message, None).unwrap();
        assert_eq!(content, Some("Hey \n yo !".to_string()));
        assert!(!result.is_empty());
        assert_eq!(result.len(), 2);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "foo");
        assert_eq!(args["a"], 1);
        assert_eq!(args["b"], 2);
        let (name, args) = extract_name_and_args(result[1].clone());
        assert_eq!(name, "bar");
        assert_eq!(args["x"], 3);
    }

    #[test] // PARSER.batch.3
    fn test_parse_tool_call_parse_pythonic_with_no_calls() {
        let message = "Hey \n yo !";
        let (result, content) = try_tool_call_parse_pythonic(message, None).unwrap();
        assert_eq!(content, Some("Hey \n yo !".to_string()));
        assert!(result.is_empty());
        assert_eq!(result.len(), 0)
    }

    #[test] // PARSER.batch.2, PARSER.fmt.3
    fn test_parse_tool_call_parse_pythonic_with_python_tags() {
        let message = "<|python_start|>[foo(a=1, b=2), bar(x=3)]<|python_end|>";
        let (result, content) = try_tool_call_parse_pythonic(message, None).unwrap();
        assert_eq!(content, Some("".to_string()));
        assert!(!result.is_empty());
        assert_eq!(result.len(), 2);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "foo");
        assert_eq!(args["a"], 1);
        assert_eq!(args["b"], 2);
        let (name, args) = extract_name_and_args(result[1].clone());
        assert_eq!(name, "bar");
        assert_eq!(args["x"], 3);
    }

    #[test] // PARSER.batch.7
    fn test_parse_tool_call_parse_pythonic_with_list_arg_values() {
        let message = "[foo(a=[1, 2, 3], b=2), bar(x=[3, 4, 5])]";
        let (result, _) = try_tool_call_parse_pythonic(message, None).unwrap();
        assert!(!result.is_empty());
        assert_eq!(result.len(), 2);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "foo");
        assert_eq!(args["a"], json!([1, 2, 3]));
        assert_eq!(args["b"], 2);
        let (name, args) = extract_name_and_args(result[1].clone());
        assert_eq!(name, "bar");
        assert_eq!(args["x"], json!([3, 4, 5]));
    }

    #[test] // PARSER.batch.7
    fn test_parse_tool_call_parse_pythonic_with_dict_arg_values() {
        let message = "[foo(a={'a': 1, 'b': 2}, b=2), bar(x={'x': 3, 'y': {'e': 'f'}})]";
        let (result, _) = try_tool_call_parse_pythonic(message, None).unwrap();
        assert!(!result.is_empty());
        assert_eq!(result.len(), 2);
        let (name, args) = extract_name_and_args(result[0].clone());
        assert_eq!(name, "foo");
        assert_eq!(args["a"], json!({"a": 1, "b": 2}));
        assert_eq!(args["b"], 2);
        let (name, args) = extract_name_and_args(result[1].clone());
        assert_eq!(name, "bar");
        assert_eq!(args["x"], json!({"x": 3, "y": {"e": "f"}}));
    }

    // === 流式起始探测 ===

    #[test] // helper
    fn test_detect_tool_call_start_pythonic_chunk_with_tool_call_start_token() {
        let text = r#"[foo(a=1, b=2), bar(x=3)]"#;
        let result = detect_tool_call_start_pythonic(text);
        assert!(result);
    }

    #[test] // helper
    fn test_detect_tool_call_start_pythonic_chunk_without_tool_call_start_token() {
        let text = r#"foo(a=1, b=2)"#;
        let result = detect_tool_call_start_pythonic(text);
        assert!(!result);
    }

    #[test] // helper
    fn test_detect_tool_call_start_pythonic_chunk_with_tool_call_start_token_in_middle() {
        let text = r#"information: [foo(a=1, b=2), bar(x=3)]"#;
        let result = detect_tool_call_start_pythonic(text);
        assert!(result);
    }

    #[test] // helper
    fn test_detect_tool_call_start_pythonic_false_positive() {
        let text = r#"Hey [ There is one tool call here . foo(a=1, b=2)"#;
        let result = detect_tool_call_start_pythonic(text);
        assert!(result);
    }
}
