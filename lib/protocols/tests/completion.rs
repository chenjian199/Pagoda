// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
//
// Based on https://github.com/64bit/async-openai/ by Himanshu Neema
// Original Copyright (c) 2022 Himanshu Neema
// Licensed under MIT License (see ATTRIBUTIONS-Rust.md)
//
// Licensed under Apache 2.0

//! 本测试主要用于确认 `From` 相关宏规则是否正确。
use pagoda_protocols::types::Prompt;

fn prompt_input<T>(input: T) -> Prompt
where
    Prompt: From<T>,
{
    input.into()
}

#[test]
fn create_prompt_input() {
    let prompt = "This is &str prompt";
    let _ = prompt_input(prompt);

    let prompt = "This is String".to_string();
    let _ = prompt_input(&prompt);
    let _ = prompt_input(prompt);

    let prompt = vec!["This is first", "This is second"];
    let _ = prompt_input(&prompt);
    let _ = prompt_input(prompt);

    let prompt = vec!["First string".to_string(), "Second string".to_string()];
    let _ = prompt_input(&prompt);
    let _ = prompt_input(prompt);

    let first = "First".to_string();
    let second = "Second".to_string();
    let prompt = vec![&first, &second];
    let _ = prompt_input(&prompt);
    let _ = prompt_input(prompt);

    let prompt = ["first", "second"];
    let _ = prompt_input(&prompt);
    let _ = prompt_input(prompt);

    let prompt = ["first".to_string(), "second".to_string()];
    let _ = prompt_input(&prompt);
    let _ = prompt_input(prompt);

    let first = "First".to_string();
    let second = "Second".to_string();
    let prompt = [&first, &second];
    let _ = prompt_input(&prompt);
    let _ = prompt_input(prompt);
}
