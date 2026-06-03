// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
//
// Based on https://github.com/64bit/async-openai/ by Himanshu Neema
// Original Copyright (c) 2022 Himanshu Neema
// Licensed under MIT License (see ATTRIBUTIONS-Rust.md)
//
// Licensed under Apache 2.0

//! 本测试主要用于确认 `From` 相关宏规则是否正确。
use pagoda_protocols::types::EmbeddingInput;

fn embedding_input<T>(input: T) -> EmbeddingInput
where
    EmbeddingInput: From<T>,
{
    input.into()
}

#[test]
fn create_embedding_input() {
    let input = [1, 2, 3];
    let _ = embedding_input(&input);
    let _ = embedding_input(input);

    let input = [[1, 2, 3], [4, 5, 6], [7, 8, 9]];
    let _ = embedding_input(&input);
    let _ = embedding_input(input);

    let (s1, s2, s3) = ([1, 2, 3], [4, 5, 6], [7, 8, 9]);
    let input = [&s1, &s2, &s3];
    let _ = embedding_input(&input);
    let _ = embedding_input(input);

    let input = vec![1, 2, 3];
    let _ = embedding_input(&input);
    let _ = embedding_input(input);

    let input = vec![[1, 2, 3], [4, 5, 6], [7, 8, 9]];
    let _ = embedding_input(&input);
    let _ = embedding_input(input);

    let input = vec![vec![1, 2, 3], vec![4, 5, 6, 7], vec![8, 9, 10, 11, 12]];
    let _ = embedding_input(&input);
    let _ = embedding_input(input);

    let input = [vec![1, 2, 3], vec![4, 5, 6, 7], vec![8, 9, 10, 11, 12]];
    let _ = embedding_input(&input);
    let _ = embedding_input(input);

    let (v1, v2, v3) = (vec![1, 2, 3], vec![4, 5, 6, 7], vec![8, 9, 10, 11, 12]);
    let input = [&v1, &v2, &v3];
    let _ = embedding_input(&input);
    let _ = embedding_input(input);
}
