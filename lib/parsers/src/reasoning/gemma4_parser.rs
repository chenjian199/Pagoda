// SPDX-FileCopyrightText: Copyright (c) 2026-2028 PAGODA.
// SPDX-License-Identifier: Apache-2.0

//! ## 设计意图
//! Gemma 4 在专用通道分隔符之间发出思维链推理：
//!
//! ```text
//! <|channel>thought
//! ...chain of thought reasoning...<channel|>
//! Final answer text.
//! ```
//!
//! 通道内的 `thought\n` 角色标签是结构性产物（类比 `<|turn>user\n...` 中的 `user\n`）。
//! 下游期望不带该标签的推理内容，故本解析器将其剥除。
//!
//! ## 外部契约
//! - `Gemma4ReasoningParser`（`new`/`default`）实现 `ReasoningParser`。
//! - 起止标记均为特殊 token，仅当推理引擎配置 `skip_special_tokens=False` 时才可见于解码文本；
//!   若标记在解析器看到前已被剥除，则回退为整段透传（不抽取推理）。
//! - 缺起始仅有结束标记时，结束前文本仍按推理处理（与上游离线解析一致）。
//!
//! ## 实现要点
//! - 流式用 buffer 累积，`overlap` 检测跨块切断的部分多字节标记。
//! - `resolve_prefix` 在流式增量下逐步决定 `thought\n` 前缀如何剥除/保留。

use crate::ParserResult;
use crate::ReasoningParser;

const START_TOKEN: &str = "<|channel>";
const END_TOKEN: &str = "<channel|>";
const THOUGHT_PREFIX: &str = "thought\n";

/// 返回 `s` 的最长后缀且同时是 `delim` 前缀的长度。用于检测跨流式块边界切断的部分
/// 多字节标记（例如 `"...<|chan"` 是 `<|channel>` 的部分前缀）。
fn overlap(s: &str, delim: &str) -> usize {
    let max = delim.len().min(s.len());
    (1..=max)
        .rev()
        .filter(|&i| delim.is_char_boundary(i) && s.is_char_boundary(s.len() - i))
        .find(|&i| s.ends_with(&delim[..i]))
        .unwrap_or(0)
}

#[derive(Debug, Clone)]
pub struct Gemma4ReasoningParser {
    /// 尚未分类的累积文本的流式缓冲。
    buffer: String,
    /// 一旦观察到 `<|channel>` 且处于推理段内即为 true。
    in_reasoning: bool,
    /// 当前推理段的 `thought\n` 前缀已被剥除（或确定不存在）即为 true。
    prefix_resolved: bool,
    /// 当前段已累积的推理文本。用于判断累积字节仍是 `thought\n` 的严格前缀（情形 2）
    /// 还是已发生分歧（情形 3）。
    reasoning_accum: String,
}

impl Gemma4ReasoningParser {
    pub fn new() -> Self {
        Self {
            buffer: String::new(),
            in_reasoning: false,
            prefix_resolved: false,
            reasoning_accum: String::new(),
        }
    }

    fn reset_span(&mut self) {
        self.in_reasoning = false;
        self.prefix_resolved = false;
        self.reasoning_accum.clear();
    }
}

impl Default for Gemma4ReasoningParser {
    fn default() -> Self {
        Self::new()
    }
}

/// 若 `text` 以 `thought\n` 角色标签开头则剥除之。
fn strip_thought_prefix(text: &str) -> &str {
    text.strip_prefix(THOUGHT_PREFIX).unwrap_or(text)
}

/// 在当前前缀剥除状态下，决定从 `raw_reasoning` 发出哪一段。返回
/// `(emit, new_prefix_resolved)`。
///
/// **前置条件：** `raw_reasoning` 必须是 `accum` 的后缀——即调用方已先把新增量推入
/// 累积器再调用本函数。据此计算 `prev_len = accum.len() - raw_reasoning.len()`
/// （本增量之前累积器的长度）。违反前置条件会使 `prev_len` 下溢并破坏发出片段。
/// `parse_reasoning_streaming_incremental` 的流式驱动通过在调用前总是把 `raw` 推入
/// `self.reasoning_accum` 来维持该不变量。
///
/// 情形 1：累积推理以 `thought\n` 开头——从增量中剥除它（或当增量整体落在前缀内时全部抑制）。
/// 情形 2：累积推理是 `thought\n` 的严格前缀——抑制，待更多字节到达再判定。
/// 情形 3：累积推理已偏离前缀——原样发出缓冲推理（数据保留）。
fn resolve_prefix<'a>(accum: &'a str, raw_reasoning: &'a str) -> (&'a str, bool) {
    debug_assert!(
        accum.ends_with(raw_reasoning),
        "resolve_prefix precondition violated: raw_reasoning ({:?}) must be a suffix of accum ({:?})",
        raw_reasoning,
        accum,
    );
    if accum.starts_with(THOUGHT_PREFIX) {
        let prev_len = accum.len() - raw_reasoning.len();
        if prev_len >= THOUGHT_PREFIX.len() {
            // 前缀已被更早的增量消费——透传。
            return (raw_reasoning, true);
        }
        let chars_of_prefix_in_delta = THOUGHT_PREFIX.len() - prev_len;
        let stripped = &raw_reasoning[chars_of_prefix_in_delta.min(raw_reasoning.len())..];
        if !stripped.is_empty() || accum.len() >= THOUGHT_PREFIX.len() {
            return (stripped, true);
        }
        return ("", false);
    }
    if THOUGHT_PREFIX.starts_with(accum) {
        // 是 "thought\n" 的严格前缀——抑制，待判定。
        return ("", false);
    }
    // 已分歧：原样发出全部缓冲推理。
    (accum, true)
}

impl ReasoningParser for Gemma4ReasoningParser {
    fn detect_and_parse_reasoning(&mut self, text: &str, _token_ids: &[u32]) -> ParserResult {
        // 非流式路径：已有完整文本，直接用普通字符串操作。
        let pass_through = |t: &str| ParserResult {
            normal_text: t.to_string(),
            reasoning_text: String::new(),
        };

        match (text.find(START_TOKEN), text.find(END_TOKEN)) {
            // 完全没有可见的推理标记（模型未发出，或 skip_special_tokens 已剥除）。
            (None, None) => pass_through(text),
            (Some(s), end_opt) => {
                let pre = &text[..s];
                let rest = &text[s + START_TOKEN.len()..];
                // 结束标记须在起始标记之后才有效
                let (reasoning_raw, post) = match end_opt
                    .filter(|e| *e > s + START_TOKEN.len())
                    .map(|e| e - (s + START_TOKEN.len()))
                {
                    Some(end_rel) => (&rest[..end_rel], &rest[end_rel + END_TOKEN.len()..]),
                    None => (rest, ""),
                };
                ParserResult {
                    normal_text: format!("{pre}{post}"),
                    reasoning_text: strip_thought_prefix(reasoning_raw).to_string(),
                }
            }
            // 缺起始仅有结束标记——上游离线解析仍把结束前文本视为推理。
            // 镜像此行为，使起始标记被剥除（如 tokenizer skip_special_tokens）时不致丢失推理内容。
            (None, Some(e)) => ParserResult {
                normal_text: text[e + END_TOKEN.len()..].to_string(),
                reasoning_text: strip_thought_prefix(&text[..e]).to_string(),
            },
        }
    }

    fn parse_reasoning_streaming_incremental(
        &mut self,
        text: &str,
        _token_ids: &[u32],
    ) -> ParserResult {
        // 把本增量与遗留缓冲合并以便前缀检测。
        let mut work = std::mem::take(&mut self.buffer);
        work.push_str(text);

        let mut normal = String::new();
        let mut reasoning_emit = String::new();

        // 在推理段内累积 `raw` 并按前缀状态决定发出内容。
        let push_reasoning =
            |raw: &str, accum: &mut String, prefix_resolved: &mut bool, emit: &mut String| {
                accum.push_str(raw);
                if *prefix_resolved {
                    emit.push_str(raw);
                } else {
                    let (slice, resolved) = resolve_prefix(accum, raw);
                    if resolved {
                        emit.push_str(slice);
                        *prefix_resolved = true;
                    }
                }
            };

        loop {
            if !self.in_reasoning {
                // 寻找完整起始标记，或缓冲末尾的部分前缀（须留存）。
                if let Some(idx) = work.find(START_TOKEN) {
                    normal.push_str(&work[..idx]);
                    work = work[idx + START_TOKEN.len()..].to_string();
                    self.in_reasoning = true;
                    self.prefix_resolved = false;
                    self.reasoning_accum.clear();
                    continue;
                }
                let lap = overlap(&work, START_TOKEN);
                if lap > 0 {
                    let split = work.len() - lap;
                    normal.push_str(&work[..split]);
                    self.buffer = work[split..].to_string();
                } else {
                    normal.push_str(&work);
                    self.buffer.clear();
                }
                break;
            }

            if let Some(idx) = work.find(END_TOKEN) {
                let raw = work[..idx].to_string();
                push_reasoning(
                    &raw,
                    &mut self.reasoning_accum,
                    &mut self.prefix_resolved,
                    &mut reasoning_emit,
                );
                // 段末仍未 resolved 时，累积文本是 `thought\n` 的严格前缀——按定义无可发出，丢弃。
                work = work[idx + END_TOKEN.len()..].to_string();
                self.reset_span();
                continue;
            }

            // 尚无结束标记。留存可能的部分结束标记后缀。
            let lap = overlap(&work, END_TOKEN);
            let split = work.len() - lap;
            let raw = work[..split].to_string();
            self.buffer = work[split..].to_string();

            if !raw.is_empty() {
                push_reasoning(
                    &raw,
                    &mut self.reasoning_accum,
                    &mut self.prefix_resolved,
                    &mut reasoning_emit,
                );
            }
            break;
        }

        ParserResult {
            normal_text: normal,
            reasoning_text: reasoning_emit,
        }
    }
}

#[cfg(test)]
mod tests {
    //! ## 测试过程
    //! 围绕 `Gemma4ReasoningParser` 的非流式与流式接口，验证：基本思维链抽取、无标记透传、
    //! 仅起始（截断）、前后文本保留、悬挂结束标记、`thought\n` 前缀剥除及跨块切分等。
    //!
    //! ## 意义
    //! 锁定 Gemma 4 通道分隔推理在批式与流式（含部分标记跨块）下的可观察行为。
    use super::*;

    #[test] // REASONING.batch.1 — non-streaming basic case
    fn detect_basic_thinking() {
        let mut p = Gemma4ReasoningParser::new();
        let r = p.detect_and_parse_reasoning(
            "<|channel>thought\nstep one\nstep two<channel|>The answer is 42.",
            &[],
        );
        assert_eq!(r.reasoning_text, "step one\nstep two");
        assert_eq!(r.normal_text, "The answer is 42.");
    }

    #[test] // REASONING.batch.3 — no reasoning markers, pass through
    fn detect_no_markers_passes_through() {
        let mut p = Gemma4ReasoningParser::new();
        let r = p.detect_and_parse_reasoning("just a plain answer", &[]);
        assert_eq!(r.reasoning_text, "");
        assert_eq!(r.normal_text, "just a plain answer");
    }

    #[test] // REASONING.batch.5 — reasoning open without close (truncation): everything after
    fn detect_truncated_reasoning_open_only() {
        let mut p = Gemma4ReasoningParser::new();
        let r = p.detect_and_parse_reasoning("intro <|channel>thought\npartial", &[]);
        assert_eq!(r.reasoning_text, "partial");
        assert_eq!(r.normal_text, "intro ");
    }

    #[test] // REASONING.batch.3 — text before AND after the reasoning span preserved
    fn detect_text_before_and_after() {
        let mut p = Gemma4ReasoningParser::new();
        let r = p.detect_and_parse_reasoning(
            "Hello. <|channel>thought\nrumination<channel|> Goodbye.",
            &[],
        );
        assert_eq!(r.reasoning_text, "rumination");
        assert_eq!(r.normal_text, "Hello.  Goodbye.");
    }

    #[test] // REASONING.batch.5, REASONING.batch.3 — dangling end marker, missing start (upstream INVALID_SIMPLE)
    fn detect_dangling_end_marker_extracts_prefix_as_reasoning() {
        let mut p = Gemma4ReasoningParser::new();
        let r = p.detect_and_parse_reasoning("some thinking<channel|>final answer", &[]);
        assert_eq!(r.reasoning_text, "some thinking");
        assert_eq!(r.normal_text, "final answer");
    }

    #[test] // REASONING.batch.5, REASONING.batch.3 — dangling end + thought prefix on the head (upstream INVALID_COMPLETE)
    fn detect_dangling_end_marker_strips_thought_prefix() {
        let mut p = Gemma4ReasoningParser::new();
        let r = p.detect_and_parse_reasoning("thought\nrumination<channel|>final answer", &[]);
        assert_eq!(r.reasoning_text, "rumination");
        assert_eq!(r.normal_text, "final answer");
    }

    #[test] // `thought\n` prefix absent (some tokens drop it): pass through unchanged
    fn detect_no_thought_prefix() {
        let mut p = Gemma4ReasoningParser::new();
        let r = p.detect_and_parse_reasoning(
            "<|channel>raw reasoning without prefix<channel|>answer",
            &[],
        );
        assert_eq!(r.reasoning_text, "raw reasoning without prefix");
        assert_eq!(r.normal_text, "answer");
    }

    #[test] // REASONING.stream.3 — streaming arrival, single chunk
    fn streaming_single_chunk() {
        let mut p = Gemma4ReasoningParser::new();
        let r = p.parse_reasoning_streaming_incremental(
            "<|channel>thought\nrumination<channel|>final",
            &[],
        );
        assert_eq!(r.reasoning_text, "rumination");
        assert_eq!(r.normal_text, "final");
    }

    #[test] // REASONING.stream.3 — streaming with `thought\n` split across deltas
    fn streaming_thought_prefix_split_across_deltas() {
        let mut p = Gemma4ReasoningParser::new();
        let chunks = [
            "<|channel>",
            "thou",
            "ght\n",
            "real reasoning here",
            "<channel|>",
            "the answer.",
        ];
        let mut reasoning = String::new();
        let mut normal = String::new();
        for c in chunks {
            let r = p.parse_reasoning_streaming_incremental(c, &[]);
            reasoning.push_str(&r.reasoning_text);
            normal.push_str(&r.normal_text);
        }
        assert_eq!(reasoning, "real reasoning here");
        assert_eq!(normal, "the answer.");
    }

    #[test] // REASONING.stream.3 — start marker split across deltas
    fn streaming_start_marker_split() {
        let mut p = Gemma4ReasoningParser::new();
        let chunks = [
            "intro ",
            "<|chan", // partial start marker
            "nel>thought\n",
            "rumination",
            "<channel|>",
            "outro",
        ];
        let mut reasoning = String::new();
        let mut normal = String::new();
        for c in chunks {
            let r = p.parse_reasoning_streaming_incremental(c, &[]);
            reasoning.push_str(&r.reasoning_text);
            normal.push_str(&r.normal_text);
        }
        assert_eq!(reasoning, "rumination");
        assert_eq!(normal, "intro outro");
    }

    #[test] // REASONING.stream.3 — end marker split across deltas
    fn streaming_end_marker_split() {
        let mut p = Gemma4ReasoningParser::new();
        let chunks = [
            "<|channel>thought\n",
            "thinking",
            "<chan", // partial end marker
            "nel|>",
            "answer",
        ];
        let mut reasoning = String::new();
        let mut normal = String::new();
        for c in chunks {
            let r = p.parse_reasoning_streaming_incremental(c, &[]);
            reasoning.push_str(&r.reasoning_text);
            normal.push_str(&r.normal_text);
        }
        assert_eq!(reasoning, "thinking");
        assert_eq!(normal, "answer");
    }

    #[test] // REASONING.stream.3 — diverged accumulated text (no `thought\n` prefix at all)
    fn streaming_no_thought_prefix_streaming() {
        let mut p = Gemma4ReasoningParser::new();
        let chunks = [
            "<|channel>",
            "raw stream of consciousness",
            "<channel|>",
            "answer",
        ];
        let mut reasoning = String::new();
        let mut normal = String::new();
        for c in chunks {
            let r = p.parse_reasoning_streaming_incremental(c, &[]);
            reasoning.push_str(&r.reasoning_text);
            normal.push_str(&r.normal_text);
        }
        assert_eq!(reasoning, "raw stream of consciousness");
        assert_eq!(normal, "answer");
    }

    #[test] // REASONING.batch.3 — streaming with no markers at all
    fn streaming_no_markers() {
        let mut p = Gemma4ReasoningParser::new();
        let r = p.parse_reasoning_streaming_incremental("plain text only", &[]);
        assert_eq!(r.reasoning_text, "");
        assert_eq!(r.normal_text, "plain text only");
    }

    #[test] // REASONING.batch.2 — multiple reasoning spans back-to-back
    fn streaming_multiple_reasoning_spans() {
        let mut p = Gemma4ReasoningParser::new();
        let input =
            "<|channel>thought\nfirst<channel|>answer1<|channel>thought\nsecond<channel|>answer2";
        let r = p.parse_reasoning_streaming_incremental(input, &[]);
        assert!(r.reasoning_text.contains("first"));
        assert!(r.reasoning_text.contains("second"));
        assert!(r.normal_text.contains("answer1"));
        assert!(r.normal_text.contains("answer2"));
    }

    #[test] // REASONING.batch.2 — paired reasoning + tool call. The reasoning parser
    fn paired_reasoning_then_tool_call_non_streaming() {
        let mut p = Gemma4ReasoningParser::new();
        let input = concat!(
            "<|channel>thought\nthinking about the request<channel|>",
            "<|tool_call>call:get_weather{location:<|\"|>Tokyo<|\"|>}<tool_call|>",
        );
        let r = p.detect_and_parse_reasoning(input, &[]);
        assert_eq!(r.reasoning_text, "thinking about the request");
        assert_eq!(
            r.normal_text, r#"<|tool_call>call:get_weather{location:<|"|>Tokyo<|"|>}<tool_call|>"#,
            "tool-call markers must survive reasoning extraction",
        );
    }

    //
}
