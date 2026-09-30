//! Reasoning activity retains the latest usable line as status details through
//! empty items and status-row replacement while the Working row stays intact.

use super::*;
use pretty_assertions::assert_eq;

fn delta(chat: &mut ChatWidget, id: &str, text: &str) {
    chat.handle_server_notification(
        ServerNotification::ReasoningSummaryTextDelta(ReasoningSummaryTextDeltaNotification {
            thread_id: "thread-1".to_string(),
            turn_id: "turn-1".to_string(),
            item_id: id.to_string(),
            delta: text.to_string(),
            summary_index: 0,
        }),
        /*replay_kind*/ None,
    );
}

fn complete(chat: &mut ChatWidget, id: &str) {
    chat.handle_server_notification(
        ServerNotification::ItemCompleted(ItemCompletedNotification {
            thread_id: "thread-1".to_string(),
            turn_id: "turn-1".to_string(),
            completed_at_ms: 0,
            item: AppServerThreadItem::Reasoning {
                id: id.to_string(),
                summary: Vec::new(),
                content: Vec::new(),
            },
        }),
        /*replay_kind*/ None,
    );
}

#[tokio::test]
async fn reasoning_status_accepts_bold_text_with_a_plain_suffix() {
    let (mut chat, _rx, _ops) = make_chatwidget_manual(/*model_override*/ None).await;
    chat.on_task_started();
    handle_agent_reasoning_started(&mut chat, "reasoning");

    let details = |chat: &ChatWidget| {
        chat.bottom_pane
            .status_widget()
            .unwrap()
            .details()
            .map(str::to_string)
    };
    delta(&mut chat, "reasoning", "**Checking tests");
    let before_close = details(&chat);
    delta(&mut chat, "reasoning", "**: running suite");
    let after_close = details(&chat);

    insta::assert_debug_snapshot!(vec![before_close, after_close], @r###"
    [
        None,
        Some(
            "Checking tests: running suite",
        ),
    ]
    "###);
}

#[tokio::test]
async fn reasoning_status_tracks_items_and_restores_after_tool_activity() {
    let (mut chat, _rx, _ops) = make_chatwidget_manual(/*model_override*/ None).await;
    chat.on_task_started();
    let mut details = Vec::new();
    let capture = |chat: &ChatWidget, details: &mut Vec<Option<String>>| {
        details.push(
            chat.bottom_pane
                .status_widget()
                .expect("running status")
                .details()
                .map(str::to_string),
        );
    };

    handle_agent_reasoning_started(&mut chat, "first");
    delta(&mut chat, "first", "**Researching backend");
    capture(&chat, &mut details);
    delta(&mut chat, "first", " freshness**");
    capture(&chat, &mut details);
    complete(&mut chat, "first");
    delta(&mut chat, "first", "**Late old header**");
    capture(&chat, &mut details);

    // A tool recreates the row after streamed commentary has hidden it.
    chat.bottom_pane.hide_status_indicator();
    begin_unified_exec_startup(&mut chat, "tool-1", "process-1", "sleep 2");
    capture(&chat, &mut details);

    handle_agent_reasoning_started(&mut chat, "second");
    capture(&chat, &mut details);
    chat.handle_server_notification(
        ServerNotification::ReasoningSummaryPartAdded(
            codex_app_server_protocol::ReasoningSummaryPartAddedNotification {
                thread_id: "thread-1".to_string(),
                turn_id: "turn-1".to_string(),
                item_id: "first".to_string(),
                summary_index: 1,
            },
        ),
        /*replay_kind*/ None,
    );
    complete(&mut chat, "first");
    delta(&mut chat, "first", "**Stale heading**");
    delta(&mut chat, "second", "No summary heading is available.");
    capture(&chat, &mut details);
    delta(&mut chat, "second", "\n**Preparing evidence report**");
    capture(&chat, &mut details);

    insta::assert_debug_snapshot!(details);
    let mut timer = crate::status_indicator_widget::StatusTimer::default();
    timer.pause_at(std::time::Instant::now());
    timer.reset(std::time::Duration::from_secs(/*secs*/ 42));
    for width in [80, 40] {
        let mut terminal =
            ratatui::Terminal::new(ratatui::backend::TestBackend::new(width, /*height*/ 3))
                .expect("terminal");
        terminal
            .draw(|frame| {
                chat.bottom_pane
                    .status_widget()
                    .expect("running status")
                    .with_timer(&timer)
                    .render(frame.area(), frame.buffer_mut());
            })
            .expect("render reasoning status");
        assert_chatwidget_snapshot!(
            format!("reasoning_activity_row_{width}"),
            terminal.backend()
        );
    }
    chat.on_interrupted_turn(TurnAbortReason::Interrupted);
    delta(&mut chat, "second", "**After interruption**");
    assert_eq!(chat.status_state.reasoning_item_id, None);
    assert_eq!(chat.reasoning_header, None);
    assert!(!chat.bottom_pane.status_indicator_visible());
    chat.on_task_started();
    assert_eq!(
        chat.bottom_pane.status_widget().unwrap().header(),
        "Working"
    );
}

#[tokio::test]
async fn voice_handoff_preserves_typed_reasoning_and_ignores_private_items() {
    let (mut chat, _rx, _ops) = make_chatwidget_manual(/*model_override*/ None).await;
    chat.on_task_started();
    handle_agent_reasoning_started(&mut chat, "typed");
    delta(&mut chat, "typed", "**Checking repository**");

    chat.remember_realtime_delegated_reasoning_turn("turn-1");
    handle_agent_reasoning_started(&mut chat, "private");
    delta(&mut chat, "private", "**Private voice reasoning**");
    complete(&mut chat, "private");
    assert_eq!(
        chat.status_state.reasoning_item_id.as_deref(),
        Some("typed")
    );
    assert_eq!(
        chat.bottom_pane.status_widget().unwrap().details(),
        Some("Checking repository")
    );

    delta(&mut chat, "typed", "\n**Verifying changes**");
    assert_eq!(
        chat.bottom_pane.status_widget().unwrap().details(),
        Some("Verifying changes")
    );
    complete(&mut chat, "typed");
    assert_eq!(chat.status_state.reasoning_item_id, None);
}

#[tokio::test]
async fn reasoning_status_preserves_an_explicit_wait_and_restores_the_heading() {
    let (mut chat, _rx, _ops) = make_chatwidget_manual(/*model_override*/ None).await;
    chat.on_task_started();
    chat.status_state
        .pending_guardian_review_status
        .start_or_update("review-1".to_string(), "Checking a command".to_string());
    chat.set_status_header("Reviewing approval request".to_string());
    handle_agent_reasoning_started(&mut chat, "first");
    delta(&mut chat, "first", "**Choosing the missing-data fix**");
    complete(&mut chat, "first");
    assert_eq!(
        chat.bottom_pane.status_widget().unwrap().header(),
        "Reviewing approval request"
    );
    chat.status_state
        .pending_guardian_review_status
        .finish("review-1");
    chat.restore_reasoning_status_header();
    assert_eq!(
        chat.bottom_pane.status_widget().unwrap().header(),
        "Working"
    );
    assert_eq!(
        chat.bottom_pane.status_widget().unwrap().details(),
        Some("Choosing the missing-data fix")
    );
}

#[tokio::test]
async fn reasoning_status_replay_retains_last_usable_heading() {
    let (mut chat, _rx, _ops) = make_chatwidget_manual(/*model_override*/ None).await;
    chat.on_task_started();
    for (id, summary, expected) in [
        (
            "first",
            vec!["**First heading**".to_string()],
            "First heading",
        ),
        (
            "second",
            vec!["**Second heading**".to_string()],
            "Second heading",
        ),
        ("third", Vec::new(), "Second heading"),
    ] {
        chat.handle_thread_item(
            AppServerThreadItem::Reasoning {
                id: id.to_string(),
                summary,
                content: Vec::new(),
            },
            "turn-1".to_string(),
            ThreadItemRenderSource::Replay(ReplayKind::ThreadSnapshot),
        );
        assert_eq!(
            chat.bottom_pane.status_widget().unwrap().details(),
            Some(expected)
        );
    }
}

#[tokio::test]
async fn reasoning_status_accepts_plain_lines_and_ignores_empty_sections() {
    let (mut chat, _rx, _ops) = make_chatwidget_manual(/*model_override*/ None).await;
    chat.on_task_started();
    handle_agent_reasoning_started(&mut chat, "first");
    delta(
        &mut chat,
        "first",
        "# Checking files\n<!-- progress -->\nPreparing response",
    );
    assert_eq!(
        chat.bottom_pane.status_widget().unwrap().header(),
        "Working"
    );
    assert_eq!(
        chat.bottom_pane.status_widget().unwrap().details(),
        Some("Preparing response")
    );
    complete(&mut chat, "first");
    handle_agent_reasoning_started(&mut chat, "second");
    delta(&mut chat, "second", "<!-- no public update -->");
    assert_eq!(
        chat.bottom_pane.status_widget().unwrap().details(),
        Some("Preparing response")
    );
    delta(&mut chat, "second", "\n**Verifying results**");
    assert_eq!(
        chat.bottom_pane.status_widget().unwrap().details(),
        Some("Verifying results")
    );
}

#[tokio::test]
async fn completed_reasoning_stays_in_expanded_transcript_for_live_and_replay() {
    let mut renders = Vec::new();
    for replay in [false, true] {
        let (mut chat, mut rx, _ops) = make_chatwidget_manual(/*model_override*/ None).await;
        chat.on_task_started();
        for (id, summary) in [
            ("first", "**Inspecting repository structure**"),
            ("second", "Mapping the app structure"),
            (
                "third",
                "**Tracing terminal components**\n\nThe playback clock preserves elapsed time.",
            ),
        ] {
            if replay {
                chat.handle_thread_item(
                    AppServerThreadItem::Reasoning {
                        id: id.to_string(),
                        summary: vec![summary.to_string()],
                        content: Vec::new(),
                    },
                    "turn-1".to_string(),
                    ThreadItemRenderSource::Replay(ReplayKind::ThreadSnapshot),
                );
            } else {
                handle_agent_reasoning_started(&mut chat, id);
                delta(&mut chat, id, summary);
                complete(&mut chat, id);
            }
        }
        let mut compact = Vec::new();
        let mut expanded = Vec::new();
        while let Ok(event) = rx.try_recv() {
            if let AppEvent::InsertHistoryCell(cell) = event {
                compact.extend(cell.display_lines(/*width*/ 80));
                expanded.extend(cell.transcript_lines(/*width*/ 80));
                assert!(cell.raw_lines().is_empty());
            }
        }
        let rendered = format!(
            "Compact history:\n{}\nLive status:\n{}\nExpanded transcript:\n{}",
            ratatui::text::Text::from(compact),
            chat.bottom_pane.status_widget().unwrap().header(),
            ratatui::text::Text::from(expanded),
        );
        renders.push(rendered);
    }
    assert_eq!(renders[0], renders[1]);
    insta::assert_snapshot!(renders[0], @r"
        Compact history:

        Live status:
        Working
        Expanded transcript:
        • Inspecting repository structure
        • Mapping the app structure
        • The playback clock preserves elapsed time.
        ");
}
