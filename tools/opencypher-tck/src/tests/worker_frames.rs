use super::*;

#[tokio::test]
async fn framed_messages_preserve_boundaries_and_reject_partial_or_invalid_input() {
    let messages = [
        serde_json::json!({"unicode":"λ\n\u{0}"}),
        serde_json::json!([1, 2, 3]),
    ];
    let mut wire = Vec::new();
    for message in &messages {
        write(&mut wire, message).await.unwrap();
    }
    let mut input = wire.as_slice();
    for message in &messages {
        assert_eq!(
            read::<_, serde_json::Value>(&mut input)
                .await
                .unwrap()
                .as_ref(),
            Some(message)
        );
    }
    assert!(read::<_, serde_json::Value>(&mut input)
        .await
        .unwrap()
        .is_none());
    for end in 1..4 {
        assert!(read::<_, serde_json::Value>(&mut &wire[..end])
            .await
            .is_err());
    }
    assert!(read::<_, serde_json::Value>(&mut &wire[..wire.len() - 1])
        .await
        .unwrap()
        .is_some());
    let truncated = &mut &wire[4 + serde_json::to_vec(&messages[0]).unwrap().len()..wire.len() - 1];
    assert!(read::<_, serde_json::Value>(truncated).await.is_err());
    for invalid in [
        vec![0, 0, 0, 0],
        vec![0, 0, 0, 1, b'{'],
        vec![0, 0, 0, 2, b'1'],
    ] {
        assert!(read::<_, serde_json::Value>(&mut invalid.as_slice())
            .await
            .is_err());
    }
    let oversized = u32::try_from(MAX_BYTES + 1).unwrap().to_be_bytes();
    assert!(read::<_, serde_json::Value>(&mut oversized.as_slice())
        .await
        .unwrap_err()
        .to_string()
        .contains("WorkerFrameLimit"));
}

#[tokio::test]
async fn writing_is_bounded_before_emitting_a_frame_and_io_errors_propagate() {
    let mut output = Vec::new();
    assert!(write(&mut output, &"x".repeat(MAX_BYTES))
        .await
        .unwrap_err()
        .to_string()
        .contains("WorkerFrameLimit"));
    assert!(output.is_empty());
    let value = "x".repeat(MAX_BYTES - 2);
    write(&mut output, &value).await.unwrap();
    assert_eq!(
        read::<_, String>(&mut output.as_slice()).await.unwrap(),
        Some(value)
    );
    let (mut writer, reader) = tokio::io::duplex(1);
    drop(reader);
    assert!(write(&mut writer, &true).await.is_err());
    let mut buffer = Buffer(Vec::new());
    buffer.flush().unwrap();
    assert_eq!(buffer.write(&[]).unwrap(), 0);
}
