//! Downstream compile and wire contracts for retained public encoding APIs.

#![allow(deprecated)]

#[test]
fn retained_property_paths_forward_to_the_canonical_v2_codecs() {
    let root_property_hash: db::encoding::indexes::PropertyHash = [0; 4];
    let root_value_hash: db::encoding::indexes::ValueHash = [0; 8];
    let v1_property_hash: db::encoding::v1::indexes::PropertyHash = root_property_hash;
    let v1_value_hash: db::encoding::v1::indexes::ValueHash = root_value_hash;
    let v2_property_hash: db::encoding::v2::keys::indexes::PropertyHash = v1_property_hash;
    let v2_value_hash: db::encoding::v2::keys::indexes::ValueHash = v1_value_hash;

    assert_eq!(v2_property_hash, [0; 4]);
    assert_eq!(v2_value_hash, [0; 8]);
    assert!(db::encoding::property::decode_properties(&[])
        .expect("root property decoder accepts the empty row")
        .is_empty());
    assert!(db::encoding::v1::property::decode_properties(&[])
        .expect("V1 compatibility decoder accepts the empty row")
        .is_empty());
    assert!(db::encoding::v2::values::property::decode_properties(&[])
        .expect("V2 property decoder accepts the empty row")
        .is_empty());
}

#[test]
fn public_text_live_state_encoder_remains_available_by_default() {
    let state = db::search::text::TextIndexLiveState::live(7);
    let encoded = db::search::text::encode_live_state_bytes(&state)
        .expect("public live-state encoder accepts a valid state");

    assert_eq!(encoded, br#"{"logical_version":7,"live":true}"#);
    assert_eq!(
        db::search::text::decode_live_state_bytes(&encoded)
            .expect("public live-state decoder accepts public encoder output"),
        state
    );
}
