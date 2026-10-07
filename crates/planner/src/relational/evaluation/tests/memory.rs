use super::*;

#[test]
fn nested_admission_preserves_live_siblings_and_branch_independence() {
    let peak = AtomicUsize::new(0);
    let root = Memory::observed(1000, 100, &peak).unwrap();
    assert_eq!(peak.load(Ordering::Relaxed), 100);
    let child = root.remaining(200).unwrap();
    assert_eq!(child.available(), 700);
    let nested = child.remaining(400).unwrap();
    assert_eq!(nested.available(), 300);
    assert_eq!(peak.load(Ordering::Relaxed), 700);
    assert_eq!(root.available(), 900);
    root.remaining(500).unwrap();
    assert_eq!(peak.load(Ordering::Relaxed), 700);
    root.remaining(800).unwrap();
    assert_eq!(peak.load(Ordering::Relaxed), 900);
}

#[test]
fn caps_do_not_count_unused_headroom_or_increase_available_bytes() {
    let peak = AtomicUsize::new(0);
    let memory = Memory::observed(1000, 100, &peak).unwrap().capped(250);
    assert_eq!(memory.available(), 250);
    assert_eq!(memory.capped(500).available(), 250);
    let used = memory.remaining(200).unwrap();
    assert_eq!(used.available(), 50);
    assert_eq!(peak.load(Ordering::Relaxed), 300);
    assert!(used.remaining(51).is_err());
    assert_eq!(peak.load(Ordering::Relaxed), 300);
}

#[test]
fn invalid_or_unaddressable_admission_does_not_raise_the_peak() {
    let peak = AtomicUsize::new(17);
    assert!(Memory::observed(10, 11, &peak).is_err());
    assert_eq!(peak.load(Ordering::Relaxed), 17);
    let memory = Memory::observed(1000, 0, &peak).unwrap();
    let error = memory.remaining(1001).err().unwrap();
    assert_eq!(
        (error.category.as_str(), error.detail.as_str()),
        ("ResourceLimit", "MemoryLimit")
    );
    assert_eq!(peak.load(Ordering::Relaxed), 17);
    let memory = Memory::observed(usize::MAX, 0, &peak).unwrap();
    assert!(memory.remaining(isize::MAX as usize + 1).is_err());
    assert_eq!(peak.load(Ordering::Relaxed), 17);
    let empty = Memory::new(0).remaining(0).unwrap();
    assert_eq!(empty.available(), 0);
    assert!(empty.remaining(1).is_err());
    assert_eq!(Memory::new(2).remaining(2).unwrap().available(), 0);
}

#[test]
fn cumulative_admission_handles_the_full_usize_boundary() {
    let peak = AtomicUsize::new(0);
    let memory = Memory::observed(usize::MAX, usize::MAX - 1, &peak).unwrap();
    let full = memory.remaining(1).unwrap();
    assert_eq!(full.available(), 0);
    assert_eq!(peak.load(Ordering::Relaxed), usize::MAX);
    assert!(full.remaining(1).is_err());
    fn send_sync<T: Send + Sync>() {}
    send_sync::<Memory<'static>>();
}
