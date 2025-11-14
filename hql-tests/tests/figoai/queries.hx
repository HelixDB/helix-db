V::ShortTermMemory{
   businessId: String,
   sessionId: String,
   timestamp: Date
}

QUERY getShortTermMemory(businessId: String, sessionId: String, limit: I64) =>
    memories <- V<ShortTermMemory>::WHERE(AND(
        _::{businessId}::EQ(businessId),
        _::{sessionId}::EQ(sessionId)
    ))
    ::ORDER<Asc>(_::{timestamp})
    ::RANGE(0, limit)
    RETURN memories