N::Call {
      INDEX room_id: String,
      INDEX organization_id: String,
      name: String,
      created_at: Date DEFAULT NOW
  }

  N::Organization {
      name: String,
  }

  V::CallTranscriptChunk {
      INDEX room_id: String,
      INDEX organization_id: String,
      speaker: String,
      start_time: F64,
      end_time: F64,
      segment_count: I64,
      content: String,
      created_at: Date DEFAULT NOW
  }

  E::Call_Has_TranscriptChunk {
      From: Call,
      To: CallTranscriptChunk
  }

  E::Call_Belongs_To_Org {
      From: Call,
      To: Organization
  }
