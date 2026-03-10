QUERY SearchCallTranscriptChunks(room_id: String, query: String, limit: I64) =>
      call <- N<Call>({room_id: room_id})
      results <- call::Out<Call_Has_TranscriptChunk>::SearchV<CallTranscriptChunk>(Embed(query), limit)
      RETURN results
