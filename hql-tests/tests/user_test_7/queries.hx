N::User {
  name: String,
  checkpoint_id: String,
  created_at: String,
}

N::Img {
  title: String,
  checkpoint_id: String,
  created_at: String,
}

E::PERMISSION {
  From: User,
  To: Img,
  Properties: {
    can_read: U8,
    can_write: U8,
    can_share: U8,
  }
}

E::MENTION {
  From: User,
  To: Img,
  Properties: {
    tag_by: String
  }
}

E::REACTION {
  From: User,
  To: Img,
  Properties: {
    is_happy: U8,
    is_sad: U8,
    is_angry: U8,
    is_support: U8,

  }
}

// chk success, but compile failed
QUERY test(
) =>
  response <- E<PERMISSION>::WHERE(
    AND(
      _::{can_read}::EQ(1),
      _::{can_write}::EQ(1)
    )
  )
  RETURN response