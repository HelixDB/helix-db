N::Email {
    UNIQUE INDEX email_id: String,
    in_reply_to: String,
    sender: String,
    recipients: [String],
    subject: String,
    body: String,
    date: Date,
    thread_id: String,
    INDEX batch_marker: String DEFAULT "",
    INDEX message_id: String,
}
