QUERY CreateEmail (
    email_id: String,
    senderEmail: String,
    recipientEmails: [String],
    subject: String,
    body: String,
    date: Date,
    thread_id: String,
    inReplyTo: String,
    messageId: String
) =>
    existing <- N<Email>::WHERE(_::{email_id}::EQ(email_id))
    email <- existing::UpsertN({
        email_id: email_id,
        sender: senderEmail,
        recipients: recipientEmails,
        subject: subject,
        body: body,
        date: date,
        thread_id: thread_id,
        in_reply_to: inReplyTo,
        message_id: messageId
    })
    RETURN email

QUERY GetEmail(email_id: String) => 
    email <- N<Email>({email_id: email_id})
    RETURN email
