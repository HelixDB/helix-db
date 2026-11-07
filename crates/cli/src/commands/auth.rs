use crate::cloud::model::{Named as _, Workspace};
use crate::errors::CliError;
use crate::output::{self, table, Step};
use crate::{
    cloud::{CloudClient, HttpError, SessionCredentials},
    host_actions,
    metrics_sender::{load_metrics_config, save_metrics_config},
    prompts, AuthAction, LoginAction,
};
use eyre::{eyre, Result, WrapErr as _};
use reqwest::StatusCode;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::{
    io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _, BufReader},
    net::{TcpListener, TcpStream},
    task::JoinSet,
    time::{sleep, timeout, Duration},
};

const LOGIN_TIMEOUT: Duration = Duration::from_secs(5 * 60);
/// Registered with WorkOS as `http://localhost:8765/callback`; another port
/// would be refused at the authorize step.
const LOGIN_CALLBACK_PORT: u16 = 8765;
/// WorkOS locks a pending verification after three wrong codes.
const MAX_VERIFICATION_ATTEMPTS: u32 = 3;
/// How long a connection to the callback port may stay silent before it is
/// closed. Browsers open idle preconnect sockets and may send the redirect on one.
const CALLBACK_READ_TIMEOUT: Duration = Duration::from_secs(30);
/// How much of a request to the callback port is read. Cookies for `localhost`
/// are shared across ports, so this has to fit a developer's whole collection.
const MAX_CALLBACK_REQUEST: u64 = 1024 * 1024;
const ACCEPT_RETRY_DELAY: Duration = Duration::from_millis(100);

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StartLoginResponse {
    url: String,
    session_id: String,
}

/// The reply shared by code exchange, password sign-in and email verification,
/// as sent: every field is optional on the wire. [`AuthStep`] is what it means.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct AuthResponse {
    access_token: String,
    refresh_token: String,
    #[serde(deserialize_with = "crate::cloud::deserialize_i64")]
    expires_at: i64,
    email: String,
    email_verification_required: bool,
    sso_required: bool,
    /// Sent by password sign-in; browser logins fill in the one they started.
    session_id: String,
}

/// A browser sign-in, which the server resolves to an authorize URL.
#[derive(Debug, Clone, PartialEq, Eq)]
enum BrowserLogin {
    Github,
    Google,
    Microsoft,
    Sso { email: String },
}

impl BrowserLogin {
    /// The body of `POST /v1/auth/login:start`.
    fn start_body(&self, redirect_uri: &str) -> Value {
        match self {
            Self::Github => {
                json!({"redirectUri": redirect_uri, "provider": "AUTH_PROVIDER_GITHUB"})
            }
            Self::Google => {
                json!({"redirectUri": redirect_uri, "provider": "AUTH_PROVIDER_GOOGLE"})
            }
            Self::Microsoft => {
                json!({"redirectUri": redirect_uri, "provider": "AUTH_PROVIDER_MICROSOFT"})
            }
            Self::Sso { email } => {
                json!({"redirectUri": redirect_uri, "provider": "AUTH_PROVIDER_SSO", "email": email})
            }
        }
    }
}

/// A login with everything it needs from the command line and prompts.
/// Deliberately not `Debug`: it can hold a password.
enum LoginMethod {
    Browser(BrowserLogin),
    Password { email: String, password: String },
}

/// What a login has to do next, parsed from an [`AuthResponse`].
#[derive(Debug, PartialEq, Eq)]
enum AuthStep {
    /// The login is complete.
    Session(SessionCredentials),
    /// Submit the code emailed to `email` under `session_id`.
    VerifyEmail { session_id: String, email: String },
    /// The email's company only allows its own identity provider.
    SsoRequired { email: String },
}

impl TryFrom<AuthResponse> for AuthStep {
    type Error = eyre::Report;

    fn try_from(response: AuthResponse) -> Result<Self> {
        if response.sso_required {
            if response.email.trim().is_empty() {
                return Err(eyre!(
                    "the login response required single sign-on without naming an email"
                ));
            }
            return Ok(Self::SsoRequired {
                email: response.email,
            });
        }
        if response.email_verification_required {
            if response.session_id.is_empty() {
                return Err(eyre!(
                    "the login response required email verification without a session"
                ));
            }
            return Ok(Self::VerifyEmail {
                session_id: response.session_id,
                email: response.email,
            });
        }
        let session = SessionCredentials {
            access_token: response.access_token,
            refresh_token: response.refresh_token,
            expires_at: response.expires_at,
            email: response.email,
        };
        session
            .validate()
            .wrap_err("the login response did not include a complete session")?;
        Ok(Self::Session(session))
    }
}

/// The person and machine at the other end of a login, kept apart from the
/// protocol so tests can drive every flow without a browser or a terminal.
trait LoginHost {
    /// Binds the loopback port the identity provider redirects to.
    async fn callback_listener(&mut self) -> Result<TcpListener>;
    /// Sends the person to the identity provider's authorize `url`.
    fn open_browser(&mut self, url: &str);
    /// Asks for an email address.
    fn email(&mut self, label: &str) -> Result<String>;
    /// Asks for the account password, exactly as typed.
    fn password(&mut self) -> Result<String>;
    /// Asks for the verification code emailed to `email`.
    fn verification_code(&mut self, email: &str) -> Result<String>;
}

struct TerminalHost;

impl LoginHost for TerminalHost {
    async fn callback_listener(&mut self) -> Result<TcpListener> {
        bind_callback(LOGIN_CALLBACK_PORT).await
    }

    fn open_browser(&mut self, url: &str) {
        // Printed first so a launcher that fails quietly still leaves a way in.
        output::essential(&format!(
            "Opening your browser. If it does not open, visit:\n{url}"
        ));
        if host_actions::open_url(url).is_err() {
            output::essential("Could not open a browser. Visit the URL above to log in.");
        }
    }

    fn email(&mut self, label: &str) -> Result<String> {
        prompts::email(label)
    }

    fn password(&mut self) -> Result<String> {
        prompts::password("Password")
    }

    fn verification_code(&mut self, email: &str) -> Result<String> {
        prompts::input_required(&format!("Verification code sent to {email}"))
    }
}

async fn bind_callback(port: u16) -> Result<TcpListener> {
    TcpListener::bind(("127.0.0.1", port))
        .await
        .wrap_err_with(|| format!("bind the WorkOS callback listener on port {port}"))
}

pub async fn run(action: AuthAction) -> Result<()> {
    match action {
        AuthAction::Login { provider, method } => {
            if let (Some(_), Some(method)) = (provider, &method) {
                return Err(CliError::new(format!(
                    "cannot use --provider with the '{}' subcommand",
                    method.name()
                ))
                .into());
            }
            let action = method.or_else(|| provider.map(Into::into));
            login(action).await
        }
        AuthAction::Status => status().await,
        AuthAction::Logout => logout().await,
    }
}

async fn login(action: Option<LoginAction>) -> Result<()> {
    if !prompts::is_interactive() {
        return Err(CliError::new("logging in needs an interactive terminal")
            .with_hint("run `helix auth login` in a terminal; the CLI never logs in with API keys")
            .into());
    }
    output::intro("Log in to Helix Cloud");
    let client = CloudClient::new()?;
    let action = action.map_or_else(prompts::select_login_action, Ok)?;
    let method = login_method(action, &client, &mut TerminalHost)?;
    let credentials = authenticate(&client, method, &mut TerminalHost).await?;
    client.store_session(&credentials)?;

    let mut metrics = load_metrics_config()?;
    metrics.user_id = None;
    save_metrics_config(&metrics)?;
    output::remark(&format!(
        "Session stored at {}",
        client.credentials_path().display()
    ));
    output::outro(&format!("Logged in as {}", credentials.email));
    output::emit(&json!({"email": credentials.email}), |_| Ok(()))
}

/// The method `action` names, asking for whatever the command line left out.
///
/// Refuses a password login before asking for the password when the server is
/// not reached over a protected connection.
fn login_method(
    action: LoginAction,
    client: &CloudClient,
    host: &mut impl LoginHost,
) -> Result<LoginMethod> {
    Ok(match action {
        LoginAction::Github => LoginMethod::Browser(BrowserLogin::Github),
        LoginAction::Google => LoginMethod::Browser(BrowserLogin::Google),
        LoginAction::Microsoft => LoginMethod::Browser(BrowserLogin::Microsoft),
        LoginAction::Sso { email } => LoginMethod::Browser(BrowserLogin::Sso {
            email: email.map_or_else(|| host.email("Work email"), Ok)?,
        }),
        LoginAction::Password { email } => {
            if !client.has_secure_transport() {
                return Err(CliError::new(
                    "refusing to send a password over an unencrypted connection",
                )
                .with_hint(
                    "use an https:// CLOUD_AUTHORITY, or a browser method such as \
                             `helix auth login sso`",
                )
                .into());
            }
            LoginMethod::Password {
                email: email.map_or_else(|| host.email("Email"), Ok)?,
                password: host.password()?,
            }
        }
    })
}

/// Runs `method` to a session, following each step the server asks for. When
/// the email must use single sign-on, continues once through its identity
/// provider, the way the web login does.
///
/// A loop rather than recursion because recursive `async fn`s need boxing. It
/// ends because a login signs in at most twice (`SsoRequired` during single
/// sign-on is an error), and [`verify_email`] asks a bounded number of times and
/// never returns another [`AuthStep::VerifyEmail`].
async fn authenticate(
    client: &CloudClient,
    mut method: LoginMethod,
    host: &mut impl LoginHost,
) -> Result<SessionCredentials> {
    let mut step = sign_in(client, &method, host).await?;
    loop {
        step = match step {
            AuthStep::Session(session) => return Ok(session),
            AuthStep::VerifyEmail { session_id, email } => {
                verify_email(client, &session_id, &email, host).await?
            }
            AuthStep::SsoRequired { email }
                if matches!(method, LoginMethod::Browser(BrowserLogin::Sso { .. })) =>
            {
                return Err(CliError::new(format!(
                    "{email} must sign in with single sign-on, which did not complete"
                ))
                .with_hint("ask your workspace admin to check the single sign-on connection")
                .into());
            }
            AuthStep::SsoRequired { email } => {
                // Essential: a second browser tab is about to open.
                output::essential(&format!(
                    "{email} signs in with single sign-on; continuing through your company's identity provider"
                ));
                method = LoginMethod::Browser(BrowserLogin::Sso { email });
                sign_in(client, &method, host).await?
            }
        };
    }
}

/// One sign-in attempt, up to the first step the server answers with.
async fn sign_in(
    client: &CloudClient,
    method: &LoginMethod,
    host: &mut impl LoginHost,
) -> Result<AuthStep> {
    let response = match method {
        LoginMethod::Browser(login) => browser_sign_in(client, login, host).await?,
        LoginMethod::Password { email, password } => {
            password_sign_in(client, email, password).await?
        }
    };
    response.try_into()
}

/// POSTs a sign-in the server has to check, behind a spinner because it can take
/// a while. "Checked" rather than "signed in": more steps may follow.
async fn check_sign_in(
    client: &CloudClient,
    path: &str,
    body: Value,
    action: &str,
) -> Result<Value> {
    let mut checking = Step::with_messages("Checking the sign-in", "Sign-in checked");
    checking.start();
    let response = client.public_post(path, body, action).await;
    match &response {
        Ok(_) => checking.done(),
        Err(_) => checking.fail(),
    }
    response
}

/// What a failed login request said, fit for one terminal line, or `fallback`
/// when the reply carried no message.
fn server_message(http: &HttpError, fallback: &str) -> String {
    let message = http.message.replace(char::is_control, "");
    if message.trim().is_empty() {
        fallback.to_owned()
    } else {
        message
    }
}

/// Signs in through the browser and exchanges the returned code.
async fn browser_sign_in(
    client: &CloudClient,
    login: &BrowserLogin,
    host: &mut impl LoginHost,
) -> Result<AuthResponse> {
    let listener = host.callback_listener().await?;
    let redirect_uri = format!(
        "http://localhost:{}/callback",
        listener.local_addr()?.port()
    );
    let started: StartLoginResponse = serde_json::from_value(
        client
            .public_post(
                "/v1/auth/login:start",
                login.start_body(&redirect_uri),
                "start WorkOS login",
            )
            .await?,
    )?;
    let state = reqwest::Url::parse(&started.url)?
        .query_pairs()
        .find_map(|(key, value)| (key == "state").then(|| value.into_owned()))
        .filter(|state| !state.is_empty())
        .ok_or_else(|| eyre!("the login URL did not include a state"))?;

    host.open_browser(&started.url);
    // The redirect only says the browser finished; the exchange decides the rest.
    let mut waiting = Step::with_messages("Waiting for the browser", "The browser responded");
    waiting.start();
    let callback = timeout(
        LOGIN_TIMEOUT,
        receive_callback(listener, &state, || {
            waiting.println(
                "A sign-in error reached the CLI but does not match this login, so it was \
                 ignored. If your browser shows a failed sign-in, press Ctrl+C and log in again.",
            );
        }),
    )
    .await;
    let code = match callback {
        Ok(Ok(code)) => {
            waiting.done();
            code
        }
        Ok(Err(error)) => {
            waiting.fail();
            return Err(error);
        }
        Err(_) => {
            waiting.fail();
            return Err(CliError::new("the browser login timed out")
                .with_hint("run `helix auth login` again and finish within 5 minutes")
                .into());
        }
    };
    let response: AuthResponse = serde_json::from_value(
        check_sign_in(
            client,
            "/v1/auth/exchange",
            json!({"sessionId": started.session_id, "code": code, "state": state}),
            "exchange WorkOS authorization code",
        )
        .await?,
    )?;
    Ok(AuthResponse {
        session_id: started.session_id,
        ..response
    })
}

async fn password_sign_in(
    client: &CloudClient,
    email: &str,
    password: &str,
) -> Result<AuthResponse> {
    let response = check_sign_in(
        client,
        "/v1/auth/password:sign-in",
        json!({"email": email, "password": password}),
        "sign in with password",
    )
    .await;
    let value = response.map_err(|error| -> eyre::Report {
        match error.downcast_ref::<HttpError>() {
            Some(http) if http.status == StatusCode::UNAUTHORIZED => {
                CliError::new(server_message(http, "the sign-in was rejected"))
                    .with_hint(
                        "sign up or reset your password at https://helix-db.com/login, \
                         or run `helix auth login` to pick another method",
                    )
                    .into()
            }
            _ => error,
        }
    })?;
    Ok(serde_json::from_value(value)?)
}

/// Asks for the emailed code until the server takes it. A wrong code (HTTP 400),
/// an outage (HTTP 503) and a request that got no answer all leave the pending
/// sign-in usable, so only those ask again; anything else means the login has to
/// start over.
///
/// Never returns [`AuthStep::VerifyEmail`]: a server asking again after taking a
/// code is an error, which is what keeps [`authenticate`] from looping.
async fn verify_email(
    client: &CloudClient,
    session_id: &str,
    email: &str,
    host: &mut impl LoginHost,
) -> Result<AuthStep> {
    let mut attempt = 1;
    loop {
        let code = host.verification_code(email)?;
        let response = check_sign_in(
            client,
            "/v1/auth/verify-email",
            json!({"sessionId": session_id, "code": code.trim()}),
            "verify WorkOS email",
        )
        .await;
        let error = match response {
            Ok(value) => {
                return match serde_json::from_value::<AuthResponse>(value)?.try_into()? {
                    AuthStep::VerifyEmail { .. } => Err(eyre!(
                        "the server asked for email verification again after taking the code"
                    )),
                    step => Ok(step),
                };
            }
            Err(error) => error,
        };
        let http = error.downcast_ref::<HttpError>();
        let can_retry = http.map_or_else(
            || error.downcast_ref::<reqwest::Error>().is_some(),
            |http| {
                matches!(
                    http.status,
                    StatusCode::BAD_REQUEST | StatusCode::SERVICE_UNAVAILABLE
                )
            },
        );
        if attempt >= MAX_VERIFICATION_ATTEMPTS || !can_retry {
            return Err(match http {
                Some(http) => CliError::new(server_message(http, "email verification failed"))
                    .with_hint(
                        "email verification did not finish; run `helix auth login` to start over",
                    )
                    .into(),
                None => error,
            });
        }
        // Essential: without it the code prompt repeats for no visible reason.
        output::essential(&http.map_or_else(
            || "Could not reach Helix Cloud; try the code again".to_owned(),
            |http| server_message(http, "That did not work; try the code again"),
        ));
        attempt += 1;
    }
}

/// What one connection to the callback port turned out to be.
enum Callback {
    /// Not this login's redirect.
    Stray,
    /// A provider error without this login's `state`. It cannot end the login,
    /// because any web page can send one, but a provider that dropped `state`
    /// from its errors would otherwise leave the person waiting.
    UnmatchedError,
    /// This login's redirect: its code, or why there is none.
    Redirect(Result<String>),
}

/// Waits for the identity provider's redirect and returns its code.
///
/// Only a request to `/callback` carrying the `state` from the authorize URL
/// counts. Everything else is answered and ignored, and each connection has its
/// own task, so a favicon request, an idle preconnect or another web page reaching
/// this port can neither end the login nor hold it up. `on_unmatched_error` runs
/// the first time an error arrives without the right `state`; it gets no text from
/// the request because any web page could have written it.
async fn receive_callback(
    listener: TcpListener,
    state: &str,
    mut on_unmatched_error: impl FnMut(),
) -> Result<String> {
    let mut connections = JoinSet::new();
    let mut warned = false;
    loop {
        tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => {
                    connections.spawn(answer_callback(stream, state.to_owned()));
                }
                // A client that left before the accept, or no free descriptors.
                // Neither says anything about this login, which `LOGIN_TIMEOUT`
                // still ends.
                Err(_) => sleep(ACCEPT_RETRY_DELAY).await,
            },
            Some(answered) = connections.join_next() => match answered {
                Ok(Callback::Redirect(result)) => return result,
                Ok(Callback::UnmatchedError) if !warned => {
                    warned = true;
                    on_unmatched_error();
                }
                Ok(_) | Err(_) => {}
            },
        }
    }
}

/// Reads one request from the callback port, answers it, and says what it was.
async fn answer_callback(mut stream: TcpStream, state: String) -> Callback {
    let mut request = BufReader::new((&mut stream).take(MAX_CALLBACK_REQUEST));
    // Only this line is parsed: cookies shared with other `localhost` services
    // need not be UTF-8, and must not make a real redirect look stray.
    let mut request_line = Vec::new();
    let Ok(Ok(_)) = timeout(
        CALLBACK_READ_TIMEOUT,
        request.read_until(b'\n', &mut request_line),
    )
    .await
    else {
        return Callback::Stray;
    };
    // Closing with unread bytes would reset the connection, and could cost the
    // browser the reply.
    let _ = timeout(CALLBACK_READ_TIMEOUT, async {
        let mut header = Vec::new();
        while request
            .read_until(b'\n', &mut header)
            .await
            .is_ok_and(|read| read > 0)
            && !header.trim_ascii().is_empty()
        {
            header.clear();
        }
    })
    .await;

    let url = std::str::from_utf8(&request_line)
        .ok()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|target| reqwest::Url::parse(&format!("http://127.0.0.1{target}")).ok())
        .filter(|url| url.path() == "/callback");
    let param = |name: &str| {
        url.as_ref()?
            .query_pairs()
            .find_map(|(key, value)| (key == name).then(|| value.into_owned()))
            .filter(|value| !value.is_empty())
    };
    let callback = if param("state").as_deref() == Some(state.as_str()) {
        Callback::Redirect(match (param("code"), param("error")) {
            (_, Some(error)) => Err(CliError::new(format!(
                "the identity provider refused the login: {}",
                param("error_description")
                    .unwrap_or(error)
                    .replace(char::is_control, "")
            ))
            .with_hint("run `helix auth login` to try again or pick another method")
            .into()),
            (Some(code), None) => Ok(code),
            (None, None) => Err(eyre!("WorkOS callback did not contain a code")),
        })
    } else if param("error").is_some() {
        Callback::UnmatchedError
    } else {
        Callback::Stray
    };
    let (status, body) = match &callback {
        Callback::Stray => ("404 Not Found", "Not a pending Helix CLI login."),
        Callback::UnmatchedError => (
            "404 Not Found",
            "This sign-in failed, but the error does not match the pending Helix CLI login. \
             Return to the terminal and log in again.",
        ),
        Callback::Redirect(Ok(_)) => (
            "200 OK",
            "Helix CLI received the sign-in. Return to the terminal.",
        ),
        Callback::Redirect(Err(_)) => (
            "400 Bad Request",
            "Helix CLI login failed. Return to the terminal.",
        ),
    };
    // The login's outcome does not depend on the browser reading this.
    let _ = stream
        .write_all(
            format!(
                "HTTP/1.1 {status}\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
            .as_bytes(),
        )
        .await;
    let _ = stream.shutdown().await;
    callback
}

/// The logged-in account and its workspaces. Listing the workspaces also
/// proves the session is still accepted.
async fn status() -> Result<()> {
    let client = require_auth().await?;
    let email = client.load_session()?.email;
    let workspaces: Vec<Workspace> = client
        .list("/v1/workspaces", &[], "workspaces", "list workspaces")
        .await?;
    let summary = json!({
        "email": email,
        "workspaces": workspaces
            .iter()
            .map(|workspace| json!({"id": workspace.id, "name": workspace.label()}))
            .collect::<Vec<_>>(),
    });
    output::emit(&summary, |_| {
        print!("{}", status_view(&email, &workspaces));
        Ok(())
    })
}

/// `auth status` for a person: the account, then one workspace per line by name,
/// since an account can belong to enough of them to wrap a single line.
fn status_view(email: &str, workspaces: &[Workspace]) -> String {
    let count = match workspaces.len() {
        0 => "none".to_owned(),
        count => count.to_string(),
    };
    let details = table::key_values(&[("Email", email.to_owned()), ("Workspaces", count)]);
    if workspaces.is_empty() {
        return details;
    }
    let mut sorted: Vec<&Workspace> = workspaces.iter().collect();
    sorted.sort_by_key(|workspace| workspace.label().to_lowercase());
    let mut rows = table::Table::new(["NAME", "ID"]);
    for workspace in sorted {
        rows.row([workspace.label(), workspace.id.as_str()]);
    }
    format!("{details}\n{}", rows.render())
}

async fn logout() -> Result<()> {
    let client = CloudClient::new()?;
    if client.load_session().is_ok()
        && let Err(error) = client
            .post("/v1/auth/logout", json!({}), "revoke WorkOS session")
            .await
    {
        output::warning(&format!("Could not revoke the remote session: {error}"));
    }
    client.remove_session()?;
    output::success("Logged out of Helix Cloud");
    output::emit(&json!({"loggedOut": true}), |_| Ok(()))
}

/// A client with a stored session, or an error pointing at `helix auth login`.
pub async fn require_auth() -> Result<CloudClient> {
    let client = CloudClient::new()?;
    client.load_session().map_err(|error| -> eyre::Report {
        CliError::new("not logged in to Helix Cloud")
            .with_caused_by(error.to_string())
            .with_hint("run `helix auth login`")
            .into()
    })?;
    Ok(client)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use wiremock::matchers::{body_json, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn tokens(email: &str) -> Value {
        json!({
            "accessToken": "access",
            "refreshToken": "refresh",
            "expiresAt": "1999999999",
            "email": email,
        })
    }

    fn session(email: &str) -> SessionCredentials {
        SessionCredentials {
            access_token: "access".into(),
            refresh_token: "refresh".into(),
            expires_at: 1_999_999_999,
            email: email.into(),
        }
    }

    /// Follows each authorize URL by calling the login's real callback listener
    /// with the next scripted query, and answers prompts from lists. An unscripted
    /// prompt panics, so a test also proves nothing else was asked.
    struct ScriptedHost {
        /// Held for the whole test, so no parallel test can take the port.
        listener: std::net::TcpListener,
        port: u16,
        /// Binds the held port a second time, as a port in use would behave.
        port_busy: bool,
        callbacks: VecDeque<&'static str>,
        emails: VecDeque<&'static str>,
        passwords: VecDeque<&'static str>,
        codes: VecDeque<&'static str>,
        opened: Vec<String>,
    }

    impl ScriptedHost {
        fn new(callbacks: &[&'static str], codes: &[&'static str]) -> Self {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let port = listener.local_addr().unwrap().port();
            Self {
                listener,
                port,
                port_busy: false,
                callbacks: callbacks.iter().copied().collect(),
                emails: VecDeque::new(),
                passwords: VecDeque::new(),
                codes: codes.iter().copied().collect(),
                opened: Vec::new(),
            }
        }

        fn answering(mut self, emails: &[&'static str], passwords: &[&'static str]) -> Self {
            self.emails = emails.iter().copied().collect();
            self.passwords = passwords.iter().copied().collect();
            self
        }
    }

    impl LoginHost for ScriptedHost {
        async fn callback_listener(&mut self) -> Result<TcpListener> {
            if self.port_busy {
                return bind_callback(self.port).await;
            }
            // `from_std` needs a non-blocking socket, and Windows does not carry the
            // mode over to a duplicate. A blocking accept would stall the test's
            // single-threaded runtime for good.
            let listener = self.listener.try_clone()?;
            listener.set_nonblocking(true)?;
            Ok(TcpListener::from_std(listener)?)
        }

        fn open_browser(&mut self, url: &str) {
            self.opened.push(url.to_owned());
            let query = self.callbacks.pop_front().expect("unexpected browser open");
            let port = self.port;
            tokio::spawn(async move {
                let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
                stream
                    .write_all(format!("GET /callback?{query} HTTP/1.1\r\n\r\n").as_bytes())
                    .await
                    .unwrap();
            });
        }

        fn email(&mut self, _label: &str) -> Result<String> {
            Ok(self
                .emails
                .pop_front()
                .expect("unexpected email prompt")
                .to_owned())
        }

        fn password(&mut self) -> Result<String> {
            Ok(self
                .passwords
                .pop_front()
                .expect("unexpected password prompt")
                .to_owned())
        }

        fn verification_code(&mut self, _email: &str) -> Result<String> {
            Ok(self
                .codes
                .pop_front()
                .expect("unexpected code prompt")
                .to_owned())
        }
    }

    struct Fixture {
        server: MockServer,
        client: CloudClient,
        _home: tempfile::TempDir,
    }

    impl Fixture {
        async fn new() -> Self {
            let server = MockServer::start().await;
            let home = tempfile::tempdir().unwrap();
            let client =
                CloudClient::with_paths(server.uri(), home.path().join("credentials")).unwrap();
            Self {
                server,
                client,
                _home: home,
            }
        }

        /// Answers `route` once with `reply`, only when the body is exactly `body`.
        async fn expect(&self, route: &str, body: Value, status: u16, reply: Value) {
            Mock::given(method("POST"))
                .and(path(route))
                .and(body_json(body))
                .respond_with(ResponseTemplate::new(status).set_body_json(reply))
                .expect(1)
                .up_to_n_times(1)
                .mount(&self.server)
                .await;
        }

        /// Mounts the start of a browser login; the session id is `session`.
        async fn expect_start(&self, host: &ScriptedHost, login: BrowserLogin, session: &str) {
            let redirect = format!("http://localhost:{}/callback", host.port);
            // `session-1` pairs with `state=s1`, the state scripted callbacks send.
            let state = session.replace("session-", "s");
            self.expect(
                "/v1/auth/login:start",
                login.start_body(&redirect),
                200,
                json!({
                    "url": format!("https://idp.test/authorize?state={state}"),
                    "sessionId": session,
                }),
            )
            .await;
        }

        async fn run(
            &self,
            login: LoginMethod,
            host: &mut ScriptedHost,
        ) -> Result<SessionCredentials> {
            authenticate(&self.client, login, host).await
        }
    }

    fn password_login() -> LoginMethod {
        LoginMethod::Password {
            email: "person@test.dev".into(),
            password: " secret ".into(),
        }
    }

    fn password_body() -> Value {
        // Passwords go out exactly as typed: spaces can be part of them.
        json!({"email": "person@test.dev", "password": " secret "})
    }

    fn sso_login() -> BrowserLogin {
        BrowserLogin::Sso {
            email: "person@acme.test".into(),
        }
    }

    #[test]
    fn every_browser_login_names_its_provider() {
        let redirect = "http://localhost:8765/callback";
        for (login, expected) in [
            (
                BrowserLogin::Github,
                json!({"redirectUri": redirect, "provider": "AUTH_PROVIDER_GITHUB"}),
            ),
            (
                BrowserLogin::Google,
                json!({"redirectUri": redirect, "provider": "AUTH_PROVIDER_GOOGLE"}),
            ),
            (
                BrowserLogin::Microsoft,
                json!({"redirectUri": redirect, "provider": "AUTH_PROVIDER_MICROSOFT"}),
            ),
            (
                sso_login(),
                json!({
                    "redirectUri": redirect,
                    "provider": "AUTH_PROVIDER_SSO",
                    "email": "person@acme.test",
                }),
            ),
        ] {
            assert_eq!(login.start_body(redirect), expected);
        }
    }

    /// Every way to name a browser method on the command line, with the email the
    /// prompt answers when the flag is left out and the exact request it must make.
    /// The providers are spelled out rather than taken from [`BrowserLogin`], so a
    /// mix-up in either mapping fails here.
    #[tokio::test]
    async fn each_browser_action_starts_its_own_provider() {
        for (action, prompted, expected) in [
            (
                LoginAction::Github,
                None,
                json!({"provider": "AUTH_PROVIDER_GITHUB"}),
            ),
            (
                LoginAction::Google,
                None,
                json!({"provider": "AUTH_PROVIDER_GOOGLE"}),
            ),
            (
                LoginAction::Microsoft,
                None,
                json!({"provider": "AUTH_PROVIDER_MICROSOFT"}),
            ),
            (
                LoginAction::Sso {
                    email: Some("flag@acme.test".into()),
                },
                None,
                json!({"provider": "AUTH_PROVIDER_SSO", "email": "flag@acme.test"}),
            ),
            (
                LoginAction::Sso { email: None },
                Some("prompt@acme.test"),
                json!({"provider": "AUTH_PROVIDER_SSO", "email": "prompt@acme.test"}),
            ),
        ] {
            let fixture = Fixture::new().await;
            let mut host = ScriptedHost::new(&["code=c1&state=s1"], &[]);
            host = host.answering(&prompted.into_iter().collect::<Vec<_>>(), &[]);
            let mut body = expected;
            body["redirectUri"] = json!(format!("http://localhost:{}/callback", host.port));
            fixture
                .expect(
                    "/v1/auth/login:start",
                    body,
                    200,
                    json!({"url": "https://idp.test/authorize?state=s1", "sessionId": "session-1"}),
                )
                .await;
            fixture
                .expect(
                    "/v1/auth/exchange",
                    json!({"sessionId": "session-1", "code": "c1", "state": "s1"}),
                    200,
                    tokens("person@test.dev"),
                )
                .await;

            let method = login_method(action, &fixture.client, &mut host).unwrap();
            let result = authenticate(&fixture.client, method, &mut host).await;

            assert_eq!(result.unwrap(), session("person@test.dev"));
            assert!(host.emails.is_empty());
        }
    }

    #[test]
    fn status_lists_one_workspace_per_line_by_name() {
        let workspaces: Vec<Workspace> = serde_json::from_value(json!([
            {"id": "ws-1", "displayName": "Zeta Labs"},
            {"id": "ws-2", "displayName": "alpha"},
            {"id": "ws-3", "slug": "only-a-slug"},
            {"id": "ws-4"},
        ]))
        .unwrap();

        assert_eq!(
            console::strip_ansi_codes(&status_view("person@test.dev", &workspaces)),
            "\
Email       person@test.dev
Workspaces  4

NAME         ID
alpha        ws-2
only-a-slug  ws-3
ws-4         ws-4
Zeta Labs    ws-1
"
        );
    }

    #[test]
    fn status_without_workspaces_prints_no_table() {
        assert_eq!(
            console::strip_ansi_codes(&status_view("person@test.dev", &[])),
            "Email       person@test.dev\nWorkspaces  none\n"
        );
    }

    #[tokio::test]
    async fn the_password_action_uses_the_flag_or_asks() {
        for (flag, prompted) in [
            (Some("person@test.dev".to_owned()), None),
            (None, Some("person@test.dev")),
        ] {
            let fixture = Fixture::new().await;
            let mut host = ScriptedHost::new(&[], &[])
                .answering(&prompted.into_iter().collect::<Vec<_>>(), &[" secret "]);
            fixture
                .expect(
                    "/v1/auth/password:sign-in",
                    password_body(),
                    200,
                    tokens("person@test.dev"),
                )
                .await;

            let method = login_method(
                LoginAction::Password { email: flag },
                &fixture.client,
                &mut host,
            )
            .unwrap();
            let result = authenticate(&fixture.client, method, &mut host).await;

            assert_eq!(result.unwrap(), session("person@test.dev"));
            assert!(host.emails.is_empty() && host.passwords.is_empty());
        }
    }

    #[tokio::test]
    async fn a_password_is_never_asked_for_over_an_unprotected_connection() {
        let home = tempfile::tempdir().unwrap();
        let client = CloudClient::with_paths(
            "http://helix.example.test".into(),
            home.path().join("credentials"),
        )
        .unwrap();
        // Nothing is scripted, so asking for an email or password would panic.
        let mut host = ScriptedHost::new(&[], &[]);

        let result = login_method(LoginAction::Password { email: None }, &client, &mut host);

        let error = result.err().expect("the login must be refused");
        assert!(error.to_string().contains("unencrypted"), "{error}");
        // Browser methods send no password, so they stay available.
        assert!(login_method(LoginAction::Github, &client, &mut host).is_ok());
    }

    #[test]
    fn auth_provider_converts_to_expected_login_action() {
        use crate::AuthProvider;
        assert_eq!(LoginAction::from(AuthProvider::Github), LoginAction::Github);
        assert_eq!(LoginAction::from(AuthProvider::Google), LoginAction::Google);
        assert_eq!(
            LoginAction::from(AuthProvider::Microsoft),
            LoginAction::Microsoft
        );
        assert_eq!(
            LoginAction::from(AuthProvider::Sso),
            LoginAction::Sso { email: None }
        );
    }

    #[tokio::test]
    async fn browser_login_exchanges_the_callback_code() {
        for login in [
            BrowserLogin::Github,
            BrowserLogin::Google,
            BrowserLogin::Microsoft,
            sso_login(),
        ] {
            let fixture = Fixture::new().await;
            let mut host = ScriptedHost::new(&["code=c1&state=s1"], &[]);
            fixture
                .expect_start(&host, login.clone(), "session-1")
                .await;
            fixture
                .expect(
                    "/v1/auth/exchange",
                    json!({"sessionId": "session-1", "code": "c1", "state": "s1"}),
                    200,
                    tokens("person@test.dev"),
                )
                .await;

            let result = fixture.run(LoginMethod::Browser(login), &mut host).await;

            assert_eq!(result.unwrap(), session("person@test.dev"));
            assert_eq!(host.opened, ["https://idp.test/authorize?state=s1"]);
        }
    }

    #[tokio::test]
    async fn social_login_continues_through_required_sso() {
        let fixture = Fixture::new().await;
        let mut host = ScriptedHost::new(&["code=c1&state=s1", "code=c2&state=s2"], &[]);
        fixture
            .expect_start(&host, BrowserLogin::Google, "session-1")
            .await;
        fixture
            .expect(
                "/v1/auth/exchange",
                json!({"sessionId": "session-1", "code": "c1", "state": "s1"}),
                200,
                json!({"email": "person@acme.test", "ssoRequired": true}),
            )
            .await;
        fixture.expect_start(&host, sso_login(), "session-2").await;
        fixture
            .expect(
                "/v1/auth/exchange",
                json!({"sessionId": "session-2", "code": "c2", "state": "s2"}),
                200,
                tokens("person@acme.test"),
            )
            .await;

        let result = fixture
            .run(LoginMethod::Browser(BrowserLogin::Google), &mut host)
            .await;

        assert_eq!(result.unwrap(), session("person@acme.test"));
        assert_eq!(host.opened.len(), 2);
    }

    #[tokio::test]
    async fn sso_required_during_sso_fails_instead_of_looping() {
        let fixture = Fixture::new().await;
        let mut host = ScriptedHost::new(&["code=c1&state=s1"], &[]);
        fixture.expect_start(&host, sso_login(), "session-1").await;
        fixture
            .expect(
                "/v1/auth/exchange",
                json!({"sessionId": "session-1", "code": "c1", "state": "s1"}),
                200,
                json!({"email": "person@acme.test", "ssoRequired": true}),
            )
            .await;

        let error = fixture
            .run(LoginMethod::Browser(sso_login()), &mut host)
            .await
            .unwrap_err();

        assert!(error.to_string().contains("did not complete"), "{error}");
    }

    #[tokio::test]
    async fn browser_email_verification_reuses_the_login_session() {
        let fixture = Fixture::new().await;
        let mut host = ScriptedHost::new(&["code=c1&state=s1"], &[" 123456 "]);
        fixture
            .expect_start(&host, BrowserLogin::Github, "session-1")
            .await;
        // The gateway may omit every empty field from this reply.
        fixture
            .expect(
                "/v1/auth/exchange",
                json!({"sessionId": "session-1", "code": "c1", "state": "s1"}),
                200,
                json!({"email": "person@test.dev", "emailVerificationRequired": true}),
            )
            .await;
        fixture
            .expect(
                "/v1/auth/verify-email",
                json!({"sessionId": "session-1", "code": "123456"}),
                200,
                tokens("person@test.dev"),
            )
            .await;

        let result = fixture
            .run(LoginMethod::Browser(BrowserLogin::Github), &mut host)
            .await;

        assert_eq!(result.unwrap(), session("person@test.dev"));
    }

    #[tokio::test]
    async fn password_login_returns_the_session() {
        let fixture = Fixture::new().await;
        let mut host = ScriptedHost::new(&[], &[]);
        fixture
            .expect(
                "/v1/auth/password:sign-in",
                password_body(),
                200,
                // The server sends every field; empty flags must not confuse parsing.
                json!({
                    "accessToken": "access",
                    "refreshToken": "refresh",
                    "expiresAt": "1999999999",
                    "email": "person@test.dev",
                    "emailVerificationRequired": false,
                    "ssoRequired": false,
                    "sessionId": "",
                    "workspaceId": "workspace",
                }),
            )
            .await;

        let result = fixture.run(password_login(), &mut host).await;

        assert_eq!(result.unwrap(), session("person@test.dev"));
        assert!(host.opened.is_empty());
    }

    #[tokio::test]
    async fn a_rejected_password_login_explains_itself_without_the_password() {
        // Not every 401 means a bad password, so the server's wording is kept.
        for message in [
            "invalid email or password",
            "no workspace assigned for user",
        ] {
            let fixture = Fixture::new().await;
            let mut host = ScriptedHost::new(&[], &[]);
            fixture
                .expect(
                    "/v1/auth/password:sign-in",
                    password_body(),
                    401,
                    json!({"code": 16, "message": message}),
                )
                .await;

            let error = fixture.run(password_login(), &mut host).await.unwrap_err();

            assert_eq!(error.to_string(), message);
            let cli_error = error.downcast_ref::<CliError>().unwrap();
            assert!(cli_error
                .hint
                .as_deref()
                .unwrap()
                .contains("helix-db.com/login"));
            assert!(!format!("{error:?}").contains("secret"));
        }
    }

    #[tokio::test]
    async fn a_rejection_without_a_message_still_says_something() {
        for body in ["", "   ", "<html><body>Unauthorized</body></html>"] {
            let fixture = Fixture::new().await;
            let mut host = ScriptedHost::new(&[], &[]);
            Mock::given(method("POST"))
                .and(path("/v1/auth/password:sign-in"))
                .respond_with(ResponseTemplate::new(401).set_body_string(body))
                .mount(&fixture.server)
                .await;

            let error = fixture.run(password_login(), &mut host).await.unwrap_err();

            assert_eq!(error.to_string(), "the sign-in was rejected", "{body}");
        }
    }

    #[tokio::test]
    async fn other_password_failures_keep_the_server_message() {
        let fixture = Fixture::new().await;
        let mut host = ScriptedHost::new(&[], &[]);
        fixture
            .expect(
                "/v1/auth/password:sign-in",
                password_body(),
                403,
                json!({"code": 7, "message": "email not permitted in this environment"}),
            )
            .await;

        let error = fixture.run(password_login(), &mut host).await.unwrap_err();

        assert_eq!(error.downcast_ref::<HttpError>().unwrap().status, 403);
        assert!(error.to_string().contains("not permitted"), "{error}");
    }

    #[tokio::test]
    async fn password_login_continues_through_required_sso() {
        let fixture = Fixture::new().await;
        let mut host = ScriptedHost::new(&["code=c1&state=s1"], &[]);
        fixture
            .expect(
                "/v1/auth/password:sign-in",
                password_body(),
                200,
                json!({"email": "person@acme.test", "ssoRequired": true}),
            )
            .await;
        fixture.expect_start(&host, sso_login(), "session-1").await;
        fixture
            .expect(
                "/v1/auth/exchange",
                json!({"sessionId": "session-1", "code": "c1", "state": "s1"}),
                200,
                tokens("person@acme.test"),
            )
            .await;

        let result = fixture.run(password_login(), &mut host).await;

        assert_eq!(result.unwrap(), session("person@acme.test"));
    }

    #[tokio::test]
    async fn password_email_verification_uses_the_returned_session() {
        let fixture = Fixture::new().await;
        let mut host = ScriptedHost::new(&[], &["123456"]);
        fixture
            .expect(
                "/v1/auth/password:sign-in",
                password_body(),
                200,
                json!({
                    "email": "person@test.dev",
                    "emailVerificationRequired": true,
                    "sessionId": "password-session",
                }),
            )
            .await;
        fixture
            .expect(
                "/v1/auth/verify-email",
                json!({"sessionId": "password-session", "code": "123456"}),
                200,
                tokens("person@test.dev"),
            )
            .await;

        let result = fixture.run(password_login(), &mut host).await;

        assert_eq!(result.unwrap(), session("person@test.dev"));
    }

    /// Sign-in that pauses on verification, for the retry tests.
    async fn pause_on_verification(fixture: &Fixture) {
        fixture
            .expect(
                "/v1/auth/password:sign-in",
                password_body(),
                200,
                json!({
                    "email": "person@test.dev",
                    "emailVerificationRequired": true,
                    "sessionId": "session-1",
                }),
            )
            .await;
    }

    #[tokio::test]
    async fn a_mistyped_code_can_be_retried() {
        let fixture = Fixture::new().await;
        let mut host = ScriptedHost::new(&[], &["wrong", "123456"]);
        pause_on_verification(&fixture).await;
        fixture
            .expect(
                "/v1/auth/verify-email",
                json!({"sessionId": "session-1", "code": "wrong"}),
                400,
                json!({"code": 3, "message": "verification code is incorrect"}),
            )
            .await;
        fixture
            .expect(
                "/v1/auth/verify-email",
                json!({"sessionId": "session-1", "code": "123456"}),
                200,
                tokens("person@test.dev"),
            )
            .await;

        let result = fixture.run(password_login(), &mut host).await;

        assert_eq!(result.unwrap(), session("person@test.dev"));
        assert!(host.codes.is_empty());
    }

    #[tokio::test]
    async fn verification_retries_through_an_outage() {
        let fixture = Fixture::new().await;
        let mut host = ScriptedHost::new(&[], &["123456", "123456"]);
        pause_on_verification(&fixture).await;
        let body = json!({"sessionId": "session-1", "code": "123456"});
        fixture
            .expect(
                "/v1/auth/verify-email",
                body.clone(),
                503,
                json!({"code": 14, "message": "email verification is temporarily unavailable"}),
            )
            .await;
        fixture
            .expect(
                "/v1/auth/verify-email",
                body,
                200,
                tokens("person@test.dev"),
            )
            .await;

        let result = fixture.run(password_login(), &mut host).await;

        assert_eq!(result.unwrap(), session("person@test.dev"));
    }

    #[tokio::test]
    async fn verification_stops_after_three_wrong_codes() {
        let fixture = Fixture::new().await;
        let mut host = ScriptedHost::new(&[], &["1", "2", "3"]);
        pause_on_verification(&fixture).await;
        for code in ["1", "2", "3"] {
            fixture
                .expect(
                    "/v1/auth/verify-email",
                    json!({"sessionId": "session-1", "code": code}),
                    400,
                    json!({"code": 3, "message": "verification code is incorrect"}),
                )
                .await;
        }

        let error = fixture.run(password_login(), &mut host).await.unwrap_err();

        assert_eq!(error.to_string(), "verification code is incorrect");
        assert!(error
            .downcast_ref::<CliError>()
            .unwrap()
            .hint
            .as_deref()
            .unwrap()
            .contains("start over"));
        assert!(host.codes.is_empty());
    }

    #[tokio::test]
    async fn an_expired_verification_is_not_retried() {
        let fixture = Fixture::new().await;
        let mut host = ScriptedHost::new(&[], &["123456", "unused"]);
        pause_on_verification(&fixture).await;
        fixture
            .expect(
                "/v1/auth/verify-email",
                json!({"sessionId": "session-1", "code": "123456"}),
                401,
                json!({"code": 16, "message": "email verification failed; start a new login"}),
            )
            .await;

        let error = fixture.run(password_login(), &mut host).await.unwrap_err();

        // Said once, though the server's wording starts the same way as ours used to.
        assert_eq!(
            error.to_string(),
            "email verification failed; start a new login"
        );
        assert_eq!(host.codes, ["unused"]);
    }

    #[tokio::test]
    async fn verification_failures_are_shown_on_one_clean_line() {
        for (reply, expected) in [
            (ResponseTemplate::new(401), "email verification failed"),
            (
                ResponseTemplate::new(401).set_body_string("<html>Bad gateway</html>"),
                "email verification failed",
            ),
            (
                ResponseTemplate::new(401).set_body_json(json!({"message": "no\u{1b}[31m good\n"})),
                "no[31m good",
            ),
        ] {
            let fixture = Fixture::new().await;
            let mut host = ScriptedHost::new(&[], &["123456"]);
            Mock::given(method("POST"))
                .and(path("/v1/auth/verify-email"))
                .respond_with(reply)
                .mount(&fixture.server)
                .await;

            let error = verify_email(&fixture.client, "session-1", "e", &mut host)
                .await
                .unwrap_err();

            assert_eq!(error.to_string(), expected);
        }
    }

    #[tokio::test]
    async fn an_outage_page_does_not_stop_verification() {
        let fixture = Fixture::new().await;
        let mut host = ScriptedHost::new(&[], &["123456", "123456"]);
        Mock::given(method("POST"))
            .and(path("/v1/auth/verify-email"))
            .respond_with(ResponseTemplate::new(503).set_body_string("<html>Down</html>"))
            .up_to_n_times(1)
            .mount(&fixture.server)
            .await;
        Mock::given(method("POST"))
            .and(path("/v1/auth/verify-email"))
            .respond_with(ResponseTemplate::new(200).set_body_json(tokens("person@test.dev")))
            .mount(&fixture.server)
            .await;

        let step = verify_email(&fixture.client, "session-1", "e", &mut host).await;

        assert_eq!(step.unwrap(), AuthStep::Session(session("person@test.dev")));
    }

    /// A server that hangs up on the first `hang_ups` requests and answers the next
    /// with `reply`. Hanging up is a failure no HTTP status can stand in for.
    async fn server_that_hangs_up(hang_ups: usize, reply: Value) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            for _ in 0..hang_ups {
                drop(listener.accept().await.unwrap());
            }
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut chunk = [0_u8; 4096];
            while !request.windows(6).any(|window| window == b"\"code\"") {
                let read = stream.read(&mut chunk).await.unwrap();
                request.extend_from_slice(&chunk[..read]);
            }
            let body = reply.to_string();
            stream
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        });
        format!("http://{address}")
    }

    #[tokio::test]
    async fn verification_retries_a_request_that_got_no_answer() {
        let home = tempfile::tempdir().unwrap();
        let client = CloudClient::with_paths(
            server_that_hangs_up(1, tokens("person@test.dev")).await,
            home.path().join("credentials"),
        )
        .unwrap();
        let mut host = ScriptedHost::new(&[], &["123456", "123456"]);

        let step = verify_email(&client, "session-1", "e", &mut host).await;

        assert_eq!(step.unwrap(), AuthStep::Session(session("person@test.dev")));
        assert!(host.codes.is_empty());
    }

    #[tokio::test]
    async fn verification_stops_when_requests_keep_getting_no_answer() {
        let home = tempfile::tempdir().unwrap();
        let client = CloudClient::with_paths(
            server_that_hangs_up(3, json!({})).await,
            home.path().join("credentials"),
        )
        .unwrap();
        let mut host = ScriptedHost::new(&[], &["1", "2", "3", "unused"]);

        let error = verify_email(&client, "session-1", "e", &mut host)
            .await
            .unwrap_err();

        assert!(error.to_string().contains("could not reach"), "{error}");
        assert_eq!(host.codes, ["unused"]);
    }

    #[tokio::test]
    async fn an_undecodable_verification_reply_is_not_retried() {
        let fixture = Fixture::new().await;
        let mut host = ScriptedHost::new(&[], &["123456", "unused"]);
        Mock::given(method("POST"))
            .and(path("/v1/auth/verify-email"))
            .respond_with(ResponseTemplate::new(200).set_body_string("not json"))
            .mount(&fixture.server)
            .await;

        let error = verify_email(&fixture.client, "session-1", "e", &mut host)
            .await
            .unwrap_err();

        assert!(error.to_string().contains("decode"), "{error}");
        assert_eq!(host.codes, ["unused"]);
    }

    #[tokio::test]
    async fn a_server_asking_to_verify_again_ends_the_login() {
        let fixture = Fixture::new().await;
        let mut host = ScriptedHost::new(&[], &["123456", "unused"]);
        pause_on_verification(&fixture).await;
        fixture
            .expect(
                "/v1/auth/verify-email",
                json!({"sessionId": "session-1", "code": "123456"}),
                200,
                json!({
                    "email": "person@test.dev",
                    "emailVerificationRequired": true,
                    "sessionId": "session-1",
                }),
            )
            .await;

        let error = fixture.run(password_login(), &mut host).await.unwrap_err();

        assert!(error.to_string().contains("again"), "{error}");
        assert_eq!(host.codes, ["unused"]);
    }

    #[tokio::test]
    async fn verification_can_end_in_required_sso() {
        let fixture = Fixture::new().await;
        let mut host = ScriptedHost::new(&["code=c1&state=s2"], &["123456"]);
        pause_on_verification(&fixture).await;
        fixture
            .expect(
                "/v1/auth/verify-email",
                json!({"sessionId": "session-1", "code": "123456"}),
                200,
                json!({"email": "person@acme.test", "ssoRequired": true}),
            )
            .await;
        fixture.expect_start(&host, sso_login(), "session-2").await;
        fixture
            .expect(
                "/v1/auth/exchange",
                json!({"sessionId": "session-2", "code": "c1", "state": "s2"}),
                200,
                tokens("person@acme.test"),
            )
            .await;

        let result = fixture.run(password_login(), &mut host).await;

        assert_eq!(result.unwrap(), session("person@acme.test"));
    }

    #[test]
    fn replies_parse_into_the_step_they_ask_for() {
        let parse = |reply: Value| -> Result<AuthStep> {
            serde_json::from_value::<AuthResponse>(reply)?.try_into()
        };

        assert_eq!(
            parse(tokens("person@test.dev")).unwrap(),
            AuthStep::Session(session("person@test.dev"))
        );
        assert_eq!(
            parse(json!({
                "email": "person@test.dev",
                "emailVerificationRequired": true,
                "sessionId": "session-1",
            }))
            .unwrap(),
            AuthStep::VerifyEmail {
                session_id: "session-1".into(),
                email: "person@test.dev".into(),
            }
        );
        // Required single sign-on outranks verification, as it does on the server.
        assert_eq!(
            parse(json!({
                "email": "person@acme.test",
                "ssoRequired": true,
                "emailVerificationRequired": true,
                "sessionId": "session-1",
            }))
            .unwrap(),
            AuthStep::SsoRequired {
                email: "person@acme.test".into(),
            }
        );
    }

    #[test]
    fn incomplete_replies_are_errors() {
        let complete = tokens("person@test.dev");
        let without = |field: &str| {
            let mut reply = complete.clone();
            reply.as_object_mut().unwrap().remove(field);
            reply
        };
        for reply in [
            json!({}),
            without("accessToken"),
            without("refreshToken"),
            without("expiresAt"),
            without("email"),
            json!({"accessToken": " ", "refreshToken": " ", "expiresAt": "1", "email": "e"}),
            json!({"accessToken": "a", "refreshToken": "r", "expiresAt": "0", "email": "e"}),
            json!({"email": "person@test.dev", "emailVerificationRequired": true}),
            json!({"ssoRequired": true}),
        ] {
            let parsed: Result<AuthStep> = serde_json::from_value::<AuthResponse>(reply.clone())
                .unwrap()
                .try_into();
            assert!(
                parsed.unwrap_err().to_string().contains("login response"),
                "{reply}"
            );
        }
    }

    #[tokio::test]
    async fn a_refused_browser_login_stops_before_the_exchange() {
        let fixture = Fixture::new().await;
        let mut host = ScriptedHost::new(
            &["state=s1&error=access_denied&error_description=Domain%20not%20allowed"],
            &[],
        );
        fixture
            .expect_start(&host, BrowserLogin::Microsoft, "session-1")
            .await;

        let error = fixture
            .run(LoginMethod::Browser(BrowserLogin::Microsoft), &mut host)
            .await
            .unwrap_err();

        assert!(error.to_string().contains("Domain not allowed"), "{error}");
        // Only the login start reached the server: no exchange was attempted.
        assert_eq!(fixture.server.received_requests().await.unwrap().len(), 1);
    }

    /// Required single sign-on binds the same fixed port a second time.
    #[tokio::test]
    async fn the_callback_port_is_free_again_after_the_callback() {
        let listener = bind_callback(0).await.unwrap();
        let address = listener.local_addr().unwrap();
        let callback = tokio::spawn(async move { receive_callback(listener, "xyz", || {}).await });
        send_raw(address, &callback_request("/callback?code=abc&state=xyz")).await;
        callback.await.unwrap().unwrap();

        bind_callback(address.port()).await.unwrap();
    }

    #[tokio::test]
    async fn a_busy_callback_port_fails_before_starting_the_login() {
        let fixture = Fixture::new().await;
        let mut host = ScriptedHost::new(&[], &[]);
        host.port_busy = true;

        let error = fixture
            .run(LoginMethod::Browser(BrowserLogin::Github), &mut host)
            .await
            .unwrap_err();

        assert!(error.to_string().contains("callback listener"), "{error}");
        assert!(fixture.server.received_requests().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_login_url_without_state_is_refused_before_opening_the_browser() {
        for url in [
            "https://idp.test/authorize",
            // No redirect could ever match this one.
            "https://idp.test/authorize?state=",
        ] {
            let fixture = Fixture::new().await;
            let mut host = ScriptedHost::new(&[], &[]);
            let redirect = format!("http://localhost:{}/callback", host.port);
            fixture
                .expect(
                    "/v1/auth/login:start",
                    BrowserLogin::Github.start_body(&redirect),
                    200,
                    json!({"url": url, "sessionId": "session-1"}),
                )
                .await;

            let error = fixture
                .run(LoginMethod::Browser(BrowserLogin::Github), &mut host)
                .await
                .unwrap_err();

            assert!(
                error.to_string().contains("did not include a state"),
                "{url}"
            );
            assert!(host.opened.is_empty());
        }
    }

    /// Sends `request` to the callback port and returns the reply.
    async fn send_raw(address: std::net::SocketAddr, request: &[u8]) -> String {
        let mut stream = TcpStream::connect(address).await.unwrap();
        stream.write_all(request).await.unwrap();
        let mut reply = String::new();
        stream.read_to_string(&mut reply).await.unwrap();
        reply
    }

    fn callback_request(target: &str) -> Vec<u8> {
        format!("GET {target} HTTP/1.1\r\nHost: localhost\r\n\r\n").into_bytes()
    }

    /// Runs a callback listener expecting `state=xyz`, feeds it `requests`, and
    /// returns its result with the reply to the last request.
    async fn callback_result(requests: &[Vec<u8>]) -> (Result<String>, String) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let callback = tokio::spawn(async move { receive_callback(listener, "xyz", || {}).await });
        let mut reply = String::new();
        for request in requests {
            reply = send_raw(address, request).await;
        }
        // Without this bound, a request wrongly taken for a stray would hang the test.
        let result = timeout(Duration::from_secs(10), callback)
            .await
            .expect("the callback must finish");
        (result.unwrap(), reply)
    }

    #[tokio::test]
    async fn callback_returns_the_code_for_the_pending_state() {
        let (result, reply) =
            callback_result(&[callback_request("/callback?code=abc&state=xyz")]).await;

        assert_eq!(result.unwrap(), "abc");
        assert!(reply.starts_with("HTTP/1.1 200"), "{reply}");
        // Single sign-on or verification may still follow.
        assert!(reply.contains("Return to the terminal"), "{reply}");
    }

    #[tokio::test]
    async fn callback_without_a_code_fails_the_login() {
        for target in ["/callback?state=xyz", "/callback?code=&state=xyz"] {
            let (result, reply) = callback_result(&[callback_request(target)]).await;

            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("did not contain a code"),
                "{target}"
            );
            assert!(reply.starts_with("HTTP/1.1 400"), "{reply}");
        }
    }

    #[tokio::test]
    async fn callback_surfaces_identity_provider_errors() {
        let (result, reply) =
            callback_result(&[callback_request("/callback?state=xyz&error=access_denied")]).await;
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("refused the login: access_denied"));
        assert!(reply.starts_with("HTTP/1.1 400"), "{reply}");

        // The description wins, an error wins over a code, and terminal control
        // characters never reach the screen.
        let (result, _) = callback_result(&[callback_request(
            "/callback?code=abc&state=xyz&error=denied&error_description=No%20%1B%5B31maccess",
        )])
        .await;
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("refused the login: No [31maccess"));
    }

    #[tokio::test]
    async fn requests_that_are_not_the_redirect_do_not_end_the_login() {
        for stray in [
            callback_request("/favicon.ico"),
            callback_request("/favicon.ico?code=abc&state=xyz"),
            callback_request("/callback"),
            callback_request("/callback?code=abc"),
            callback_request("/callback?code=evil&state=other"),
            callback_request("/callback?error=denied"),
            callback_request("/callback?error=denied&error_description=Run%20this&state=other"),
            b"not http\r\n\r\n".to_vec(),
            b"\xff\xfe\xfd\r\n\r\n".to_vec(),
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let callback =
                tokio::spawn(async move { receive_callback(listener, "xyz", || {}).await });

            let reply = send_raw(address, &stray).await;
            assert!(reply.starts_with("HTTP/1.1 404"), "{stray:?}: {reply}");
            assert!(!callback.is_finished());

            send_raw(address, &callback_request("/callback?code=abc&state=xyz")).await;
            assert_eq!(callback.await.unwrap().unwrap(), "abc", "{stray:?}");
        }
    }

    #[tokio::test]
    async fn an_unmatched_provider_error_is_flagged_once_without_ending_the_login() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let flagged = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let count = flagged.clone();
        let callback = tokio::spawn(async move {
            receive_callback(listener, "xyz", move || {
                count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            })
            .await
        });

        for request in [
            "/callback?error=access_denied&error_description=Run%20this",
            "/callback?error=access_denied&state=other",
        ] {
            let reply = send_raw(address, &callback_request(request)).await;
            assert!(reply.starts_with("HTTP/1.1 404"), "{reply}");
            assert!(reply.contains("log in again"), "{reply}");
            // Any web page can send this, so none of its text is repeated.
            assert!(!reply.contains("Run this"), "{reply}");
            sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(flagged.load(std::sync::atomic::Ordering::SeqCst), 1);

        send_raw(address, &callback_request("/callback?code=abc&state=xyz")).await;
        assert_eq!(callback.await.unwrap().unwrap(), "abc");
    }

    #[tokio::test]
    async fn headers_that_are_not_text_do_not_hide_the_redirect() {
        let (result, reply) = callback_result(&[
            b"GET /callback?code=abc&state=xyz HTTP/1.1\r\nCookie: \xff\xfe\r\n\r\n".to_vec(),
        ])
        .await;

        assert_eq!(result.unwrap(), "abc");
        assert!(reply.starts_with("HTTP/1.1 200"), "{reply}");
    }

    /// Closing with unread bytes resets the connection, which can cost the browser
    /// the reply on some systems. Linux delivers it anyway, so this checks the order
    /// directly: no reply until the request has ended.
    #[tokio::test]
    async fn the_reply_waits_for_the_end_of_the_request() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let callback = tokio::spawn(async move { receive_callback(listener, "xyz", || {}).await });

        let mut stream = TcpStream::connect(address).await.unwrap();
        stream
            .write_all(b"GET /callback?code=abc&state=xyz HTTP/1.1\r\nCookie: a=b\r\n")
            .await
            .unwrap();
        let mut early = [0_u8; 1];
        assert!(
            timeout(Duration::from_millis(100), stream.read(&mut early))
                .await
                .is_err(),
            "replied before the request ended"
        );

        stream.write_all(b"\r\n").await.unwrap();
        let mut reply = String::new();
        stream.read_to_string(&mut reply).await.unwrap();
        assert!(reply.starts_with("HTTP/1.1 200"), "{reply}");
        assert_eq!(callback.await.unwrap().unwrap(), "abc");
    }

    /// Cookies can make a request far larger than one read.
    #[tokio::test]
    async fn a_large_request_still_gets_its_reply() {
        let cookie = "c".repeat(200 * 1024);
        let (result, reply) = callback_result(&[format!(
            "GET /callback?code=abc&state=xyz HTTP/1.1\r\nCookie: {cookie}\r\n\r\n"
        )
        .into_bytes()])
        .await;

        assert_eq!(result.unwrap(), "abc");
        assert!(reply.starts_with("HTTP/1.1 200"), "{reply}");
    }

    #[tokio::test]
    async fn a_request_line_split_across_reads_still_counts() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let callback = tokio::spawn(async move { receive_callback(listener, "xyz", || {}).await });

        let mut stream = TcpStream::connect(address).await.unwrap();
        stream.write_all(b"GET /callback?code=ab").await.unwrap();
        stream.flush().await.unwrap();
        sleep(Duration::from_millis(50)).await;
        stream
            .write_all(b"c&state=xyz HTTP/1.1\r\n\r\n")
            .await
            .unwrap();

        let code = timeout(Duration::from_secs(10), callback).await.unwrap();
        assert_eq!(code.unwrap().unwrap(), "abc");
    }

    /// Handled one at a time, these connections would take far longer than the
    /// bound here: each would hold the redirect up for a whole read timeout.
    #[tokio::test]
    async fn idle_or_closed_connections_do_not_hold_up_the_redirect() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let callback = tokio::spawn(async move { receive_callback(listener, "xyz", || {}).await });

        drop(TcpStream::connect(address).await.unwrap());
        let mut idle = Vec::new();
        for _ in 0..20 {
            idle.push(TcpStream::connect(address).await.unwrap());
        }

        let code = timeout(CALLBACK_READ_TIMEOUT / 2, async {
            send_raw(address, &callback_request("/callback?code=abc&state=xyz")).await;
            callback.await
        })
        .await
        .expect("idle connections held up the redirect");
        assert_eq!(code.unwrap().unwrap(), "abc");
    }
}
