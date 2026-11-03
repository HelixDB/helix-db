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
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::TcpListener,
    time::{timeout, Duration},
};

const LOGIN_TIMEOUT: Duration = Duration::from_secs(5 * 60);
/// Registered with WorkOS as `http://localhost:8765/callback`; another port
/// would be refused at the authorize step.
const LOGIN_CALLBACK_PORT: u16 = 8765;
/// WorkOS locks a pending verification after three wrong codes.
const MAX_VERIFICATION_ATTEMPTS: u32 = 3;
/// How long one connection to the callback port may stay silent. Browsers open
/// idle preconnect sockets, which would otherwise hold up the real redirect.
const CALLBACK_READ_TIMEOUT: Duration = Duration::from_secs(1);

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
    /// Sends the person to the identity provider's authorize `url`.
    fn open_browser(&mut self, url: &str);
    /// Asks for the verification code emailed to `email`.
    fn verification_code(&mut self, email: &str) -> Result<String>;
}

struct TerminalHost;

impl LoginHost for TerminalHost {
    fn open_browser(&mut self, url: &str) {
        match host_actions::open_url(url) {
            Ok(()) => output::info(&format!(
                "Opened your browser. If it did not open, visit:\n{url}"
            )),
            Err(_) => output::warning(&format!(
                "Could not open a browser. Visit this URL to log in:\n{url}"
            )),
        }
    }

    fn verification_code(&mut self, email: &str) -> Result<String> {
        prompts::input_required(&format!("Verification code sent to {email}"))
    }
}

pub async fn run(action: AuthAction) -> Result<()> {
    match action {
        AuthAction::Login { method } => login(method).await,
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
    let method = match action {
        LoginAction::Github => LoginMethod::Browser(BrowserLogin::Github),
        LoginAction::Google => LoginMethod::Browser(BrowserLogin::Google),
        LoginAction::Microsoft => LoginMethod::Browser(BrowserLogin::Microsoft),
        LoginAction::Sso { email } => LoginMethod::Browser(BrowserLogin::Sso {
            email: email.map_or_else(|| prompts::input_required("Work email"), Ok)?,
        }),
        LoginAction::Password { email } => LoginMethod::Password {
            email: email.map_or_else(|| prompts::input_required("Email"), Ok)?,
            password: prompts::password("Password")?,
        },
    };
    let credentials = authenticate(&client, method, LOGIN_CALLBACK_PORT, &mut TerminalHost).await?;
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

/// Runs `method` to a session, following each step the server asks for. When
/// the email must use single sign-on, continues once through its identity
/// provider, the way the web login does.
///
/// A loop rather than recursion because recursive `async fn`s need boxing. It
/// ends because verification is bounded and `SsoRequired` during single sign-on
/// is an error.
async fn authenticate(
    client: &CloudClient,
    mut method: LoginMethod,
    callback_port: u16,
    host: &mut impl LoginHost,
) -> Result<SessionCredentials> {
    let mut step = sign_in(client, &method, callback_port, host).await?;
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
                output::info(&format!(
                    "{email} signs in with single sign-on; continuing through your company's identity provider"
                ));
                method = LoginMethod::Browser(BrowserLogin::Sso { email });
                sign_in(client, &method, callback_port, host).await?
            }
        };
    }
}

/// One sign-in attempt, up to the first step the server answers with.
async fn sign_in(
    client: &CloudClient,
    method: &LoginMethod,
    callback_port: u16,
    host: &mut impl LoginHost,
) -> Result<AuthStep> {
    let response = match method {
        LoginMethod::Browser(login) => browser_sign_in(client, login, callback_port, host).await?,
        LoginMethod::Password { email, password } => {
            password_sign_in(client, email, password).await?
        }
    };
    response.try_into()
}

/// Signs in through the browser and exchanges the returned code.
async fn browser_sign_in(
    client: &CloudClient,
    login: &BrowserLogin,
    callback_port: u16,
    host: &mut impl LoginHost,
) -> Result<AuthResponse> {
    let listener = TcpListener::bind(("127.0.0.1", callback_port))
        .await
        .wrap_err_with(|| format!("bind the WorkOS callback listener on port {callback_port}"))?;
    let redirect_uri = format!("http://localhost:{callback_port}/callback");
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
        .ok_or_else(|| eyre!("the login URL did not include a state"))?;

    host.open_browser(&started.url);
    let mut waiting = Step::with_messages("Waiting for the browser", "Signed in with the browser");
    waiting.start();
    let callback = timeout(LOGIN_TIMEOUT, receive_callback(listener, &state)).await;
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
        client
            .public_post(
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
    let response = client
        .public_post(
            "/v1/auth/password:sign-in",
            json!({"email": email, "password": password}),
            "sign in with password",
        )
        .await;
    let value = response.map_err(|error| -> eyre::Report {
        match error.downcast_ref::<HttpError>() {
            Some(http) if http.status == StatusCode::UNAUTHORIZED => {
                CliError::new(http.message.clone())
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

/// Asks for the emailed code until the server takes it. Both a wrong code
/// (HTTP 400) and an outage (HTTP 503) leave the pending sign-in usable, so only
/// those ask again; anything else means the login has to start over.
async fn verify_email(
    client: &CloudClient,
    session_id: &str,
    email: &str,
    host: &mut impl LoginHost,
) -> Result<AuthStep> {
    let mut attempt = 1;
    loop {
        let code = host.verification_code(email)?;
        let response = client
            .public_post(
                "/v1/auth/verify-email",
                json!({"sessionId": session_id, "code": code.trim()}),
                "verify WorkOS email",
            )
            .await;
        let error = match response {
            Ok(value) => return serde_json::from_value::<AuthResponse>(value)?.try_into(),
            Err(error) => error,
        };
        let Some(http) = error.downcast_ref::<HttpError>() else {
            return Err(error);
        };
        if attempt >= MAX_VERIFICATION_ATTEMPTS
            || !matches!(
                http.status,
                StatusCode::BAD_REQUEST | StatusCode::SERVICE_UNAVAILABLE
            )
        {
            return Err(
                CliError::new(format!("email verification failed: {}", http.message))
                    .with_hint("run `helix auth login` to start over")
                    .into(),
            );
        }
        output::warning(&http.message);
        attempt += 1;
    }
}

/// Waits for the identity provider's redirect and returns its code.
///
/// Only a request to `/callback` carrying the `state` from the authorize URL
/// counts. Everything else is answered and ignored, so a favicon request, an
/// idle preconnect or another web page reaching this port cannot end the login.
async fn receive_callback(listener: TcpListener, state: &str) -> Result<String> {
    loop {
        let (mut stream, _) = listener.accept().await?;
        let mut request = vec![0_u8; 8192];
        let Ok(Ok(read)) = timeout(CALLBACK_READ_TIMEOUT, stream.read(&mut request)).await else {
            continue;
        };
        let url = std::str::from_utf8(&request[..read])
            .ok()
            .and_then(|request| request.lines().next())
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|target| reqwest::Url::parse(&format!("http://127.0.0.1{target}")).ok())
            .filter(|url| url.path() == "/callback");
        let param = |name: &str| {
            url.as_ref()?
                .query_pairs()
                .find_map(|(key, value)| (key == name).then(|| value.into_owned()))
                .filter(|value| !value.is_empty())
        };
        let result: Option<Result<String>> =
            (param("state").as_deref() == Some(state)).then(|| {
                match (param("code"), param("error")) {
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
                }
            });
        let (status, body) = match &result {
            None => ("404 Not Found", "Not a pending Helix CLI login."),
            Some(Ok(_)) => (
                "200 OK",
                "Helix CLI received the sign-in. Return to the terminal.",
            ),
            Some(Err(_)) => (
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
        let Some(result) = result else {
            continue;
        };
        return result;
    }
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
        let names = workspaces
            .iter()
            .map(|workspace| workspace.label())
            .collect::<Vec<_>>()
            .join(", ");
        print!(
            "{}",
            table::key_values(&[
                ("Email", email.clone()),
                (
                    "Workspaces",
                    if names.is_empty() {
                        "none".to_owned()
                    } else {
                        names
                    }
                ),
            ])
        );
        Ok(())
    })
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
    /// with the next scripted query, and answers code prompts from a list.
    struct ScriptedHost {
        port: u16,
        callbacks: VecDeque<&'static str>,
        codes: VecDeque<&'static str>,
        opened: Vec<String>,
    }

    impl ScriptedHost {
        fn new(callbacks: &[&'static str], codes: &[&'static str]) -> Self {
            let port = std::net::TcpListener::bind("127.0.0.1:0")
                .unwrap()
                .local_addr()
                .unwrap()
                .port();
            Self {
                port,
                callbacks: callbacks.iter().copied().collect(),
                codes: codes.iter().copied().collect(),
                opened: Vec::new(),
            }
        }
    }

    impl LoginHost for ScriptedHost {
        fn open_browser(&mut self, url: &str) {
            self.opened.push(url.to_owned());
            let query = self.callbacks.pop_front().expect("unexpected browser open");
            let port = self.port;
            tokio::spawn(async move {
                let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
                    .await
                    .unwrap();
                stream
                    .write_all(format!("GET /callback?{query} HTTP/1.1\r\n\r\n").as_bytes())
                    .await
                    .unwrap();
            });
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
            let port = host.port;
            authenticate(&self.client, login, port, host).await
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

        assert_eq!(
            error.to_string(),
            "email verification failed: verification code is incorrect"
        );
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

        assert!(error.to_string().contains("start a new login"), "{error}");
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

    #[tokio::test]
    async fn the_callback_port_is_free_again_after_each_login() {
        let fixture = Fixture::new().await;
        let mut host = ScriptedHost::new(&["code=c1&state=s1", "code=c2&state=s2"], &[]);
        for session_id in ["session-1", "session-2"] {
            fixture
                .expect_start(&host, BrowserLogin::Github, session_id)
                .await;
        }
        fixture
            .expect(
                "/v1/auth/exchange",
                json!({"sessionId": "session-1", "code": "c1", "state": "s1"}),
                200,
                tokens("person@test.dev"),
            )
            .await;
        fixture
            .expect(
                "/v1/auth/exchange",
                json!({"sessionId": "session-2", "code": "c2", "state": "s2"}),
                200,
                tokens("person@test.dev"),
            )
            .await;

        for _ in 0..2 {
            fixture
                .run(LoginMethod::Browser(BrowserLogin::Github), &mut host)
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn a_busy_callback_port_fails_before_starting_the_login() {
        let fixture = Fixture::new().await;
        let mut host = ScriptedHost::new(&[], &[]);
        let _occupied = TcpListener::bind(("127.0.0.1", host.port)).await.unwrap();

        let error = fixture
            .run(LoginMethod::Browser(BrowserLogin::Github), &mut host)
            .await
            .unwrap_err();

        assert!(error.to_string().contains("callback listener"), "{error}");
        assert!(fixture.server.received_requests().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_login_url_without_state_is_refused_before_opening_the_browser() {
        let fixture = Fixture::new().await;
        let mut host = ScriptedHost::new(&[], &[]);
        let redirect = format!("http://localhost:{}/callback", host.port);
        fixture
            .expect(
                "/v1/auth/login:start",
                BrowserLogin::Github.start_body(&redirect),
                200,
                json!({"url": "https://idp.test/authorize", "sessionId": "session-1"}),
            )
            .await;

        let error = fixture
            .run(LoginMethod::Browser(BrowserLogin::Github), &mut host)
            .await
            .unwrap_err();

        assert!(error.to_string().contains("did not include a state"));
        assert!(host.opened.is_empty());
    }

    /// Sends `request` to the callback port and returns the reply.
    async fn send_raw(address: std::net::SocketAddr, request: &[u8]) -> String {
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
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
        let callback = tokio::spawn(async move { receive_callback(listener, "xyz").await });
        let mut reply = String::new();
        for request in requests {
            reply = send_raw(address, request).await;
        }
        (callback.await.unwrap(), reply)
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
            b"not http".to_vec(),
            vec![0xff, 0xfe, 0xfd],
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let callback = tokio::spawn(async move { receive_callback(listener, "xyz").await });

            let reply = send_raw(address, &stray).await;
            assert!(reply.starts_with("HTTP/1.1 404"), "{stray:?}: {reply}");
            assert!(!callback.is_finished());

            send_raw(address, &callback_request("/callback?code=abc&state=xyz")).await;
            assert_eq!(callback.await.unwrap().unwrap(), "abc", "{stray:?}");
        }
    }

    #[tokio::test]
    async fn an_idle_or_closed_connection_does_not_hold_up_the_redirect() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let callback = tokio::spawn(async move { receive_callback(listener, "xyz").await });

        drop(tokio::net::TcpStream::connect(address).await.unwrap());
        let _idle = tokio::net::TcpStream::connect(address).await.unwrap();
        send_raw(address, &callback_request("/callback?code=abc&state=xyz")).await;

        assert_eq!(callback.await.unwrap().unwrap(), "abc");
    }
}
