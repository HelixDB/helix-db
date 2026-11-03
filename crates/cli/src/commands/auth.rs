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

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StartLoginResponse {
    url: String,
    session_id: String,
}

/// The reply shared by code exchange, password sign-in and email verification.
/// Tokens are absent or empty whenever a flag asks for another step.
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
    /// Set by password sign-in only; browser logins already hold their session.
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

/// Where a sign-in ended up once any email verification is done.
#[derive(Debug, PartialEq, Eq)]
enum SignedIn {
    Session(SessionCredentials),
    /// The email's company only allows its own identity provider.
    SsoRequired {
        email: String,
    },
}

impl TryFrom<AuthResponse> for SignedIn {
    type Error = eyre::Report;

    fn try_from(response: AuthResponse) -> Result<Self> {
        if response.sso_required {
            return Ok(Self::SsoRequired {
                email: response.email,
            });
        }
        if response.access_token.is_empty() || response.refresh_token.is_empty() {
            return Err(eyre!("the login response did not include a session"));
        }
        Ok(Self::Session(SessionCredentials {
            access_token: response.access_token,
            refresh_token: response.refresh_token,
            expires_at: response.expires_at,
            email: response.email,
        }))
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

/// Runs `method` to a session. When the server answers that the email must use
/// single sign-on, continues once through that email's identity provider, the
/// way the web login does.
///
/// A loop rather than recursion because recursive `async fn`s need boxing. It
/// ends because `SsoRequired` during single sign-on is an error.
async fn authenticate(
    client: &CloudClient,
    mut method: LoginMethod,
    callback_port: u16,
    host: &mut impl LoginHost,
) -> Result<SessionCredentials> {
    loop {
        match (
            sign_in(client, &method, callback_port, host).await?,
            &method,
        ) {
            (SignedIn::Session(session), _) => return Ok(session),
            (SignedIn::SsoRequired { email }, LoginMethod::Browser(BrowserLogin::Sso { .. })) => {
                return Err(CliError::new(format!(
                    "{email} must sign in with single sign-on, which did not complete"
                ))
                .with_hint("ask your workspace admin to check the single sign-on connection")
                .into());
            }
            (SignedIn::SsoRequired { email }, _) => {
                output::info(&format!(
                    "{email} signs in with single sign-on; continuing through your company's identity provider"
                ));
                method = LoginMethod::Browser(BrowserLogin::Sso { email });
            }
        }
    }
}

/// One sign-in attempt, through email verification when the server asks for it.
async fn sign_in(
    client: &CloudClient,
    method: &LoginMethod,
    callback_port: u16,
    host: &mut impl LoginHost,
) -> Result<SignedIn> {
    let (response, session_id) = match method {
        LoginMethod::Browser(login) => browser_sign_in(client, login, callback_port, host).await?,
        LoginMethod::Password { email, password } => {
            let response = password_sign_in(client, email, password).await?;
            let session_id = response.session_id.clone();
            (response, session_id)
        }
    };
    if response.email_verification_required && !response.sso_required {
        return verify_email(client, &session_id, &response.email, host)
            .await?
            .try_into();
    }
    response.try_into()
}

/// Signs in through the browser and exchanges the returned code. Returns the
/// exchange reply with the session id any email verification has to reuse.
async fn browser_sign_in(
    client: &CloudClient,
    login: &BrowserLogin,
    callback_port: u16,
    host: &mut impl LoginHost,
) -> Result<(AuthResponse, String)> {
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

    host.open_browser(&started.url);
    let mut waiting = Step::with_messages("Waiting for the browser", "Signed in with the browser");
    waiting.start();
    let callback = timeout(LOGIN_TIMEOUT, receive_callback(listener)).await;
    let (code, state) = match callback {
        Ok(Ok(callback)) => {
            waiting.done();
            callback
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
    let response = serde_json::from_value(
        client
            .public_post(
                "/v1/auth/exchange",
                json!({"sessionId": started.session_id, "code": code, "state": state}),
                "exchange WorkOS authorization code",
            )
            .await?,
    )?;
    Ok((response, started.session_id))
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
        if error
            .downcast_ref::<HttpError>()
            .is_some_and(|http| http.status == StatusCode::UNAUTHORIZED)
        {
            return CliError::new("invalid email or password")
                .with_hint(
                    "sign up or reset your password at https://helix-db.com/login, \
                     or run `helix auth login` to pick another method",
                )
                .into();
        }
        error
    })?;
    Ok(serde_json::from_value(value)?)
}

/// Asks for the emailed code until it is accepted. The server keeps the pending
/// sign-in through a wrong code (HTTP 400), so only that case asks again.
async fn verify_email(
    client: &CloudClient,
    session_id: &str,
    email: &str,
    host: &mut impl LoginHost,
) -> Result<AuthResponse> {
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
        match response {
            Ok(value) => return Ok(serde_json::from_value(value)?),
            Err(error)
                if attempt < MAX_VERIFICATION_ATTEMPTS
                    && error
                        .downcast_ref::<HttpError>()
                        .is_some_and(|http| http.status == StatusCode::BAD_REQUEST) =>
            {
                output::warning("That code is incorrect. Try again.");
                attempt += 1;
            }
            Err(error) => return Err(error),
        }
    }
}

async fn receive_callback(listener: TcpListener) -> Result<(String, String)> {
    let (mut stream, _) = listener.accept().await?;
    let mut request = vec![0_u8; 8192];
    let read = stream.read(&mut request).await?;
    let first_line = std::str::from_utf8(&request[..read])?
        .lines()
        .next()
        .ok_or_else(|| eyre!("browser callback was empty"))?;
    let target = first_line
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| eyre!("browser callback was malformed"))?;
    let url = reqwest::Url::parse(&format!("http://127.0.0.1{target}"))?;
    let param = |name: &str| {
        url.query_pairs()
            .find_map(|(key, value)| (key == name).then(|| value.into_owned()))
            .filter(|value| !value.is_empty())
    };
    let result: Result<(String, String)> = match (param("code"), param("state"), param("error")) {
        (_, _, Some(error)) => Err(CliError::new(format!(
            "the identity provider refused the login: {}",
            param("error_description").unwrap_or(error)
        ))
        .with_hint("run `helix auth login` to try again or pick another method")
        .into()),
        (Some(code), Some(state), None) => Ok((code, state)),
        _ => Err(eyre!("WorkOS callback did not contain code and state")),
    };
    let (status, body) = if result.is_ok() {
        (
            "200 OK",
            "Helix CLI login complete. You can close this window.",
        )
    } else {
        (
            "400 Bad Request",
            "Helix CLI login failed. Return to the terminal.",
        )
    };
    stream
        .write_all(
            format!(
                "HTTP/1.1 {status}\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
            .as_bytes(),
        )
        .await?;
    result
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
            self.expect(
                "/v1/auth/login:start",
                login.start_body(&redirect),
                200,
                json!({"url": format!("https://idp.test/{session}"), "sessionId": session}),
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
            assert_eq!(host.opened, ["https://idp.test/session-1"]);
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
    async fn wrong_password_is_a_clear_error_without_the_password() {
        let fixture = Fixture::new().await;
        let mut host = ScriptedHost::new(&[], &[]);
        fixture
            .expect(
                "/v1/auth/password:sign-in",
                password_body(),
                401,
                json!({"code": 16, "message": "invalid email or password"}),
            )
            .await;

        let error = fixture.run(password_login(), &mut host).await.unwrap_err();

        let shown = format!("{error:?}");
        assert!(shown.contains("invalid email or password"), "{shown}");
        assert!(!shown.contains("secret"), "{shown}");
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

        assert_eq!(error.downcast_ref::<HttpError>().unwrap().status, 400);
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

        assert_eq!(error.downcast_ref::<HttpError>().unwrap().status, 401);
        assert_eq!(host.codes, ["unused"]);
    }

    #[tokio::test]
    async fn verification_can_end_in_required_sso() {
        let fixture = Fixture::new().await;
        let mut host = ScriptedHost::new(&["code=c1&state=s1"], &["123456"]);
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
                json!({"sessionId": "session-2", "code": "c1", "state": "s1"}),
                200,
                tokens("person@acme.test"),
            )
            .await;

        let result = fixture.run(password_login(), &mut host).await;

        assert_eq!(result.unwrap(), session("person@acme.test"));
    }

    #[tokio::test]
    async fn a_reply_without_tokens_or_flags_is_an_error() {
        for reply in [
            json!({}),
            json!({"email": "person@test.dev", "accessToken": "access"}),
            json!({"email": "person@test.dev", "refreshToken": "refresh"}),
        ] {
            let fixture = Fixture::new().await;
            let mut host = ScriptedHost::new(&[], &[]);
            fixture
                .expect("/v1/auth/password:sign-in", password_body(), 200, reply)
                .await;

            let error = fixture.run(password_login(), &mut host).await.unwrap_err();

            assert!(error.to_string().contains("did not include a session"));
        }
    }

    #[tokio::test]
    async fn a_refused_browser_login_stops_before_the_exchange() {
        let fixture = Fixture::new().await;
        let mut host = ScriptedHost::new(
            &["error=access_denied&error_description=Domain%20not%20allowed"],
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

    /// Serves one raw callback request and returns its parsed result and the
    /// HTTP status the browser saw.
    async fn callback_result(query: &str) -> (Result<(String, String)>, String) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let callback = tokio::spawn(receive_callback(listener));
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        stream
            .write_all(
                format!("GET /callback?{query} HTTP/1.1\r\nHost: localhost\r\n\r\n").as_bytes(),
            )
            .await
            .unwrap();
        let mut reply = String::new();
        stream.read_to_string(&mut reply).await.unwrap();
        (callback.await.unwrap(), reply)
    }

    #[tokio::test]
    async fn callback_requires_code_and_state() {
        let (result, reply) = callback_result("code=abc&state=xyz").await;
        assert_eq!(result.unwrap(), ("abc".into(), "xyz".into()));
        assert!(reply.starts_with("HTTP/1.1 200"), "{reply}");

        for query in [
            "code=abc",
            "state=xyz",
            "code=&state=xyz",
            "code=abc&state=",
            "",
        ] {
            let (result, reply) = callback_result(query).await;
            assert!(
                result.unwrap_err().to_string().contains("code and state"),
                "{query}"
            );
            assert!(reply.starts_with("HTTP/1.1 400"), "{reply}");
        }
    }

    #[tokio::test]
    async fn callback_surfaces_identity_provider_errors() {
        let (result, reply) = callback_result("error=access_denied").await;
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("refused the login: access_denied"));
        assert!(reply.starts_with("HTTP/1.1 400"), "{reply}");

        // An error wins even if a code came along with it.
        let (result, _) =
            callback_result("code=abc&state=xyz&error=denied&error_description=No%20access").await;
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("refused the login: No access"));
    }
}
