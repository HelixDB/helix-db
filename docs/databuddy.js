// Databuddy analytics for docs.helix-db.com. Mintlify loads every .js file in
// this directory on each page, so this is the whole integration.
(() => {
  // HelixDB's production Databuddy site, shared with www.helix-db.com and the
  // dashboard so one visitor's journey spans all three. Client IDs are public.
  const CLIENT_ID = "b8daa65f-f99a-4a99-b8dd-03695d01cf82";
  const DOCS_HOST = "docs.helix-db.com";
  // Other HelixDB sites a docs reader continues to: the landing site and the
  // dashboard, where signup happens.
  const HANDOFF_HOSTS = new Set([
    "helix-db.com",
    "www.helix-db.com",
    "prod.app.helix-db.com",
  ]);

  // Local previews and Mintlify preview deployments stay out of production data.
  if (window.location.hostname !== DOCS_HOST) return;
  if (document.querySelector("script[data-helix-databuddy]")) return;

  const script = document.createElement("script");
  script.src = "https://cdn.databuddy.cc/databuddy.js";
  script.setAttribute("data-client-id", CLIENT_ID);
  script.setAttribute("data-track-outgoing-links", "true");
  script.setAttribute("data-track-web-vitals", "true");
  script.setAttribute("data-track-errors", "true");
  script.setAttribute("data-helix-databuddy", "true");
  script.crossOrigin = "anonymous";
  script.async = true;
  document.head.appendChild(script);

  // Each origin keeps its own Databuddy IDs. Adding the reader's IDs to links
  // for the other HelixDB sites as they are followed lets the tracker there
  // adopt them, so docs reads count toward signup funnels. These are the same
  // storage keys the Databuddy SDK's getTrackingIds() reads.
  function handOff(event) {
    const anchor =
      event.target instanceof Element ? event.target.closest("a[href]") : null;
    if (!anchor) return;
    let url;
    try {
      url = new URL(anchor.href);
    } catch {
      return;
    }
    if (!HANDOFF_HOSTS.has(url.hostname)) return;

    let anonId = null;
    let sessionId = null;
    try {
      anonId = window.localStorage.getItem("did");
      sessionId = window.sessionStorage.getItem("did_session");
    } catch {
      return;
    }
    if (!anonId) return;
    url.searchParams.set("anonId", anonId);
    if (sessionId) url.searchParams.set("sessionId", sessionId);
    const original = anchor.getAttribute("href");
    anchor.href = url.toString();
    // The browser has taken the link by the time this runs, so restore it and
    // leave no IDs in the page for a later copy or share.
    window.setTimeout(() => anchor.setAttribute("href", original), 0);
  }

  document.addEventListener("click", handOff, true);
  document.addEventListener("auxclick", handOff, true);
  // "Open in new tab" from the context menu fires neither click event.
  document.addEventListener("contextmenu", handOff, true);
})();
