// Fetches the API status and shows it on the page.
window.addEventListener("DOMContentLoaded", async () => {
  const el = document.getElementById("status");
  try {
    const res = await fetch("/api/status");
    const body = await res.json();
    el.textContent = "API status: " + body.status;
  } catch (err) {
    el.textContent = "API unreachable: " + err;
  }
});
