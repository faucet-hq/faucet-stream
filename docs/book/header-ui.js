// Pydantic-style header. Built on top of mdBook's own search + theme machinery
// (we relocate and restyle its elements rather than replacing them, so search
// stays fully functional):
//   · mdBook's search box is moved into the menu bar and kept always-visible as
//     a centered "Search  ⌘K" pill; ⌘K / Ctrl-K focuses it.
//   · a single light/dark toggle replaces mdBook's five-theme popup.
// Loaded site-wide via book.toml `additional-js`. A no-op if the menu bar or a
// given control is absent.
(function () {
  "use strict";

  // Clean line-style (Feather) sun / moon so the control reads unmistakably as
  // a light/dark switch (not a settings cog).
  var SUN =
    '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><circle cx="12" cy="12" r="4"/><path d="M12 2v2M12 20v2M4.93 4.93l1.41 1.41M17.66 17.66l1.41 1.41M2 12h2M20 12h2M4.93 19.07l1.41-1.41M17.66 6.34l1.41-1.41"/></svg>';
  var MOON =
    '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/></svg>';

  var LINKEDIN_URL = "https://www.linkedin.com/company/faucet-stream";
  var LINKEDIN =
    '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 448 512" aria-hidden="true"><path d="M416 32H31.9C14.3 32 0 46.5 0 64.3v383.4C0 465.5 14.3 480 31.9 480H416c17.6 0 32-14.5 32-32.3V64.3c0-17.8-14.4-32.3-32-32.3zM135.4 416H69V202.2h66.5V416zm-33.2-243c-21.3 0-38.5-17.3-38.5-38.5S80.9 96 102.2 96c21.2 0 38.5 17.3 38.5 38.5 0 21.3-17.2 38.5-38.5 38.5zm282.1 243h-66.4V312c0-24.8-.5-56.7-34.5-56.7-34.6 0-39.9 27-39.9 54.9V416h-66.4V202.2h63.7v29.2h.9c8.9-17 30.6-34.9 56.4-34.9 67.2 0 79.7 44.3 79.7 101.9V416z"/></svg>';

  function init() {
    var bar = document.getElementById("mdbook-menu-bar");
    if (!bar) return;
    var left = bar.querySelector(".left-buttons");
    var right = bar.querySelector(".right-buttons");

    // ---- Search: relocate mdBook's wrapper into the bar, always visible ------
    var wrapper = document.getElementById("mdbook-search-wrapper");
    if (wrapper && left) {
      // NB: we intentionally do NOT strip mdBook's `hidden` class here. Its
      // search toggle only loads the search index when the wrapper is still
      // hidden (otherwise the click is treated as "close"). Our CSS forces the
      // wrapper visible via `!important` regardless of the class, and
      // `ensureSearchReady` (below) clicks the toggle to load the index.
      left.insertAdjacentElement("afterend", wrapper);
      bar.classList.add("fs-has-search");
      var input = document.getElementById("mdbook-searchbar");
      if (input) {
        input.setAttribute("placeholder", "Search");
        // A ⌘K / Ctrl-K hint badge, right-aligned inside the pill.
        var sw = input.parentElement; // .search-wrapper
        if (sw && !sw.querySelector(".fs-kbd")) {
          var isMac = /Mac|iPhone|iPad/.test(navigator.platform || "");
          var kbd = document.createElement("kbd");
          kbd.className = "fs-kbd";
          kbd.textContent = isMac ? "⌘ K" : "Ctrl K";
          kbd.setAttribute("aria-hidden", "true");
          sw.appendChild(kbd);
          // Hide the hint once the field has focus or text.
          var sync = function () {
            kbd.style.display =
              input.value || document.activeElement === input ? "none" : "";
          };
          input.addEventListener("focus", sync);
          input.addEventListener("blur", sync);
          input.addEventListener("input", sync);
        }
      }
    }

    // mdBook lazy-loads its search index + wires the type-to-search handler only
    // when search is first "opened" via its toggle — and it wires that toggle on
    // window `load`, AFTER this DOMContentLoaded init. Our box is always
    // visible, so we open it ourselves. `ensureSearchReady` clicks the (hidden)
    // toggle once the index isn't already loading; we call it eagerly on `load`
    // (blurring so the field doesn't grab the keyboard) and lazily on first
    // focus as a timing-proof safety net.
    var ensureSearchReady = function () {
      if (document.querySelector('script[src*="searchindex"]')) return;
      var t = document.getElementById("mdbook-search-toggle");
      if (!t) return;
      var y = window.scrollY; // the toggle does scrollTo(0,0); keep our place
      t.click();
      if (window.scrollY !== y) window.scrollTo(0, y);
    };
    window.addEventListener("load", function () {
      setTimeout(function () {
        ensureSearchReady();
        var sb0 = document.getElementById("mdbook-searchbar");
        if (sb0) {
          setTimeout(function () {
            sb0.blur();
            sb0.classList.remove("active");
          }, 0);
        }
      }, 60);
    });
    var sbFocus = document.getElementById("mdbook-searchbar");
    if (sbFocus) {
      sbFocus.addEventListener("focus", ensureSearchReady, { once: true });
    }

    // ---- ⌘K / Ctrl-K focuses the search field -------------------------------
    document.addEventListener("keydown", function (e) {
      if ((e.metaKey || e.ctrlKey) && (e.key === "k" || e.key === "K")) {
        e.preventDefault();
        var i = document.getElementById("mdbook-searchbar");
        if (i) i.focus();
      }
    });

    // ---- Code blocks: always CONTRAST the page -----------------------------
    // Dark code panel on a light page, light code panel on a dark page, so code
    // always stands out. mdBook paints code backgrounds from its highlight
    // stylesheet (light `highlight.css` vs dark `tomorrow-night.css`) and swaps
    // it to MATCH the page theme; we invert that — pick the OPPOSITE sheet from
    // the page — and re-assert it on load and after every toggle.
    function forceContrastCode() {
      var cl = document.documentElement.classList;
      var pageDark =
        cl.contains("navy") || cl.contains("coal") || cl.contains("ayu");
      var lightSheet = document.getElementById("mdbook-highlight-css");
      var ayuSheet = document.getElementById("mdbook-ayu-highlight-css");
      var darkSheet = document.getElementById("mdbook-tomorrow-night-css");
      if (lightSheet) lightSheet.disabled = !pageDark; // light code only on a dark page
      if (darkSheet) darkSheet.disabled = pageDark; //   dark code only on a light page
      if (ayuSheet) ayuSheet.disabled = true;
    }
    forceContrastCode();
    window.addEventListener("load", forceContrastCode);

    if (right && !document.getElementById("fs-linkedin")) {
      var li = document.createElement("a");
      li.id = "fs-linkedin";
      li.href = LINKEDIN_URL;
      li.target = "_blank";
      li.rel = "noopener";
      li.title = "Follow faucet-stream on LinkedIn";
      li.setAttribute("aria-label", li.title);
      li.innerHTML = '<span class="fa-svg">' + LINKEDIN + "</span>";
      var repo = right.querySelector('a[title="Git repository"]');
      if (repo) repo.insertAdjacentElement("afterend", li);
      else right.appendChild(li);
    }

    // ---- Theme: one light/dark toggle ---------------------------------------
    if (right) {
      var THEMES = ["light", "rust", "coal", "navy", "ayu"];
      var DARK = "navy", LIGHT = "light";
      var isDark = function () {
        var cl = document.documentElement.classList;
        return cl.contains("navy") || cl.contains("coal") || cl.contains("ayu");
      };
      var btn = document.createElement("button");
      btn.id = "fs-theme-toggle";
      btn.className = "icon-button";
      btn.type = "button";
      var updateIcon = function () {
        btn.innerHTML =
          '<span class="fa-svg">' + (isDark() ? SUN : MOON) + "</span>";
        btn.title = isDark() ? "Switch to light theme" : "Switch to dark theme";
        btn.setAttribute("aria-label", btn.title);
      };
      var setTheme = function (t) {
        var cl = document.documentElement.classList;
        THEMES.forEach(function (x) { cl.remove(x); });
        cl.add(t);
        try { localStorage.setItem("mdbook-theme", t); } catch (e) {}
        updateIcon();
      };
      btn.addEventListener("click", function () {
        // Delegate to mdBook's own theme switch via its (hidden) theme-list
        // buttons: mdBook swaps BOTH the <html> theme class AND the syntax-
        // highlight stylesheet (highlight.css ↔ tomorrow-night.css) and persists
        // the choice. Our earlier hand-rolled class swap left code blocks stuck
        // on the wrong highlight theme (light page, dark code) until the next
        // page load re-synced them.
        var goDark = !isDark();
        var mdBtn = document.getElementById(
          goDark ? "mdbook-theme-navy" : "mdbook-theme-light"
        );
        if (mdBtn) {
          mdBtn.click();
        } else {
          setTheme(goDark ? DARK : LIGHT); // fallback if mdBook's buttons are absent
        }
        forceContrastCode(); // book.js reset the highlight theme — re-invert it
        updateIcon();
      });
      updateIcon();
      right.insertBefore(btn, right.firstChild);
    }
  }

  if (document.readyState !== "loading") init();
  else document.addEventListener("DOMContentLoaded", init);
})();
